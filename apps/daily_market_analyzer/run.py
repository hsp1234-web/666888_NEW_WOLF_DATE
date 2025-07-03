# -*- coding: utf-8 -*-
"""
每日市場分析儀 主執行入口。

接收命令列參數，協調 YFinanceClient 進行數據擷取與考古，
使用 DBManager 將數據存入資料庫，透過 AnalysisEngine 分析數據，
最後使用 ReportGenerator 生成每日市場洞察報告。
"""
import argparse
import sys
import os
from datetime import datetime
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import io
import contextlib
import psutil # <-- 第三戰區：導入 psutil

# 設定專案路徑，確保可以正確匯入其他模組
def setup_project_path():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        # print(f"DEBUG: Project root added to sys.path: {project_root}") # 移除調試信息

setup_project_path()

try:
    from apps.daily_market_analyzer.yfinance_client import YFinanceClient
    from apps.daily_market_analyzer.db_manager import DBManager
    from apps.daily_market_analyzer.analysis_engine import AnalysisEngine
    from apps.daily_market_analyzer.report_generator import ReportGenerator
    # print("DEBUG: Successfully imported YFinanceClient, DBManager, AnalysisEngine, ReportGenerator") # 移除調試信息
except ModuleNotFoundError as e:
    print(f"錯誤：導入模組時發生錯誤 (ModuleNotFoundError): {e}") # 中文化
    # print(f"DEBUG: Current sys.path: {sys.path}") # 保留或移除調試信息
    # try:
    #     print(f"DEBUG: Contents of 'apps/': {os.listdir('apps')}")
    #     print(f"DEBUG: Contents of 'apps/daily_market_analyzer/': {os.listdir('apps/daily_market_analyzer')}")
    # except FileNotFoundError:
    #     print("DEBUG: 'apps/' or 'apps/daily_market_analyzer/' directory not found from current working directory.")
    raise

# --- process_single_ticker 函數定義移至 main 函數之前 ---
def process_single_ticker(ticker, start_date, end_date, db_path, cache_db_path, table_name, force_refresh, verbose_mode):
    """
    處理單一金融標的的完整數據回填與寫入邏輯。
    此函數將在獨立的進程中執行。
    """
    pid = os.getpid() # 保留 PID 用於日誌

    # 如果不是詳細模式，則捕獲所有輸出
    if not verbose_mode:
        log_capture_string = io.StringIO()
        with contextlib.redirect_stdout(log_capture_string), contextlib.redirect_stderr(log_capture_string):
            try:
                # 這部分的核心邏輯不變
                # 注意：此處的 print 將被捕獲
                print(f"--- [PID:{pid}] (靜默) 開始處理標的: {ticker} ---")

                # 重新初始化 DBManager 和 YFinanceClient
                # 確保這些類別的初始化過程輕量，或能在多進程環境下安全獨立運行。
                db_manager_process = DBManager(db_path=db_path)
                yf_client_process = YFinanceClient(db_manager=db_manager_process, cache_db_path=cache_db_path)

                hydrated_df, execution_log = yf_client_process.hydrate_data_range(
                    ticker, start_date, end_date,
                    db_table_name=table_name,
                    force_refresh=force_refresh
                )
                print(f"--- [PID:{pid}] (靜默) 標的: {ticker} 處理完畢 ---")
            except Exception as e:
                print(f"--- [PID:{pid}] (靜默) 標的: {ticker} 發生嚴重錯誤: {e} ---")
                # 保持與原邏輯相似的錯誤處理方式
                hydrated_df = None
                error_log = {}
                # 考慮到 execution_log 的結構可能比較複雜，這裡暫時返回一個簡化的錯誤標記。
                # 如果 hydrate_data_range 本身會產生包含錯誤訊息的 execution_log，則應優先使用那個。
                # 這裡的目標是確保即使在捕獲日誌時，錯誤也能被記錄和返回。
                temp_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                current_date_obj = temp_date_obj
                from datetime import timedelta # 確保 timedelta 可用
                while current_date_obj <= end_date_obj:
                    date_str = current_date_obj.strftime("%Y-%m-%d")
                    error_log.setdefault(date_str, {}).setdefault(ticker, {
                        "status": "hydration_error_in_silent_mode",
                        "message": str(e),
                        "count": 0,
                        "interval": None
                    })
                    current_date_obj += timedelta(days=1)
                execution_log = error_log # 將錯誤日誌賦給 execution_log

        worker_logs = log_capture_string.getvalue()
        return hydrated_df, execution_log, worker_logs

    # 如果是詳細模式，則不捕獲，直接讓日誌打印出來
    else:
        # 核心邏輯與上面幾乎相同，只是沒有了 with contextlib.redirect... 的包裝
        # 且 print 語句會直接輸出到主控台
        print(f"--- [PID:{pid}] (詳細) 開始處理標的: {ticker} ---")
        try:
            db_manager_process = DBManager(db_path=db_path)
            yf_client_process = YFinanceClient(db_manager=db_manager_process, cache_db_path=cache_db_path)

            hydrated_df, execution_log = yf_client_process.hydrate_data_range(
                ticker, start_date, end_date,
                db_table_name=table_name,
                force_refresh=force_refresh
            )
            print(f"--- [PID:{pid}] (詳細) 標的: {ticker} 處理完畢 ---")
        except Exception as e:
            print(f"--- [PID:{pid}] (詳細) 標的: {ticker} 發生嚴重錯誤: {e} ---")
            hydrated_df = None
            # 與原邏輯相似的錯誤處理
            error_log = {}
            temp_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            current_date_obj = temp_date_obj
            from datetime import timedelta # 確保 timedelta 可用
            while current_date_obj <= end_date_obj:
                date_str = current_date_obj.strftime("%Y-%m-%d")
                error_log.setdefault(date_str, {}).setdefault(ticker, {
                    "status": "hydration_error_in_verbose_mode",
                    "message": str(e),
                    "count": 0,
                    "interval": None
                })
                current_date_obj += timedelta(days=1)
            execution_log = error_log

        # 在詳細模式下，我們不需要返回日誌字串，因為它已經被打印了
        return hydrated_df, execution_log, ""

def main():
    """
    主執行函數 for Daily Market Analyzer。
    """
    parser = argparse.ArgumentParser(description="每日市場洞察報告與智能數據考古引擎。")
    parser.add_argument("--tickers", required=True, help="要分析的標的列表，以逗號分隔 (例如: AAPL,MSFT)。") # 中文化 help
    # 調整 --start-date 和 --end-date，使其在 --report-only 模式下非必需
    parser.add_argument("--start-date", help="數據獲取分析起始日期 (格式: YYYY-MM-DD)。若非 --report-only 模式則為必需。")
    parser.add_argument("--end-date", help="數據獲取分析結束日期 (格式: YYYY-MM-DD)。若非 --report-only 模式則為必需。")
    parser.add_argument("--db-path", default="data_workspace/daily_market_analyzer.duckdb",
                        help="主分析資料庫的完整路徑 (例如: data_workspace/daily_market_analysis.duckdb)。")
    parser.add_argument("--db-name", default="daily_market_analysis.duckdb", # 新增/調整 db-name
                        help="主分析資料庫的檔案名稱 (非完整路徑)。") # 新增 help 文字以符合指令
    parser.add_argument("--cache-db-path",
                        help="DuckDB 快取資料庫的最終存檔路徑。") # 新增 cache-db-path 參數
    parser.add_argument("--table-name", default="market_ohlcv_data",
                        help="資料庫中儲存 OHLCV 數據的表格名稱。") # 中文化 help
    parser.add_argument("--force-refresh", action="store_true",
                        help="若指定，則強制重新獲取所有數據，忽略快取。") # 新增 force-refresh 參數
    parser.add_argument("--process-uploads", action="store_true",
                        help="若指定，則處理 'uploads' 資料夾 (此功能待實現)。") # 中文化 help

    # --- 新增指令 ---
    parser.add_argument("--verbose", action="store_true", help="啟用詳細日誌模式，將所有進程的即時日誌打印到主控台。")
    # --- 第二戰區：新增命令列參數 ---
    parser.add_argument("--data-only", action="store_true", help="若啟用，僅執行數據獲取與儲存，不生成報告。")
    parser.add_argument("--report-only", action="store_true", help="若啟用，僅從資料庫讀取數據並生成報告，不進行數據獲取。")
    parser.add_argument("--report-start-date", help="與 --report-only 配合使用，定義報告的起始日期 (格式: YYYY-MM-DD)。")
    parser.add_argument("--report-end-date", help="與 --report-only 配合使用，定義報告的結束日期 (格式: YYYY-MM-DD)。")
    # --tickers 參數已存在，可在 report-only 模式下複用以指定報告的標的
    # --- 第三戰區：新增 DuckDB 記憶體配置參數 ---
    parser.add_argument("--duckdb-memory-limit", type=str, default=None,
                        help="直接設定 DuckDB 的記憶體限制 (例如 '1GB', '512MB')。若設定此項，則忽略 --duckdb-memory-limit-ratio。")
    parser.add_argument("--duckdb-memory-limit-ratio", type=float, default=0.7,
                        help="設定 DuckDB 可使用的系統可用記憶體比例 (0.1 至 1.0)。預設值: 0.7。僅在未設定 --duckdb-memory-limit 時生效。")

    args = parser.parse_args()

    # --- 第三戰區：計算 DuckDB 記憶體限制 ---
    duckdb_mem_limit_str = None
    if args.duckdb_memory_limit:
        duckdb_mem_limit_str = args.duckdb_memory_limit
        print(f"INFO: 使用指定的 DuckDB 記憶體限制: {duckdb_mem_limit_str}")
    else:
        if not (0.1 <= args.duckdb_memory_limit_ratio <= 1.0):
            print(f"警告: --duckdb-memory-limit-ratio 提供的比例 {args.duckdb_memory_limit_ratio} 超出有效範圍 (0.1-1.0)。將使用預設值 0.7。") # 中文化
            args.duckdb_memory_limit_ratio = 0.7

        available_memory_bytes = psutil.virtual_memory().available
        calculated_limit_bytes = int(available_memory_bytes * args.duckdb_memory_limit_ratio)
        # DuckDB 接受如 '1GB', '1500MB' 的格式
        # 簡單轉換為 MB 以增加可讀性
        calculated_limit_mb = calculated_limit_bytes / (1024 * 1024)
        duckdb_mem_limit_str = f"{int(calculated_limit_mb)}MB"
        print(f"INFO: 系統可用記憶體: {available_memory_bytes / (1024*1024):.2f} MB。動態設定 DuckDB 記憶體限制為可用記憶體的 {args.duckdb_memory_limit_ratio*100:.0f}%: {duckdb_mem_limit_str}")

    # --- 第二戰區：參數校驗 ---
    if args.data_only and args.report_only:
        print("錯誤：--data-only 和 --report-only 選項不能同時啟用。請選擇一個或都不選（完整流程）。") # 中文化
        sys.exit(1)
    if args.report_only and (not args.report_start_date or not args.report_end_date):
        print("錯誤：使用 --report-only 時，必須同時提供 --report-start-date 和 --report-end-date。") # 中文化
        sys.exit(1)
    # 新增校驗：如果不是 report_only 模式，則 start-date 和 end-date 是必需的
    if not args.report_only and (not args.start_date or not args.end_date):
        print("錯誤：在數據處理或完整流程模式下，必須提供 --start-date 和 --end-date。") # 中文化
        sys.exit(1)
    if (args.report_start_date or args.report_end_date) and not args.report_only and not (args.data_only is False and args.report_only is False): # 嚴格來說，這兩個參數只給 report_only 用
        # 這條警告的條件可能需要調整，因為完整流程中 report_s/e 會用到 args.start_date/end_date
        # 主要目的是防止用戶在 --data-only 時誤用 --report-start-date/end-date
        if args.data_only :
             print("警告：--report-start-date 和 --report-end-date 參數在 --data-only 模式下無效。") # 中文化

    print("--- 每日市場洞察報告引擎 v31.0 ---") # 更新版本號
    overall_start_time = datetime.now()
    # report_generation_time_for_filename = datetime.now() # 移到報告生成部分，以反映實際報告生成時間

    print(f"任務開始時間: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    # 根據模式調整打印的執行參數
    mode = "完整流程"
    if args.data_only:
        mode = "數據處理模式 (--data-only)"
    elif args.report_only:
        mode = "報告生成模式 (--report-only)"

    print(f"執行模式: {mode}") # 中文化
    print(f"通用參數: 資料庫='{args.db_path}', 資料表='{args.table_name}'") # 中文化
    if not args.report_only: # 數據處理或完整流程
        print(f"數據處理參數: 標的='{args.tickers}', 起始日='{args.start_date}', 結束日='{args.end_date}'")
    if not args.data_only: # 報告生成或完整流程
        report_s = args.report_start_date if args.report_only else args.start_date
        report_e = args.report_end_date if args.report_only else args.end_date
        report_t = args.tickers # 報告的tickers總是來自 --tickers
        print(f"報告生成參數: 標的='{report_t}', 報告起始日='{report_s}', 報告結束日='{report_e}'")


    if args.process_uploads:
        print("資訊：--process-uploads 選項已指定，但此功能尚在開發中，將被略過。")
        # TODO: 添加 file_processor 邏輯

    # 初始化 DBManager (所有模式都需要)，傳入計算好的記憶體限制
    db_manager = DBManager(db_path=args.db_path, memory_limit=duckdb_mem_limit_str)
    # AnalysisEngine 和 ReportGenerator 則根據模式按需初始化

    # --- 第二戰區：改造 main 函數邏輯 ---
    data_processed_successfully = False
    if not args.report_only: # 完整流程 或 --data-only 模式
        print("\n--- [階段開始] 數據獲取與處理 ---")
        # 初始化 YFinanceClient 相關的組件 (只在需要數據處理時)
        # AnalysisEngine 實例在完整流程和報告模式下都需要，但其 db_manager 在此階段已初始化
        # YFinanceClient 的實例化移至 process_single_ticker 函數內部

        # 確保主資料庫和快取資料庫的資料表結構都已建立
        db_manager.create_ohlcv_table(table_name=args.table_name, target_db_path=args.db_path)
        if args.cache_db_path:
             db_manager.create_ohlcv_table(table_name=args.table_name, target_db_path=args.cache_db_path)
        else:
            print("警告: 未提供 --cache-db-path，快取功能將受限或無法運作。")

        tickers_list_data = [ticker.strip().upper() for ticker in args.tickers.split(',')]
        overall_execution_log_data = {}
        all_hydrated_dfs_data = []

        print(f"INFO: 啟動聯合作戰模式，自動偵測並使用可用核心處理 {len(tickers_list_data)} 個標的。")
        with concurrent.futures.ProcessPoolExecutor() as executor:
            future_to_ticker = {
                executor.submit(
                    process_single_ticker,
                    ticker, args.start_date, args.end_date, args.db_path, args.cache_db_path, args.table_name, args.force_refresh, args.verbose
                ): ticker for ticker in tickers_list_data
            }
            for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(tickers_list_data), desc="數據聯合作戰引擎執行中"):
                ticker = future_to_ticker[future]
                try:
                    hydrated_df_single, ticker_execution_log_single, worker_logs = future.result()
                    if worker_logs and not args.verbose:
                        pass
                    if ticker_execution_log_single:
                        for date_key, log_val_per_day in ticker_execution_log_single.items():
                            if date_key not in overall_execution_log_data:
                                overall_execution_log_data[date_key] = {}
                            overall_execution_log_data[date_key].update(log_val_per_day)
                    if hydrated_df_single is not None and not hydrated_df_single.empty:
                        all_hydrated_dfs_data.append(hydrated_df_single)
                    elif hydrated_df_single is None:
                        print(f"資訊：標的 {ticker} 的平行處理任務未返回有效的 DataFrame。")
                except Exception as exc:
                    print(f"錯誤：處理標的 {ticker} 的平行任務時發生例外: {exc}")
                    for date_str_in_range in pd.date_range(args.start_date, args.end_date).strftime('%Y-%m-%d'):
                        overall_execution_log_data.setdefault(date_str_in_range, {}).setdefault(ticker, {
                            "status": "parallel_task_exception", "message": f"平行任務執行失敗: {str(exc)}",
                            "count": 0, "interval": None
                        })

        print("\n--- 所有平行數據回填任務完成，正在合併數據... ---")
        if all_hydrated_dfs_data:
            final_master_df = pd.concat(all_hydrated_dfs_data, ignore_index=True)
            if not final_master_df.empty:
                print(f"INFO: 數據合併完成，總共 {len(final_master_df)} 筆數據。準備一次性寫入主分析資料庫 '{args.db_path}'...")
                try:
                    db_manager.upsert_data(final_master_df, table_name=args.table_name, target_db_path=args.db_path)
                    print(f"INFO: {len(final_master_df)} 筆合併數據成功寫入主分析資料庫。")
                    data_processed_successfully = True # 標記數據處理成功
                except Exception as e_main_upsert:
                    print(f"錯誤：將合併後的數據寫入主分析資料庫 '{args.db_path}' 失敗: {e_main_upsert}")
                    data_processed_successfully = False
            else:
                print("INFO: 合併後的 DataFrame 為空，無需寫入主分析資料庫。")
                data_processed_successfully = True # 也算處理完成，只是沒數據
        else:
            print("INFO: 本次執行未獲取到任何新的數據可寫入主分析資料庫。")
            data_processed_successfully = True # 也算處理完成

        data_processing_end_time = datetime.now()
        data_task_duration_seconds = (data_processing_end_time - overall_start_time).total_seconds()
        print(f"\n--- [階段結束] 所有數據處理與寫入完成 ---")
        print(f"數據處理階段結束時間: {data_processing_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"數據處理階段執行時長: {data_task_duration_seconds:.2f} 秒")

        if args.data_only:
            print("\n--- --data-only 模式啟用，任務結束 ---")
            sys.exit(0)

        # 如果是完整流程，將數據處理的日誌傳遞給報告生成器
        # 注意：如果 --data-only 失敗，是否還要繼續生成報告？目前設計是如果 data_processed_successfully 為 False，則不生成報告
        if not data_processed_successfully:
            print("\n錯誤：數據處理階段未能成功完成，將跳過報告生成。")
            sys.exit(1)

        # 將 overall_execution_log_data 賦值給後續報告生成使用的 overall_execution_log
        overall_execution_log = overall_execution_log_data

    # --- 報告生成階段 (完整流程 或 --report-only 模式) ---
    if not args.data_only:
        print("\n--- [階段開始] 市場分析報告生成 ---")
        report_start_time_actual = datetime.now() # 報告階段的實際開始時間
        report_generation_time_for_filename = datetime.now() # 用於檔案名的時間戳

        # 初始化報告相關組件
        analysis_engine = AnalysisEngine(db_manager_instance=db_manager) # DBManager 已在上游初始化

        # 如果是 --report-only 模式，overall_execution_log 會是空的，這是預期的
        # ReportGenerator 需要能處理空的 execution_log (例如，只基於數據庫內容生成，不包含執行過程日誌)
        # 或者，我們可以考慮在 report_only 模式下，不傳遞 execution_log，或傳遞一個標記性的空日誌
        if args.report_only:
            overall_execution_log = {} # 在 report_only 模式下，沒有數據獲取日誌
            print("INFO: --report-only 模式，將基於資料庫數據生成報告，無實時執行日誌。")
            # 獲取 tickers 和日期範圍以用於報告
            report_tickers_list = [ticker.strip().upper() for ticker in args.tickers.split(',')]
            report_start_date_str = args.report_start_date
            report_end_date_str = args.report_end_date
            # 在 report-only 模式下，task_duration_seconds 不適用於數據獲取，可以設為0或計算報告生成時間
            task_duration_seconds_for_report = 0
        else: # 完整流程
            report_tickers_list = [ticker.strip().upper() for ticker in args.tickers.split(',')] # 從 args.tickers 獲取
            report_start_date_str = args.start_date # 完整流程使用數據處理的日期
            report_end_date_str = args.end_date
            # task_duration_seconds_for_report 指的是數據處理部分的時長
            task_duration_seconds_for_report = data_task_duration_seconds if 'data_task_duration_seconds' in locals() else 0


        report_gen = ReportGenerator(execution_log=overall_execution_log, # overall_execution_log 在完整流程時有值，report-only時為空
                                     analysis_engine_instance=analysis_engine)

        print(f"INFO: 準備生成報告，標的: {report_tickers_list}, 日期範圍: [{report_start_date_str} to {report_end_date_str}]")

        current_report_time = datetime.now() # 用於傳遞給 ReportGenerator
        final_report_str = report_gen.generate_full_report(
            overall_start_date_str=report_start_date_str,
            overall_end_date_str=report_end_date_str,
            report_generation_time=current_report_time, # 實際報告生成時間
            task_duration_seconds=task_duration_seconds_for_report, # 數據處理耗時 (report_only時為0)
            target_tickers=report_tickers_list,
            db_table_name=args.table_name
        )

        # 打印報告到控制台 (預覽)
        print("\n--- 市場分析報告內容預覽 ---")
        preview_lines = final_report_str.splitlines()[:30]
        for line in preview_lines:
            print(line)
        if len(final_report_str.splitlines()) > 30:
            print("... (報告內容過長，已截斷預覽) ...")

        # 將報告寫入檔案
        report_output_dir = os.path.join("data_workspace", "reports")
        os.makedirs(report_output_dir, exist_ok=True)

        report_filename_dt_str = report_generation_time_for_filename.strftime('%Y%m%d_%H%M%S')
        # 檔案名可以包含更多識別信息，例如模式和日期範圍
        report_mode_tag = "FULL"
        if args.report_only: report_mode_tag = "REPORTONLY"

        # 確保 report_tickers_list 和日期是字串，適合檔案名
        tickers_for_filename = "_".join(report_tickers_list).replace("^","").replace("=","_")[:30] # 簡化並限制長度
        date_range_for_filename = f"{report_start_date_str}_to_{report_end_date_str}"

        report_filename = f"market_analysis_{report_mode_tag}_{tickers_for_filename}_{date_range_for_filename}_{report_filename_dt_str}.md"
        report_filepath = os.path.join(report_output_dir, report_filename)

        try:
            with open(report_filepath, "w", encoding="utf-8") as f:
                f.write(final_report_str)
            print(f"\n報告已成功儲存至：{report_filepath}")
        except IOError as e:
            print(f"\n錯誤：儲存報告至檔案失敗：{e}")

        report_generation_end_time = datetime.now()
        report_task_duration_seconds = (report_generation_end_time - report_start_time_actual).total_seconds()
        print(f"\n--- [階段結束] 市場分析報告生成完畢 ---")
        print(f"報告生成階段結束時間: {report_generation_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"報告生成階段執行時長: {report_task_duration_seconds:.2f} 秒")


    overall_end_time_final = datetime.now()
    total_script_duration_seconds = (overall_end_time_final - overall_start_time).total_seconds()
    print(f"\n--- 每日市場洞察報告引擎任務總執行完畢 ---")
    print(f"總任務結束時間: {overall_end_time_final.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"總執行時長: {total_script_duration_seconds:.2f} 秒")


if __name__ == "__main__":
    # print(f"DEBUG: Current CWD for __main__ in daily_market_analyzer/run.py: {os.getcwd()}") # 移除調試信息
    # print(f"DEBUG: Current sys.path for __main__ in daily_market_analyzer/run.py: {sys.path}") # 移除調試信息

    # 移除 __main__ 中的延遲導入，因為已在頂部導入
    # if 'YFinanceClient' not in globals() or 'AnalysisEngine' not in globals(): # 檢查新加入的 AnalysisEngine
    #     try:
    #         # 更新導入路徑以匹配新的應用名稱
    #         from apps.daily_market_analyzer.yfinance_client import YFinanceClient
    #         from apps.daily_market_analyzer.db_manager import DBManager
    #         from apps.daily_market_analyzer.analysis_engine import AnalysisEngine
    #         from apps.daily_market_analyzer.report_generator import ReportGenerator
    #         # print("DEBUG: Late imports in daily_market_analyzer __main__ successful.") # 移除調試信息
    #     except ModuleNotFoundError as e:
    #         print(f"ERROR: Late ModuleNotFoundError in daily_market_analyzer __main__: {e}") # 中文化

    main()
