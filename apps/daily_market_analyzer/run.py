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
import concurrent.futures # <== 新增導入
from tqdm import tqdm # <== 新增導入
import io # <== 根據作戰計畫新增導入
import contextlib # <== 根據作戰計畫新增導入

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
    parser.add_argument("--start-date", required=True, help="分析起始日期 (格式: YYYY-MM-DD)。") # 中文化 help
    parser.add_argument("--end-date", required=True, help="分析結束日期 (格式: YYYY-MM-DD)。") # 中文化 help
    parser.add_argument("--db-path", default="data_workspace/daily_market_analyzer.duckdb", # 保留現有的 db-path 作為主要/永久數據庫的路徑
                        help="主分析資料庫的完整路徑 (例如: data_workspace/daily_market_analysis.duckdb)。") # 更新 help 文字
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

    args = parser.parse_args()

    print("--- 每日市場洞察報告引擎 v12.0 ---") # 更新版本號
    overall_start_time = datetime.now()
    report_generation_time_for_filename = datetime.now() # <<-- 新增：提前定義用於檔案名的時間戳

    # 時區信息 %Z%z 可能因環境導致不同輸出，可考慮標準化為 UTC 或移除
    print(f"任務開始時間: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}") # 簡化時間格式
    print(f"執行參數: 標的='{args.tickers}', 起始日='{args.start_date}', 結束日='{args.end_date}', 資料庫='{args.db_path}', 資料表='{args.table_name}'") # 中文化

    if args.process_uploads:
        print("資訊：--process-uploads 選項已指定，但此功能尚在開發中，將被略過。") # 中文化
        # TODO: 添加 file_processor 邏輯

    # 初始化組件
    # 主進程保留 DBManager 和 AnalysisEngine 實例，用於後續的統一寫入和報告生成。
    # YFinanceClient 的實例化移至 process_single_ticker 函數內部，以便在各個進程中獨立運作。
    db_manager = DBManager(db_path=args.db_path)
    analysis_engine = AnalysisEngine(db_manager_instance=db_manager)

    # 確保主資料庫和快取資料庫的資料表結構都已建立
    # 快取資料庫的表格由 process_single_ticker 內的 YFinanceClient -> DBManager 鏈路確保
    # 主資料庫的表格在此處確保
    db_manager.create_ohlcv_table(table_name=args.table_name, target_db_path=args.db_path)
    # 為了確保 process_single_ticker 中的 YFClient->DBManager 可以寫入快取，快取DB的表也需要提前建立
    # 或者讓 process_single_ticker 內的 DBManager 自己去 create_table_if_not_exists
    # 這裡選擇在主進程統一建立，確保一致性
    if args.cache_db_path: # 僅當提供了 cache_db_path 時才嘗試建立快取表
         db_manager.create_ohlcv_table(table_name=args.table_name, target_db_path=args.cache_db_path)
    else:
        print("警告: 未提供 --cache-db-path，快取功能將受限或無法運作。")


    tickers_list = [ticker.strip().upper() for ticker in args.tickers.split(',')]
    overall_execution_log = {} # 用於聚合所有 tickers 的執行日誌
    all_hydrated_dfs = []      # 用於收集所有成功回填的 DataFrame

    # --- 使用 ProcessPoolExecutor 進行平行處理 ---
    # 決定最大工作進程數，讓 ProcessPoolExecutor 自動決定 (通常是 os.cpu_count())
    # 移除先前手動計算 max_workers 的邏輯
    # max_workers = min(os.cpu_count() or 1, len(tickers_list)) # 舊邏輯
    # print(f"INFO: 啟動聯合作戰模式，使用最多 {max_workers} 個進程處理 {len(tickers_list)} 個標的。")
    print(f"INFO: 啟動聯合作戰模式，自動偵測並使用可用核心處理 {len(tickers_list)} 個標的。")

    with concurrent.futures.ProcessPoolExecutor() as executor: # 移除 max_workers 參數，讓其自動決定
        # 準備提交給進程池的任務
        # future_to_ticker 映射，用於在任務完成時識別對應的 ticker
        future_to_ticker = {
            # --- 修改 submit 呼叫，傳入 verbose 狀態 ---
            executor.submit(
                process_single_ticker,
                ticker, args.start_date, args.end_date, args.db_path, args.cache_db_path, args.table_name, args.force_refresh, args.verbose
            ): ticker for ticker in tickers_list
        }

        # 使用 tqdm 顯示進度
        # concurrent.futures.as_completed 會在任何 future 完成時 yield 它
        for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(tickers_list), desc="聯合作戰引擎執行中"):
            ticker = future_to_ticker[future]
            try:
                # 獲取單個任務的結果 (hydrated_df, ticker_execution_log, worker_logs)
                # worker_logs 是在靜默模式下捕獲的日誌，詳細模式下為空字串
                hydrated_df_single, ticker_execution_log_single, worker_logs = future.result()

                # 如果在靜默模式下捕獲了日誌，可以在這裡選擇性地打印或記錄它們
                # 例如，如果發生了錯誤，即使在靜默模式下，也可能希望看到特定 worker 的日誌
                if worker_logs and not args.verbose: # 只有在靜默模式且 worker_logs 非空時處理
                    # 這裡可以決定如何處理 worker_logs，例如：
                    # print(f"---來自背景進程 {ticker} 的日誌---\n{worker_logs}\n---日誌結束---")
                    # 或者將其寫入一個聚合的日誌檔案
                    pass # 目前暫不處理，但保留此處以供未來擴展

                # 合併日誌
                if ticker_execution_log_single: # 確保日誌不是 None
                    for date_key, log_val_per_day in ticker_execution_log_single.items():
                        if date_key not in overall_execution_log:
                            overall_execution_log[date_key] = {}
                        # log_val_per_day 應該是 {ticker_name: daily_log_detail}
                        # 如果 process_single_ticker 返回的 execution_log 結構與原先不同，這裡需要調整
                        # 假設 execution_log 結構是 {date_str: {ticker_str: log_details}}
                        overall_execution_log[date_key].update(log_val_per_day)


                if hydrated_df_single is not None and not hydrated_df_single.empty:
                    all_hydrated_dfs.append(hydrated_df_single)
                elif hydrated_df_single is None:
                    print(f"資訊：標的 {ticker} 的平行處理任務未返回有效的 DataFrame。")


            except Exception as exc:
                print(f"錯誤：處理標的 {ticker} 的平行任務時發生例外: {exc}")
                # 可以在此處記錄整個 Ticker 的處理失敗日誌到 overall_execution_log
                # 例如，為該 ticker 在請求日期範圍內的所有日期標記錯誤
                for date_str_in_range in pd.date_range(args.start_date, args.end_date).strftime('%Y-%m-%d'):
                    overall_execution_log.setdefault(date_str_in_range, {}).setdefault(ticker, {
                        "status": "parallel_task_exception",
                        "message": f"平行任務執行失敗: {str(exc)}",
                        "count": 0,
                        "interval": None
                    })

    # --- 所有平行任務完成後，在主進程中統一處理 ---
    print("\n--- 所有平行數據回填任務完成，正在合併數據... ---")
    if all_hydrated_dfs:
        # 合併所有從各進程收集回來的 DataFrame
        # 這些 DataFrame 應該是已經經過快取處理的數據 (即 yf_client.hydrate_data_range 的輸出)
        # 它們代表的是需要在主數據庫中進行 upsert 的數據。
        final_master_df = pd.concat(all_hydrated_dfs, ignore_index=True)
        if not final_master_df.empty:
            print(f"INFO: 數據合併完成，總共 {len(final_master_df)} 筆數據。準備一次性寫入主分析資料庫 '{args.db_path}'...")
            try:
                db_manager.upsert_data(final_master_df, table_name=args.table_name, target_db_path=args.db_path)
                print(f"INFO: {len(final_master_df)} 筆合併數據成功寫入主分析資料庫。")
            except Exception as e_main_upsert:
                print(f"錯誤：將合併後的數據寫入主分析資料庫 '{args.db_path}' 失敗: {e_main_upsert}")
                # 此處的錯誤比較嚴重，可能需要更詳細的日誌或通知
        else:
            print("INFO: 合併後的 DataFrame 為空，無需寫入主分析資料庫。")
    else:
        print("INFO: 本次執行未獲取到任何新的數據可寫入主分析資料庫。")

    overall_end_time = datetime.now()
    task_duration_seconds = (overall_end_time - overall_start_time).total_seconds()
    print(f"\n--- 所有數據處理與寫入完成 ---") # 更新日誌
    print(f"任務結束時間: {overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"總執行時長: {task_duration_seconds:.2f} 秒")

    # 初始化報告生成器 (傳入合併後的日誌和主進程的分析引擎實例)
    report_gen = ReportGenerator(execution_log=overall_execution_log,
                                 analysis_engine_instance=analysis_engine)

    print("\n--- 生成市場分析報告 ---") # 中文化

    current_report_time = datetime.now() # 用於傳遞給 ReportGenerator
    final_report_str = report_gen.generate_full_report(
        overall_start_date_str=args.start_date,
        overall_end_date_str=args.end_date,
        report_generation_time=current_report_time,
        task_duration_seconds=task_duration_seconds,
        target_tickers=tickers_list,
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
    report_output_dir = os.path.join("data_workspace", "reports") # 定義報告輸出目錄
    os.makedirs(report_output_dir, exist_ok=True) # 確保目錄存在

    # 使用提前定義的時間戳 report_generation_time_for_filename
    print(f"DEBUG: Type of report_generation_time_for_filename before strftime: {type(report_generation_time_for_filename)}")
    report_filename_dt_str = report_generation_time_for_filename.strftime('%Y%m%d_%H%M%S')
    report_filename = f"market_analysis_report_{report_filename_dt_str}.md"
    report_filepath = os.path.join(report_output_dir, report_filename)

    try:
        with open(report_filepath, "w", encoding="utf-8") as f:
            f.write(final_report_str)
        print(f"\n報告已成功儲存至：{report_filepath}")
    except IOError as e:
        print(f"\n錯誤：儲存報告至檔案失敗：{e}")

    print("\n--- 每日市場洞察報告引擎任務執行完畢 ---")

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
