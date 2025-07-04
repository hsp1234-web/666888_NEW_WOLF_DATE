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

import psutil # 新增：用於偵測系統資源

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

def main():
    """
    主執行函數 for Daily Market Analyzer。
    """
    parser = argparse.ArgumentParser(description="每日市場洞察報告與智能數據考古引擎。")
    # 核心參數
    parser.add_argument("--tickers", help="要分析的標的列表，以逗號分隔 (例如: AAPL,MSFT)。在純報告模式下非必需，但若提供則用於報告。")
    parser.add_argument("--start-date", help="數據分析/獲取的起始日期 (格式: YYYY-MM-DD)。")
    parser.add_argument("--end-date", help="數據分析/獲取的結束日期 (格式: YYYY-MM-DD)。")

    # 流程控制參數
    parser.add_argument("--data-only", action="store_true", help="僅執行數據獲取和存儲流程。")
    parser.add_argument("--report-only", action="store_true", help="僅執行報告生成流程 (需要已存在的數據)。")
    parser.add_argument("--report-start-date", help="報告生成的起始日期 (格式: YYYY-MM-DD)，配合 --report-only 使用。")
    parser.add_argument("--report-end-date", help="報告生成的結束日期 (格式: YYYY-MM-DD)，配合 --report-only 使用。")

    # 資料庫與表格參數
    parser.add_argument("--db-path", default="data_workspace/daily_market_analyzer.duckdb",
                        help="主分析資料庫的完整路徑 (例如: data_workspace/daily_market_analysis.duckdb)。")
    parser.add_argument("--db-name", default="daily_market_analysis.duckdb", # 實際已較少直接使用，db_path 更為主要
                        help="主分析資料庫的檔案名稱 (參考用，主要以 db_path 為準)。")
    parser.add_argument("--cache-db-path",
                        help="DuckDB 快取資料庫的最終存檔路徑 (目前 yfinance_client 直接使用主DB進行快取檢查)。") # 說明其當前用途
    parser.add_argument("--table-name", default="market_ohlcv_data",
                        help="資料庫中儲存 OHLCV 數據的表格名稱。")
    parser.add_argument("--process-uploads", action="store_true",
                        help="若指定，則處理 'uploads' 資料夾 (此功能待實現)。")

    args = parser.parse_args()

    # 參數校驗
    if args.data_only and args.report_only:
        print("錯誤：--data-only 和 --report-only 選項不能同時指定。")
        sys.exit(1)

    if args.report_only:
        if not args.report_start_date or not args.report_end_date:
            print("錯誤：當使用 --report-only 時，必須提供 --report-start-date 和 --report-end-date。")
            sys.exit(1)
        if not args.tickers: # 在報告模式下，tickers 也是需要的，以確定報告內容
            print("錯誤：當使用 --report-only 時，必須提供 --tickers。")
            sys.exit(1)
    elif not args.data_only: # 即完整流程模式
        if not args.tickers or not args.start_date or not args.end_date:
            print("錯誤：在完整流程模式下，必須提供 --tickers, --start-date, 和 --end-date。")
            sys.exit(1)
    elif args.data_only: # 純數據模式
        if not args.tickers or not args.start_date or not args.end_date:
            print("錯誤：當使用 --data-only 時，必須提供 --tickers, --start-date, 和 --end-date。")
            sys.exit(1)


    print("--- 每日市場洞察報告引擎 v33.0 ---")
    overall_start_time = datetime.now()

    print(f"任務開始時間: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 「動態資源壓榨」：DuckDB 記憶體動態配置 ---
    db_config = {}
    try:
        available_bytes = psutil.virtual_memory().available
        available_gb = available_bytes / (1024**3)

        # 計算目標記憶體限制：可用記憶體的 70%，上限 12GB，最小 256MB
        mem_limit_gb = min(available_gb * 0.7, 12.0)
        mem_limit_gb = max(mem_limit_gb, 0.25) # 最小 256MB

        memory_limit_mb = int(mem_limit_gb * 1024)
        memory_limit_setting_str = f"{memory_limit_mb}MB"

        db_config = {'memory_limit': memory_limit_setting_str}
        print(f"INFO: 偵測到可用記憶體: {available_gb:.2f} GB。動態設定 DuckDB memory_limit 為: {memory_limit_setting_str}")
    except Exception as e:
        print(f"警告: 偵測系統可用記憶體或設定 DuckDB 組態時發生錯誤: {e}。將使用 DuckDB 預設記憶體配置。")
    # --- 結束 「動態資源壓榨」 ---

    # 初始化通用組件，傳入動態計算的 DB 組態
    db_manager = DBManager(db_path=args.db_path, duckdb_config=db_config)
    analysis_engine = AnalysisEngine(db_manager_instance=db_manager) # ReportGenerator 需要

    overall_execution_log = {}
    tickers_list = []
    if args.tickers: # 即使在 report-only 模式也可能需要解析
        tickers_list = [ticker.strip().upper() for ticker in args.tickers.split(',')]

    task_duration_seconds = 0 # 初始化

    # 根據模式執行不同流程
    if args.data_only:
        print("執行模式：僅數據處理 (--data-only)")
        print(f"執行參數: 標的='{args.tickers}', 起始日='{args.start_date}', 結束日='{args.end_date}', 資料庫='{args.db_path}', 資料表='{args.table_name}'")
        yf_client = YFinanceClient(db_manager=db_manager)
        overall_execution_log, _ = run_data_pipeline(args, db_manager, yf_client, tickers_list)
        # task_duration_seconds 將在 run_data_pipeline 內部計算或在此處計算實際數據處理時間

    elif args.report_only:
        print("執行模式：僅報告生成 (--report-only)")
        print(f"執行參數: 標的='{args.tickers}', 報告起始日='{args.report_start_date}', 報告結束日='{args.report_end_date}', 資料庫='{args.db_path}', 資料表='{args.table_name}'")
        # 注意：overall_execution_log 在此模式下通常為空，因為不執行數據獲取
        # ReportGenerator 將主要基於資料庫中的數據生成報告
        # report_start_date 和 report_end_date 將覆蓋 args.start_date 和 args.end_date 用於報告範圍
        args.start_date = args.report_start_date # 將報告日期賦給主日期參數以供 ReportGenerator 使用
        args.end_date = args.report_end_date
        run_report_generation(args, db_manager, analysis_engine, {}, tickers_list, overall_start_time, 0) # log 為空, duration 0

    else: # 完整流程
        print("執行模式：完整流程 (數據處理與報告生成)")
        print(f"執行參數: 標的='{args.tickers}', 起始日='{args.start_date}', 結束日='{args.end_date}', 資料庫='{args.db_path}', 資料表='{args.table_name}'")
        yf_client = YFinanceClient(db_manager=db_manager)
        data_pipeline_start_time = datetime.now()
        overall_execution_log, _ = run_data_pipeline(args, db_manager, yf_client, tickers_list)
        data_pipeline_end_time = datetime.now()
        task_duration_seconds = (data_pipeline_end_time - data_pipeline_start_time).total_seconds() # 僅數據處理時間

        report_generation_start_time = datetime.now()
        run_report_generation(args, db_manager, analysis_engine, overall_execution_log, tickers_list, overall_start_time, task_duration_seconds)
        report_generation_end_time = datetime.now()
        # 可以選擇是否將報告生成時間也計入總時長，或分開記錄

    final_overall_end_time = datetime.now()
    total_script_duration = (final_overall_end_time - overall_start_time).total_seconds()
    print(f"\n--- 總任務執行完畢 ---")
    print(f"總體結束時間: {final_overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"腳本總執行時長: {total_script_duration:.2f} 秒")


def run_data_pipeline(args, db_manager: DBManager, yf_client: YFinanceClient, tickers_list: list):
    """
    執行數據獲取、處理和存儲的流程。
    """
    print("\n--- 開始數據處理流程 ---")
    pipeline_start_time = datetime.now()

    # 確保資料表存在
    db_manager.create_ohlcv_table(table_name=args.table_name)

    current_overall_execution_log = {}

    if not tickers_list: # 應該在 main 校驗過，但以防萬一
        print("警告 (run_data_pipeline): 標的列表為空，無法執行數據流程。")
        return {}, []

    for ticker_symbol in tickers_list:
        print(f"\n--- 開始處理標的 (數據流程): {ticker_symbol} ---")
        hydrated_df, ticker_execution_log = yf_client.hydrate_data_range(
            ticker_symbol, args.start_date, args.end_date,
            db_table_name=args.table_name,
            force_refresh=False # 可考慮添加 force_refresh 參數到命令列
        )

        for date_key, ticker_daily_log_value in ticker_execution_log.items():
            if date_key not in current_overall_execution_log:
                current_overall_execution_log[date_key] = {}
            current_overall_execution_log[date_key].update(ticker_daily_log_value)

        if hydrated_df is not None and not hydrated_df.empty:
            print(f"資訊：標的 {ticker_symbol} 成功擷取 {len(hydrated_df)} 筆數據。準備寫入資料庫...")
            try:
                db_manager.upsert_data(hydrated_df, table_name=args.table_name)
                print(f"資訊：標的 {ticker_symbol} 數據成功寫入資料庫。")
            except Exception as e:
                print(f"錯誤：標的 {ticker_symbol} 數據寫入資料庫失敗: {e}")
                for date_str_key in pd.date_range(args.start_date, args.end_date).strftime('%Y-%m-%d'):
                    if date_str_key in current_overall_execution_log and ticker_symbol in current_overall_execution_log[date_str_key]:
                        current_overall_execution_log[date_str_key][ticker_symbol]['status'] = 'db_upsert_failed'
                        base_message = current_overall_execution_log[date_str_key][ticker_symbol].get('message', "")
                        if not isinstance(base_message, str): base_message = str(base_message)
                        current_overall_execution_log[date_str_key][ticker_symbol]['message'] = base_message + f" 資料庫更新失敗: {str(e)}"
        else:
            print(f"資訊：標的 {ticker_symbol} 未擷取到任何數據。")
        print(f"--- 標的 (數據流程): {ticker_symbol} 處理完畢 ---")

    pipeline_end_time = datetime.now()
    pipeline_duration_seconds = (pipeline_end_time - pipeline_start_time).total_seconds()
    print(f"\n--- 數據處理流程結束 ---")
    print(f"數據流程執行時長: {pipeline_duration_seconds:.2f} 秒")

    return current_overall_execution_log, tickers_list


def run_report_generation(args, db_manager: DBManager, analysis_engine: AnalysisEngine,
                          input_execution_log: dict, report_tickers_list: list,
                          report_overall_start_time: datetime, data_task_duration_seconds: float):
    """
    執行報告生成的流程。
    """
    print("\n--- 開始報告生成流程 ---")
    report_pipeline_start_time = datetime.now()

    # 如果是 report-only 模式，tickers_list 可能需要從 args 重新獲取
    # 但已在 main 函數中將 args.tickers 賦予 tickers_list，並傳遞給 report_tickers_list
    if not report_tickers_list:
         print("警告 (run_report_generation): 標的列表為空，無法生成報告。")
         return

    # ReportGenerator 初始化時可以傳入空的 execution_log，它主要用於記錄數據獲取過程。
    # 在 report-only 模式下，這個 log 可能不包含數據獲取的詳細信息。
    # AnalysisEngine 則直接從 DB 讀取數據。
    report_gen = ReportGenerator(execution_log=input_execution_log, # 可以是空的，或者只包含數據獲取階段的日誌
                                 analysis_engine_instance=analysis_engine)

    # 報告的時間戳使用傳入的 overall_start_time (即腳本開始運行的時間或特定模式的開始時間)
    # 而不是重新生成一個 report_generation_time_for_filename，以保持一致性
    report_filename_dt_str = report_overall_start_time.strftime('%Y%m%d_%H%M%S')

    # 報告的日期範圍應使用 args.start_date 和 args.end_date
    # 在 report-only 模式下，這兩個值已在 main 中被 report_start_date 和 report_end_date 覆蓋
    report_start_d = args.start_date
    report_end_d = args.end_date
    if args.report_only: # 再次確認，以防萬一
        report_start_d = args.report_start_date
        report_end_d = args.report_end_date

    print(f"INFO (run_report_generation): Generating report for tickers: {report_tickers_list} over range [{report_start_d} to {report_end_d}]")

    final_report_str = report_gen.generate_full_report(
        overall_start_date_str=report_start_d,
        overall_end_date_str=report_end_d,
        report_generation_time=datetime.now(), # 使用當前時間作為報告生成時間點
        task_duration_seconds=data_task_duration_seconds, # 這是數據處理時長，或在純報告模式下為0
        target_tickers=report_tickers_list,
        db_table_name=args.table_name
    )

    print("\n--- 市場分析報告內容預覽 ---")
    preview_lines = final_report_str.splitlines()[:30]
    for line in preview_lines:
        print(line)
    if len(final_report_str.splitlines()) > 30:
        print("... (報告內容過長，已截斷預覽) ...")

    report_output_dir = os.path.join("data_workspace", "reports")
    os.makedirs(report_output_dir, exist_ok=True)

    report_filename = f"market_analysis_report_{report_filename_dt_str}.md"
    if args.data_only:
        report_filename = f"data_pipeline_summary_{report_filename_dt_str}.md" # 若未來要為 data-only 生成摘要
    elif args.report_only:
        report_filename = f"on_demand_report_{report_filename_dt_str}.md"

    report_filepath = os.path.join(report_output_dir, report_filename)

    try:
        with open(report_filepath, "w", encoding="utf-8") as f:
            f.write(final_report_str)
        print(f"\n報告已成功儲存至：{report_filepath}")
    except IOError as e:
        print(f"\n錯誤：儲存報告至檔案失敗：{e}")

    report_pipeline_end_time = datetime.now()
    report_duration_seconds = (report_pipeline_end_time - report_pipeline_start_time).total_seconds()
    print(f"報告生成流程執行時長: {report_duration_seconds:.2f} 秒")
    print("--- 報告生成流程結束 ---")


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
