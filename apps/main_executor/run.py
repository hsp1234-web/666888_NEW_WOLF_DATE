# apps/main_executor/run.py
import argparse
import sys
import os # 預期會用到
import logging # 引入 logging 模組

# --- 動態調整 sys.path 以支持作為腳本運行時的模組導入 ---
# 獲取當前腳本 (run.py) 的絕對路徑
_current_script_path = os.path.abspath(__file__)
# main_executor 目錄 (.../apps/main_executor)
_main_executor_dir = os.path.dirname(_current_script_path)
# apps 目錄 (.../apps)
_apps_dir = os.path.dirname(_main_executor_dir)
# 專案根目錄 (...)
_project_root = os.path.dirname(_apps_dir)

# 將專案根目錄和 apps 目錄添加到 sys.path 的開頭，以優先級別導入
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _apps_dir not in sys.path: # 雖然根目錄已加入，但顯式加入 apps 也可以
    sys.path.insert(0, _apps_dir)

# 現在可以使用從 apps 開始的絕對導入路徑
from main_executor.log_utils import setup_logging, log_hardware_metric
# --- sys.path 調整結束 ---

# 預計將來會從這裡導入業務邏輯模組
# from ..daily_market_analyzer import core_analyzer # 示例
# from ..taifex_data_pipeline import core_pipeline # 示例


def main():
    parser = argparse.ArgumentParser(description="鳳凰計畫 - 主任務執行器 v70.0")

    # 核心執行控制參數
    parser.add_argument("--execution-mode", required=True,
                        choices=["standard_analysis", "elt_load", "elt_transform"], # 根據需要擴展
                        help="要執行的主要任務模式。")

    # 通用參數示例 (將在後續步驟中擴展)
    parser.add_argument("--db-path-logs", required=True, help="日誌資料庫 (logs.sqlite) 的完整路徑。")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據 (如果適用於所選模式)。")

    # 標準分析流程相關參數 (示例)
    parser.add_argument("--tickers", help="標準分析流程：要分析的標的列表，以逗號分隔。")
    parser.add_argument("--start-date", help="標準分析流程：數據分析/獲取的起始日期 (YYYY-MM-DD)。")
    parser.add_argument("--end-date", help="標準分析流程：數據分析/獲取的結束日期 (YYYY-MM-DD)。")
    parser.add_argument("--db-path-yfinance-cache", help="標準分析流程：YFinance 快取資料庫的路徑。")

    # ELT 流程相關參數 (示例)
    parser.add_argument("--elt-input-file-path", help="ELT 載入：原始數據檔案的路徑。")
    parser.add_argument("--db-path-raw-taifex", help="ELT 流程：原始 Taifex 資料庫的路徑。")
    parser.add_argument("--db-path-taifex-historical", help="ELT 流程：歷史 Taifex 資料庫的路徑。")
    # ELT 的 pipeline_step 可以通過不同的 execution-mode (elt_load, elt_transform) 來區分，或作為額外參數

    args = parser.parse_args()

    # 設置日誌記錄
    # 注意：根 logger 的級別通常在 setup_logging 中設置。
    # 如果需要更細緻的控制，可以在這裡獲取特定的 logger 實例。
    # logging level 可以通過參數傳入，或者固定
    logger = setup_logging(args.db_path_logs, level=logging.DEBUG) # 設置為 DEBUG 以記錄更多信息

    logger.info(f"主任務執行器啟動。執行模式: {args.execution_mode}")
    logger.debug(f"接收到的完整參數: {args}")

    # 模擬硬體日誌記錄 (示例)
    # 在實際應用中，這可能是一個背景線程或定期任務
    try:
        import psutil # 嘗試導入 psutil 以獲取真實數據，如果失敗則使用假數據
        cpu = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory().percent
    except ImportError:
        logger.warning("psutil 模組未找到，將使用虛假的硬體監控數據。")
        cpu, mem = 10.0, 20.0 # 虛假數據
    except Exception as e:
        logger.warning(f"獲取硬體監控數據時出錯: {e}，將使用虛假的硬體監控數據。")
        cpu, mem = 11.1, 22.2 # 虛假數據

    log_hardware_metric(args.db_path_logs, cpu, mem)
    logger.info(f"已記錄初步硬體指標: CPU={cpu}%, Memory={mem}%")


    # 業務邏輯分派
    try:
        if args.execution_mode == "standard_analysis":
            logger.info(f"開始執行標準分析流程。標的: {args.tickers}, 日期範圍: {args.start_date} 至 {args.end_date}")
            # 此處應調用實際的分析邏輯，例如：
            # result = analyze_markets(tickers=args.tickers, start=args.start_date, end=args.end_date,
            #                          cache_db=args.db_path_yfinance_cache,
            #                          force_refresh=args.force_data_refresh)
            # if result.success:
            #     logger.info("標準分析流程成功完成。")
            # else:
            #     logger.error(f"標準分析流程失敗: {result.error_message}")
            #     sys.exit(1)
            logger.info("模擬：標準分析流程執行完畢。") # 佔位符日誌

        elif args.execution_mode == "elt_load":
            logger.info(f"開始執行 ELT 載入流程。輸入檔案: {args.elt_input_file_path}, 原始資料庫: {args.db_path_raw_taifex}")
            # result = run_elt_load(input_file=args.elt_input_file_path, raw_db=args.db_path_raw_taifex, ...)
            logger.info("模擬：ELT 載入流程執行完畢。") # 佔位符日誌

        elif args.execution_mode == "elt_transform":
            logger.info(f"開始執行 ELT 轉換流程。原始資料庫: {args.db_path_raw_taifex}, 歷史資料庫: {args.db_path_taifex_historical}")
            # result = run_elt_transform(raw_db=args.db_path_raw_taifex, historical_db=args.db_path_taifex_historical, ...)
            logger.info("模擬：ELT 轉換流程執行完畢。") # 佔位符日誌

        else:
            logger.error(f"未知的執行模式: {args.execution_mode}")
            sys.exit(1)

        logger.info(f"模式 {args.execution_mode} 成功執行完畢。")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"執行模式 {args.execution_mode} 時發生未處理的致命錯誤: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # 可以在此處添加專案路徑設置，以確保 main_executor 可以找到其他 apps 子目錄中的模組
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # project_root = os.path.dirname(os.path.dirname(current_dir)) # 退兩層到專案根目錄
    # if project_root not in sys.path:
    #     sys.path.insert(0, project_root)
    main()
