# -*- coding: utf-8 -*-
"""
FMP Data Ingestor 主執行入口。

負責接收命令列參數，調用 fmp_client 獲取數據，並使用 db_manager 將數據儲存到資料庫。
"""
import argparse
import os
import sys
import logging

# --- 路徑自我校正樣板碼 START ---
# 如果是從專案根目錄執行 (例如：python -m apps.fmp_data_ingestor.run --action profile --symbol AAPL)
# 則 __file__ 會是 apps/fmp_data_ingestor/run.py，其 dirname 是 apps/fmp_data_ingestor
# 往上兩層即為專案根目錄
# 如果是直接執行 scripts/run_fmp_ingestor.sh，且 cwd 在專案根目錄
# 則 sys.path[0] 可能會是專案根目錄的 apps/fmp_data_ingestor 或 scripts
# 我們希望將專案根目錄加入 sys.path
current_file_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_file_dir, "..", ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"[fmp_data_ingestor.run] INFO: 已將專案根目錄 {project_root} 加入 sys.path")

try:
    from apps.fmp_data_ingestor.fmp_client import get_fmp_api_key, get_company_profile, get_financial_statements
    from apps.fmp_data_ingestor.db_manager import DBManager
except ModuleNotFoundError:
    print("[fmp_data_ingestor.run] ERROR: 無法導入模組。請確保從專案根目錄執行，或者 PYTHONPATH 設定正確。")
    # 嘗試相對導入 (如果 run.py 是作為套件的一部分被調用)
    from .fmp_client import get_fmp_api_key, get_company_profile, get_financial_statements
    from .db_manager import DBManager
# --- 路徑自我校正樣板碼 END ---

# 設定日誌
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    主執行函數。
    解析命令列參數，獲取 FMP API 金鑰，然後根據指定的動作執行數據擷取和儲存。
    """
    parser = argparse.ArgumentParser(description="美股基本面擷取器 - 從 Financial Modeling Prep API 獲取數據並儲存。")
    parser.add_argument("--action", type=str, required=True, choices=['profile', 'financials'],
                        help="要執行的動作: 'profile' (公司基本資料) 或 'financials' (財務報表)。")
    parser.add_argument("--symbol", type=str, required=True,
                        help="目標股票代碼 (例如: AAPL)。")
    parser.add_argument("--period", type=str, default="quarter", choices=['quarter', 'annual'],
                        help="財報週期 (僅 'financials' 動作需要): 'quarter' (預設) 或 'annual'。")
    parser.add_argument("--limit", type=int, default=5,
                        help="要獲取的財報期數 (僅 'financials' 動作需要，預設為 5)。")
    parser.add_argument("--db-path", type=str, required=True,
                        help="DuckDB 資料庫的儲存路徑 (例如: data_workspace/fmp_data.duckdb)。")

    args = parser.parse_args()

    logger.info(f"接收到指令: Action={args.action}, Symbol={args.symbol}, DBPath={args.db_path}")
    if args.action == 'financials':
        logger.info(f"Financials 參數: Period={args.period}, Limit={args.limit}")

    api_key = get_fmp_api_key()
    if not api_key:
        logger.error("無法獲取 FMP API 金鑰。請檢查環境變數 API_KEY_FMP 是否已設定。程式即將終止。")
        sys.exit(1) # 異常退出

    db_manager = DBManager(db_path=args.db_path)
    # 確保資料表已建立 (如果不存在)
    try:
        db_manager.create_tables()
    except Exception as e:
        logger.error(f"建立資料庫資料表時發生錯誤: {e}。程式即將終止。")
        sys.exit(1)


    if args.action == "profile":
        logger.info(f"正在為 {args.symbol} 獲取公司基本資料...")
        profile_data = get_company_profile(api_key=api_key, symbol=args.symbol)
        if profile_data:
            try:
                db_manager.save_profile(profile_data)
                logger.info(f"成功儲存 {args.symbol} 的公司基本資料到資料庫。")
            except Exception as e:
                logger.error(f"儲存 {args.symbol} 公司基本資料到資料庫時發生錯誤: {e}")
        else:
            logger.warning(f"未能獲取 {args.symbol} 的公司基本資料。")

    elif args.action == "financials":
        logger.info(f"正在為 {args.symbol} 獲取財務報表 (週期: {args.period}, 期數: {args.limit})...")
        financial_data = get_financial_statements(api_key=api_key, symbol=args.symbol, period=args.period, limit=args.limit)
        if financial_data:
            try:
                db_manager.save_financial_statements(financial_data)
                logger.info(f"成功儲存 {args.symbol} 的財務報表到資料庫。")
            except Exception as e:
                logger.error(f"儲存 {args.symbol} 財務報表到資料庫時發生錯誤: {e}")
        else:
            logger.warning(f"未能獲取 {args.symbol} 的財務報表。")
    else:
        # argparse 的 choices 應該已經處理了這個情況，但為了周全還是加上
        logger.error(f"未知的動作: {args.action}。請使用 'profile' 或 'financials'。")
        sys.exit(1)

    logger.info(f"任務完成: Action={args.action}, Symbol={args.symbol}")

if __name__ == "__main__":
    main()
