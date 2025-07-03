import argparse
import pathlib
import sys
import duckdb
import pandas as pd

# --- 路徑自我校正 ---
# 確保腳本可以從任何地方執行，並且能夠正確地找到其兄弟模組
# (例如 finmind_client.py, db_manager.py)
current_file_path = pathlib.Path(__file__).resolve()
# 微應用的根目錄 (例如 apps/finmind_data_ingestor)
app_root = current_file_path.parent
# 專案的根目錄 (例如 apps/ 的上一層)
project_root = app_root.parent.parent

# 將微應用根目錄和專案根目錄加入到 Python 的模組搜索路徑中
# 這樣可以確保 `from . import finmind_client` 或 `from apps.finmind_data_ingestor import ...`
# 這樣的導入語句能夠正常工作
if str(app_root) not in sys.path:
    sys.path.insert(0, str(app_root))
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- 結束路徑自我校正 ---

# 現在可以安全地從兄弟模組導入
try:
    from . import finmind_client
    from . import db_manager
except ImportError:
    # 如果上面的相對導入失敗（例如，當直接從 apps/ 目錄運行此腳本時）
    # 嘗試使用 apps.finmind_data_ingestor 的絕對路徑導入
    # 這假設 apps 目錄在 PYTHONPATH 中，或者腳本是從專案根目錄運行的
    import finmind_client
    import db_manager

def main():
    parser = argparse.ArgumentParser(description="FinMind 數據擷取器：用於獲取台灣市場數據並儲存到 DuckDB。")

    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        choices=["TaiwanStockInfo", "TaiwanStockPrice", "TaiwanStockFinancialStatements", "TaiwanStockTotalInstitutionalInvestors"],
        help="要抓取的數據集名稱。"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="查詢起始日期 (YYYY-MM-DD)。對於 TaiwanStockInfo 非必要。"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="查詢結束日期 (YYYY-MM-DD)。主要用於 TaiwanStockPrice 和 TaiwanStockTotalInstitutionalInvestors。"
    )
    parser.add_argument(
        "--stock-id",
        type=str,
        default=None,
        help="股票代號。對於 TaiwanStockPrice 和 TaiwanStockFinancialStatements 為必要參數。"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data_workspace/finmind_tw_data.duckdb",
        help="DuckDB 資料庫檔案的路徑。預設：data_workspace/finmind_tw_data.duckdb"
    )

    args = parser.parse_args()

    # 確保 data_workspace 目錄存在
    db_file_path = pathlib.Path(args.db_path)
    db_file_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"資料庫路徑: {db_file_path.resolve()}")

    conn = None  # 初始化 conn
    try:
        conn = duckdb.connect(database=str(db_file_path), read_only=False)
        print("成功連接到 DuckDB。")
        db_manager.create_tables(conn)

        df = pd.DataFrame()
        table_name = ""

        if args.dataset == "TaiwanStockInfo":
            table_name = "taiwan_stock_info"
            df = finmind_client.get_stock_info()
        elif args.dataset == "TaiwanStockPrice":
            table_name = "taiwan_stock_price"
            if not args.stock_id or not args.start_date or not args.end_date:
                parser.error("--stock-id, --start-date, 和 --end-date 對於 TaiwanStockPrice 是必要參數。")
            df = finmind_client.get_stock_price(args.stock_id, args.start_date, args.end_date)
        elif args.dataset == "TaiwanStockFinancialStatements":
            table_name = "taiwan_stock_financial_statements"
            if not args.stock_id or not args.start_date:
                parser.error("--stock-id 和 --start-date 對於 TaiwanStockFinancialStatements 是必要參數。")
            df = finmind_client.get_financial_statements(args.stock_id, args.start_date)
        elif args.dataset == "TaiwanStockTotalInstitutionalInvestors":
            table_name = "taiwan_stock_total_institutional_investors"
            if not args.start_date or not args.end_date:
                parser.error("--start-date 和 --end-date 對於 TaiwanStockTotalInstitutionalInvestors 是必要參數。")
            df = finmind_client.get_total_institutional_investors(args.start_date, args.end_date)

        if not df.empty:
            print(f"成功從 API 獲取 {len(df)} 筆 '{args.dataset}' 數據。")
            db_manager.save_data(df, table_name, conn)
        elif df is not None: # df 可能是空的 DataFrame，而不是 None
             print(f"從 API 未獲取到 '{args.dataset}' 數據，或獲取到的數據為空。")
        else: # 理論上 fetch_finmind_api 總是返回 DataFrame，即使是空的
             print(f"從 API 獲取 '{args.dataset}' 數據失敗。")


    except Exception as e:
        print(f"❌ 在主執行流程中發生錯誤: {e}")
    finally:
        if conn:
            conn.close()
            print("已關閉 DuckDB 連接。")

if __name__ == "__main__":
    # 範例執行指令 (在終端機中，進入 apps/finmind_data_ingestor 目錄後執行):
    # python run.py --dataset TaiwanStockInfo
    # python run.py --dataset TaiwanStockPrice --stock-id 2330 --start-date 2024-01-01 --end-date 2024-01-05
    # python run.py --dataset TaiwanStockFinancialStatements --stock-id 2330 --start-date 2023-01-01
    # python run.py --dataset TaiwanStockTotalInstitutionalInvestors --start-date 2024-01-01 --end-date 2024-01-05 --db-path my_custom_db.duckdb
    main()
