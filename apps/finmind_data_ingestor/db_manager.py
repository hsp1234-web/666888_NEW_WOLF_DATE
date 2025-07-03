import duckdb
import pandas as pd

def create_tables(db_conn: duckdb.DuckDBPyConnection) -> None:
    """
    在 DuckDB 中創建所需的資料表 (如果它們尚不存在)。
    欄位名稱和類型是根據 FinMind API 回傳的 DataFrame 推斷的。
    """
    # TaiwanStockInfo Table
    # 欄位範例: date, stock_id, stock_name, industry_category, type
    # 注意：FinMind API 回傳的欄位類型可能都是字串，這裡盡可能推斷合適類型
    # DuckDB 在從 Pandas 插入時會自動推斷類型，但明確定義更好
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS taiwan_stock_info (
            date VARCHAR,
            stock_id VARCHAR,
            stock_name VARCHAR,
            industry_category VARCHAR,
            type VARCHAR,
            PRIMARY KEY (stock_id) -- 假設 stock_id 是唯一的，或者 (stock_id, date) 如果每日更新
        );
    """)
    print("資料表 'taiwan_stock_info' 已確認/創建。")

    # TaiwanStockPrice Table
    # 欄位範例: date, stock_id, Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover
    # 假設 Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover 應為數值
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS taiwan_stock_price (
            date VARCHAR,
            stock_id VARCHAR,
            Trading_Volume DOUBLE,
            Trading_money DOUBLE,
            open DOUBLE,
            max DOUBLE,
            min DOUBLE,
            close DOUBLE,
            spread DOUBLE,
            Trading_turnover DOUBLE,
            PRIMARY KEY (stock_id, date)
        );
    """)
    print("資料表 'taiwan_stock_price' 已確認/創建。")

    # TaiwanStockFinancialStatements Table
    # 欄位範例: date, stock_id, type (會計科目英文名), value, origin_name (會計科目中文名)
    # value 應為數值
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS taiwan_stock_financial_statements (
            date VARCHAR,
            stock_id VARCHAR,
            type VARCHAR,       -- 會計科目英文名
            origin_name VARCHAR, -- 會計科目中文名
            value DOUBLE,
            PRIMARY KEY (stock_id, date, type) -- 假設這三者組合是唯一的
        );
    """)
    print("資料表 'taiwan_stock_financial_statements' 已確認/創建。")

    # TaiwanStockTotalInstitutionalInvestors Table
    # 欄位範例: date, name (法人別), buy, sell, diff
    # buy, sell, diff 應為數值
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS taiwan_stock_total_institutional_investors (
            date VARCHAR,
            name VARCHAR,       -- 法人別
            buy DOUBLE,
            sell DOUBLE,
            diff DOUBLE,
            PRIMARY KEY (date, name)
        );
    """)
    print("資料表 'taiwan_stock_total_institutional_investors' 已確認/創建。")

def save_data(df: pd.DataFrame, table_name: str, db_conn: duckdb.DuckDBPyConnection) -> None:
    """
    將 DataFrame 數據高效地寫入指定的 DuckDB 資料表。
    使用 INSERT OR REPLACE (透過 DuckDB 的 ON CONFLICT DO REPLACE) 或類似的 UPSERT 邏輯。
    DuckDB 的 `ON CONFLICT DO REPLACE` 需要指定衝突目標 (主鍵)。
    如果主鍵定義正確，可以直接使用 `df.to_sql(table_name, db_conn, if_exists='append', index=False)`
    配合 DuckDB 的衝突處理。但更保險的方式是先註冊 DataFrame，然後使用 SQL 進行插入。

    注意：DuckDB 對於 `INSERT OR REPLACE` 的原生語法是 `INSERT INTO ... ON CONFLICT ... DO UPDATE SET ...`
    或 `INSERT INTO ... ON CONFLICT ... DO NOTHING`.
    對於簡單的 "replace" (如果主鍵衝突就替換整行)，可以先 DELETE 再 INSERT，
    或者如果表結構允許，使用 `CREATE OR REPLACE TABLE temp_table AS SELECT * FROM df; DELETE FROM real_table WHERE pk IN (SELECT pk FROM temp_table); INSERT INTO real_table SELECT * FROM temp_table;`

    為了簡化並遵循指令的 "INSERT OR REPLACE" 意圖，這裡採用先刪除衝突數據再插入的模式。
    這需要知道每個表的主鍵。
    """
    if df.empty:
        print(f"提供的 DataFrame 為空，不對資料表 '{table_name}' 進行任何操作。")
        return

    # 轉換欄位名稱以避免特殊字元問題，並確保與 SQL 相容
    df.columns = [col.replace('(', '_').replace(')', '').replace('%', 'pct') for col in df.columns]

    # 根據表名確定主鍵
    primary_keys = {
        "taiwan_stock_info": ["stock_id"],
        "taiwan_stock_price": ["stock_id", "date"],
        "taiwan_stock_financial_statements": ["stock_id", "date", "type"],
        "taiwan_stock_total_institutional_investors": ["date", "name"]
    }

    if table_name not in primary_keys:
        print(f"錯誤：資料表 '{table_name}' 的主鍵未定義。無法執行 UPSERT 操作。")
        return

    pk_list = primary_keys[table_name]

    # 檢查 DataFrame 是否包含所有主鍵欄位
    missing_pks = [pk for pk in pk_list if pk not in df.columns]
    if missing_pks:
        print(f"錯誤：DataFrame 中缺少主鍵欄位 {missing_pks}，無法對資料表 '{table_name}' 執行 UPSERT 操作。")
        print(f"DataFrame 欄位: {df.columns.tolist()}")
        return

    try:
        # 1. 創建一個暫存表來存放新的 DataFrame 數據
        temp_table_name = f"temp_{table_name}"
        db_conn.register(temp_table_name, df)

        # 2. 從目標表中刪除與暫存表中主鍵匹配的現有記錄
        # 構造 WHERE 子句進行刪除
        # 例如: "t1.stock_id = t2.stock_id AND t1.date = t2.date"
        delete_condition_parts = [f"target.{pk} = source.{pk}" for pk in pk_list]
        delete_condition = " AND ".join(delete_condition_parts)

        delete_sql = f"""
        DELETE FROM {table_name} target
        USING {temp_table_name} source
        WHERE {delete_condition};
        """
        db_conn.execute(delete_sql)
        print(f"已從 '{table_name}' 刪除與新數據衝突的舊記錄。")

        # 3. 將暫存表中的所有數據插入目標表
        # 這裡假設 df 的欄位順序和類型與目標表兼容
        # DuckDB 的 from_df 功能通常能很好地處理類型推斷
        db_conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table_name};")
        print(f"已將新數據成功插入 '{table_name}'。共 {len(df)} 筆記錄。")

    except Exception as e:
        print(f"❌ 在儲存數據到 '{table_name}' 時發生錯誤: {e}")
        print(f"    DataFrame 的前幾行:\n{df.head()}")
    finally:
        # 移除暫存表
        db_conn.unregister(temp_table_name)

if __name__ == '__main__':
    # 簡單測試 db_manager (需要一個 DuckDB 連接)
    db_file = "test_finmind_data.duckdb"
    conn = duckdb.connect(database=db_file, read_only=False)

    print(f"--- 測試資料庫: {db_file} ---")
    create_tables(conn)

    # 創建一些模擬數據進行測試
    # TaiwanStockInfo 模擬數據
    sample_stock_info_data = {
        'date': ['2024-07-01', '2024-07-01'],
        'stock_id': ['2330', '0050'],
        'stock_name': ['台積電', '元大台灣50'],
        'industry_category': ['半導體業', 'ETF'],
        'type': ['twse', 'ETF']
    }
    df_sample_info = pd.DataFrame(sample_stock_info_data)
    print("\n--- 測試儲存 TaiwanStockInfo ---")
    save_data(df_sample_info, "taiwan_stock_info", conn)
    print(conn.execute("SELECT * FROM taiwan_stock_info").fetchdf())

    # 再次儲存相同主鍵的數據 (測試 UPSERT)
    sample_stock_info_data_updated = {
        'date': ['2024-07-02'], # 日期更新
        'stock_id': ['2330'],
        'stock_name': ['台積積電電'], # 名稱更新
        'industry_category': ['半導體'],
        'type': ['TWSE']
    }
    df_sample_info_updated = pd.DataFrame(sample_stock_info_data_updated)
    print("\n--- 測試更新 TaiwanStockInfo (2330) ---")
    save_data(df_sample_info_updated, "taiwan_stock_info", conn)
    print(conn.execute("SELECT * FROM taiwan_stock_info").fetchdf())


    # TaiwanStockPrice 模擬數據
    sample_stock_price_data = {
        'date': ['2024-07-01', '2024-07-01', '2024-07-02'],
        'stock_id': ['2330', '0050', '2330'],
        'Trading_Volume': [1000, 2000, 1500],
        'Trading_money': [100000, 200000, 150000],
        'open': [100.0, 200.0, 101.0],
        'max': [102.0, 202.0, 103.0],
        'min': [99.0, 199.0, 100.0],
        'close': [101.5, 201.5, 102.0],
        'spread': [1.5, 1.5, 1.0],
        'Trading_turnover': [100, 200, 150]
    }
    df_sample_price = pd.DataFrame(sample_stock_price_data)
    print("\n--- 測試儲存 TaiwanStockPrice ---")
    save_data(df_sample_price, "taiwan_stock_price", conn)
    print(conn.execute("SELECT * FROM taiwan_stock_price").fetchdf())

    conn.close()
    print(f"\n測試完成，資料庫 '{db_file}' 已關閉。請手動刪除此測試資料庫檔案。")
