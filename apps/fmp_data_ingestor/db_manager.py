# -*- coding: utf-8 -*-
"""
DuckDB 資料庫管理模組 for FMP Data Ingestor。
負責處理所有與 DuckDB 的互動，例如建立資料表、寫入公司基本資料和財務報表數據。
"""
import duckdb
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

class DBManager:
    """
    DuckDB 資料庫管理器 for FMP Data.

    提供方法來建立資料庫連線、建立資料表以及儲存公司基本資料和財務報表。
    """
    def __init__(self, db_path: str):
        """
        初始化 DBManager。

        Args:
            db_path (str): DuckDB 資料庫檔案的路徑。
        """
        self.db_path = db_path
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"已建立資料庫目錄: {db_dir}")
        logger.info(f"DBManager (FMP Data Ingestor) 初始化完畢，資料庫路徑: {self.db_path}")

    def create_tables(self):
        """
        建立 company_profiles 和 income_statements 資料表（如果它們尚不存在）。
        """
        try:
            with duckdb.connect(self.db_path) as con:
                # 建立 company_profiles 資料表
                # 欄位將根據 get_company_profile 回傳的字典動態決定，但主鍵是 symbol
                # 為簡化起見，我們將所有 profile 數據儲存在一個 JSON/TEXT 欄位或多個通用欄位
                # 這裡選擇較為明確的欄位定義，但實際應用中可能需要更彈性的結構或 JSON 欄位
                con.execute("""
                CREATE TABLE IF NOT EXISTS company_profiles (
                    symbol VARCHAR PRIMARY KEY,
                    companyName VARCHAR,
                    currency VARCHAR,
                    isin VARCHAR,
                    exchangeShortName VARCHAR,
                    industry VARCHAR,
                    website VARCHAR,
                    description TEXT,
                    ceo VARCHAR,
                    sector VARCHAR,
                    country VARCHAR,
                    fullTimeEmployees VARCHAR,
                    phone VARCHAR,
                    address VARCHAR,
                    city VARCHAR,
                    state VARCHAR,
                    zip VARCHAR,
                    dcfDiff REAL,
                    dcf REAL,
                    image VARCHAR,
                    ipoDate DATE,
                    defaultImage BOOLEAN,
                    isEtf BOOLEAN,
                    isActivelyTrading BOOLEAN,
                    isAdr BOOLEAN,
                    isFund BOOLEAN,
                    lastDiv REAL,
                    range VARCHAR,
                    beta REAL,
                    volAvg BIGINT,
                    mktCap BIGINT,
                    price REAL,
                    changes REAL,
                    cik VARCHAR,
                    cusip VARCHAR,
                    exchange VARCHAR,
                    -- 儲存整個 profile 的 JSON 字串，以備不時之需或未來擴展
                    raw_json_data TEXT,
                    fetched_at TIMESTAMPTZ DEFAULT current_timestamp
                );
                """)
                logger.info("資料表 'company_profiles' 已在資料庫中準備就緒。")

                # 建立 income_statements 資料表
                # 欄位將根據 get_financial_statements 回傳的字典列表動態決定
                # 主鍵是 (symbol, date, period)
                con.execute("""
                CREATE TABLE IF NOT EXISTS income_statements (
                    symbol VARCHAR,
                    date DATE,
                    period VARCHAR,         -- 'quarter' 或 'annual'
                    reportedCurrency VARCHAR,
                    cik VARCHAR,
                    fillingDate DATE,
                    acceptedDate TIMESTAMPTZ,
                    calendarYear VARCHAR,
                    revenue BIGINT,
                    costOfRevenue BIGINT,
                    grossProfit BIGINT,
                    grossProfitRatio REAL,
                    researchAndDevelopmentExpenses BIGINT,
                    generalAndAdministrativeExpenses BIGINT,
                    sellingAndMarketingExpenses BIGINT,
                    sellingGeneralAndAdministrativeExpenses BIGINT,
                    otherExpenses BIGINT,
                    operatingExpenses BIGINT,
                    costAndExpenses BIGINT,
                    interestIncome BIGINT,
                    interestExpense BIGINT,
                    depreciationAndAmortization BIGINT,
                    ebitda BIGINT,
                    ebitdaratio REAL,
                    operatingIncome BIGINT,
                    operatingIncomeRatio REAL,
                    totalOtherIncomeExpensesNet BIGINT,
                    incomeBeforeTax BIGINT,
                    incomeBeforeTaxRatio REAL,
                    incomeTaxExpense BIGINT,
                    netIncome BIGINT,
                    netIncomeRatio REAL,
                    eps REAL,
                    epsdiluted REAL,
                    weightedAverageShsOut BIGINT,
                    weightedAverageShsOutDil BIGINT,
                    link VARCHAR,
                    finalLink VARCHAR,
                    -- 儲存整個 statement 的 JSON 字串
                    raw_json_data TEXT,
                    fetched_at TIMESTAMPTZ DEFAULT current_timestamp,
                    PRIMARY KEY (symbol, date, period)
                );
                """)
                logger.info("資料表 'income_statements' 已在資料庫中準備就緒。")

        except Exception as e:
            logger.error(f"建立資料表失敗: {e}")
            raise

    def save_profile(self, profile_data: dict):
        """
        將一個公司 Profile 的字典存入 company_profiles 表。
        使用 INSERT OR REPLACE (UPSERT) 語義。
        """
        if not profile_data or not isinstance(profile_data, dict) or 'symbol' not in profile_data:
            logger.warning("傳入的 profile_data 無效或缺少 'symbol'，略過儲存。")
            return

        symbol = profile_data['symbol']
        logger.info(f"準備儲存公司 Profile: {symbol}")

        # 將字典轉換為適合 SQL 查詢的格式
        # 為了簡化，我們只取部分已知欄位，其餘存入 raw_json_data
        # 實際應用中，可能需要更完善的欄位映射和型態轉換
        profile_columns = [
            "symbol", "companyName", "currency", "isin", "exchangeShortName", "industry",
            "website", "description", "ceo", "sector", "country", "fullTimeEmployees",
            "phone", "address", "city", "state", "zip", "dcfDiff", "dcf", "image",
            "ipoDate", "defaultImage", "isEtf", "isActivelyTrading", "isAdr", "isFund",
            "lastDiv", "range", "beta", "volAvg", "mktCap", "price", "changes",
            "cik", "cusip", "exchange"
        ]

        # 準備插入的值，對於不存在的鍵使用 None
        values_to_insert = {}
        for col in profile_columns:
            values_to_insert[col] = profile_data.get(col)

        # 特殊處理日期和布林值
        if values_to_insert.get("ipoDate"):
            try:
                # FMP API 的 ipoDate 可能是 'YYYY-MM-DD' 或空字串/None
                if values_to_insert["ipoDate"]:
                    pd.to_datetime(values_to_insert["ipoDate"]).strftime('%Y-%m-%d')
                else:
                    values_to_insert["ipoDate"] = None
            except ValueError:
                logger.warning(f"公司 {symbol} 的 ipoDate '{values_to_insert['ipoDate']}' 格式無效，將設為 NULL。")
                values_to_insert["ipoDate"] = None

        for bool_col in ["defaultImage", "isEtf", "isActivelyTrading", "isAdr", "isFund"]:
            if values_to_insert.get(bool_col) is not None:
                values_to_insert[bool_col] = bool(values_to_insert[bool_col])
            else:
                values_to_insert[bool_col] = None # 或 False，視需求而定

        # 將整個 profile_data 轉為 JSON 字串儲存
        import json
        values_to_insert["raw_json_data"] = json.dumps(profile_data)

        # 移除 fetched_at，讓資料庫自動填入
        # values_to_insert["fetched_at"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        cols_for_sql = ", ".join(values_to_insert.keys())
        placeholders = ", ".join(["?"] * len(values_to_insert))

        try:
            with duckdb.connect(self.db_path) as con:
                # 使用 UPSERT (INSERT OR REPLACE)
                # DuckDB 支援 INSERT OR REPLACE INTO table_name VALUES (...)
                # 或者更標準的 INSERT INTO table_name (...) VALUES (...) ON CONFLICT (primary_key_column) DO UPDATE SET ...
                # 這裡使用 INSERT OR REPLACE，因為主鍵是 symbol
                # 我們需要確保欄位順序與 placeholders 一致
                sql_insert = f"INSERT OR REPLACE INTO company_profiles ({cols_for_sql}) VALUES ({placeholders})"
                con.execute(sql_insert, list(values_to_insert.values()))
            logger.info(f"成功儲存/更新公司 Profile: {symbol}")
        except Exception as e:
            logger.error(f"儲存公司 Profile ({symbol}) 失敗: {e}")
            logger.error(f"失敗的資料: {values_to_insert}")
            raise

    def save_financial_statements(self, financial_statements: list[dict]):
        """
        將財務報表（一個字典列表）存入 income_statements 表。
        使用 INSERT OR REPLACE (UPSERT) 語義。
        """
        if not financial_statements or not isinstance(financial_statements, list):
            logger.warning("傳入的 financial_statements 無效或為空，略過儲存。")
            return

        num_statements = len(financial_statements)
        symbol = financial_statements[0].get('symbol', '未知 Symbol') # 假設所有報表都來自同一個 symbol
        logger.info(f"準備儲存 {num_statements} 筆財務報表 ({symbol})")

        # 預期欄位列表 (基於 FMP API income-statement 的常見欄位)
        # 需要與 create_tables 中的定義保持一致
        statement_columns = [
            "symbol", "date", "period", "reportedCurrency", "cik", "fillingDate", "acceptedDate",
            "calendarYear", "revenue", "costOfRevenue", "grossProfit", "grossProfitRatio",
            "researchAndDevelopmentExpenses", "generalAndAdministrativeExpenses",
            "sellingAndMarketingExpenses", "sellingGeneralAndAdministrativeExpenses", "otherExpenses",
            "operatingExpenses", "costAndExpenses", "interestIncome", "interestExpense",
            "depreciationAndAmortization", "ebitda", "ebitdaratio", "operatingIncome",
            "operatingIncomeRatio", "totalOtherIncomeExpensesNet", "incomeBeforeTax",
            "incomeBeforeTaxRatio", "incomeTaxExpense", "netIncome", "netIncomeRatio", "eps",
            "epsdiluted", "weightedAverageShsOut", "weightedAverageShsOutDil", "link", "finalLink"
        ]

        import json
        records_to_insert = []
        for stmt_dict in financial_statements:
            record = {}
            for col in statement_columns:
                record[col] = stmt_dict.get(col)

            # 型態轉換與處理
            for date_col in ["date", "fillingDate"]:
                if record.get(date_col):
                    try:
                        record[date_col] = pd.to_datetime(record[date_col]).strftime('%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"財務報表 {symbol} 日期 {record.get(date_col)} 格式無效，將設為 NULL。")
                        record[date_col] = None
            if record.get("acceptedDate"):
                try:
                    # FMP 的 acceptedDate 通常包含時間，可能帶有毫秒和'Z'
                    record["acceptedDate"] = pd.to_datetime(record["acceptedDate"]).strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    logger.warning(f"財務報表 {symbol} acceptedDate {record.get('acceptedDate')} 格式無效，將設為 NULL。")
                    record["acceptedDate"] = None

            for num_col in [
                "revenue", "costOfRevenue", "grossProfit", "researchAndDevelopmentExpenses",
                "generalAndAdministrativeExpenses", "sellingAndMarketingExpenses",
                "sellingGeneralAndAdministrativeExpenses", "otherExpenses", "operatingExpenses",
                "costAndExpenses", "interestIncome", "interestExpense", "depreciationAndAmortization",
                "ebitda", "operatingIncome", "totalOtherIncomeExpensesNet", "incomeBeforeTax",
                "incomeTaxExpense", "netIncome", "weightedAverageShsOut", "weightedAverageShsOutDil"
            ]:
                if record.get(num_col) is not None:
                    try:
                        record[num_col] = int(record[num_col]) # FMP 通常返回整數
                    except (ValueError, TypeError):
                        logger.warning(f"財務報表 {symbol} 欄位 {col}='{record[num_col]}' 無法轉換為整數，將設為 NULL。")
                        record[num_col] = None

            for float_col in [
                "grossProfitRatio", "ebitdaratio", "operatingIncomeRatio", "incomeBeforeTaxRatio",
                "netIncomeRatio", "eps", "epsdiluted"
            ]:
                if record.get(float_col) is not None:
                    try:
                        record[float_col] = float(record[float_col])
                    except (ValueError, TypeError):
                        logger.warning(f"財務報表 {symbol} 欄位 {col}='{record[float_col]}' 無法轉換為浮點數，將設為 NULL。")
                        record[float_col] = None

            record["raw_json_data"] = json.dumps(stmt_dict)
            records_to_insert.append(tuple(record.get(col_name) for col_name in statement_columns + ["raw_json_data"]))


        if not records_to_insert:
            logger.warning(f"沒有可儲存的財務報表數據 for {symbol}。")
            return

        cols_for_sql = ", ".join(statement_columns + ["raw_json_data"])
        placeholders = ", ".join(["?"] * len(records_to_insert[0])) # 根據第一條記錄的欄位數

        try:
            with duckdb.connect(self.db_path) as con:
                # 使用 INSERT OR REPLACE INTO，因為主鍵是 (symbol, date, period)
                # DuckDB的 executemany 配合 INSERT OR REPLACE
                # 確保 statement_columns + ["raw_json_data"] 的順序與 record.get 的順序一致
                sql_insert = f"INSERT OR REPLACE INTO income_statements ({cols_for_sql}) VALUES ({placeholders})"
                con.executemany(sql_insert, records_to_insert)
            logger.info(f"成功儲存/更新 {len(records_to_insert)} 筆財務報表 for {symbol}。")
        except Exception as e:
            logger.error(f"儲存財務報表 ({symbol}) 失敗: {e}")
            logger.error(f"失敗的資料 (第一筆): {records_to_insert[0] if records_to_insert else 'N/A'}")
            raise

if __name__ == '__main__':
    # 測試用的設定
    test_db_path_fmp = "data_workspace/temp/test_fmp_data.duckdb"
    if os.path.exists(test_db_path_fmp):
        os.remove(test_db_path_fmp)

    # 配置日誌記錄器以查看輸出
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    db_manager_fmp = DBManager(db_path=test_db_path_fmp)

    print("\n--- 測試 FMP DBManager ---")
    print("--- 測試 1: 建立資料表 (company_profiles, income_statements) ---")
    db_manager_fmp.create_tables()

    print("\n--- 測試 2: 儲存公司 Profile ---")
    sample_profile_aapl = {
        "symbol": "AAPL", "price": 150.0, "beta": 1.2, "volAvg": 75000000, "mktCap": 2500000000000,
        "lastDiv": 0.88, "range": "130-180", "changes": -1.5, "companyName": "Apple Inc.",
        "currency": "USD", "isin": "US0378331005", "cusip": "037833100", "exchange": "NASDAQ Global Select",
        "exchangeShortName": "NASDAQ", "industry": "Consumer Electronics", "website": "https://www.apple.com",
        "description": "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide.",
        "ceo": "Mr. Timothy D. Cook", "sector": "Technology", "country": "US", "fullTimeEmployees": "154000",
        "phone": "1.408.996.1010", "address": "One Apple Park Way", "city": "Cupertino", "state": "CA", "zip": "95014",
        "dcfDiff": 0.15, "dcf": 160.0, "image": "https://financialmodelingprep.com/image-stock/AAPL.png",
        "ipoDate": "1980-12-12", "defaultImage": True, "isEtf": False, "isActivelyTrading": True,
        "isAdr": False, "isFund": False, "cik": "0000320193"
    }
    sample_profile_msft = {
        "symbol": "MSFT", "companyName": "Microsoft Corporation", "price": 300.0, "ipoDate": "1986-03-13",
        "industry": "Software - Infrastructure", "sector": "Technology", "country": "US",
        "description": "Microsoft Corp is a technology company...", "isActivelyTrading": True
        # 其他欄位可以省略以測試 get 的預設行為
    }
    db_manager_fmp.save_profile(sample_profile_aapl)
    db_manager_fmp.save_profile(sample_profile_msft)

    with duckdb.connect(test_db_path_fmp) as con:
        aapl_profile_from_db = con.execute("SELECT * FROM company_profiles WHERE symbol = 'AAPL'").fetchdf()
        print(f"從資料庫讀取的 AAPL Profile:\n{aapl_profile_from_db}")
        assert not aapl_profile_from_db.empty
        assert aapl_profile_from_db['companyName'].iloc[0] == "Apple Inc."
        assert aapl_profile_from_db['ipoDate'].iloc[0] == pd.to_datetime("1980-12-12").date() # DuckDB stores as date

    print("\n--- 測試 3: 儲存財務報表 (損益表) ---")
    sample_income_statements_aapl = [
        {
            "date": "2023-09-30", "symbol": "AAPL", "reportedCurrency": "USD", "cik": "0000320193",
            "fillingDate": "2023-10-26", "acceptedDate": "2023-10-26T18:00:00.000Z",
            "calendarYear": "2023", "period": "Q4", "revenue": 90146000000, "costOfRevenue": 52051000000,
            "grossProfit": 38095000000, "grossProfitRatio": 0.4226,
            "researchAndDevelopmentExpenses": 7307000000, "sellingGeneralAndAdministrativeExpenses": 6938000000,
            "operatingExpenses": 14245000000, "operatingIncome": 23850000000, "netIncome": 20721000000,
            "eps": 1.29, "epsdiluted": 1.29, "weightedAverageShsOut": 16072000000,
            "weightedAverageShsOutDil": 16164000000,
            "link": "https://www.sec.gov/Archives/edgar/data/320193/.../aapl-20230930.htm",
            "finalLink": "https://www.sec.gov/Archives/edgar/data/320193/.../aapl-20230930.htm"
        },
        {
            "date": "2023-06-30", "symbol": "AAPL", "reportedCurrency": "USD", "cik": "0000320193",
            "fillingDate": "2023-07-27", "acceptedDate": "2023-07-27T18:00:00.000Z",
            "calendarYear": "2023", "period": "Q3", "revenue": 81797000000, "costOfRevenue": 45384000000,
            "grossProfit": 36413000000, "grossProfitRatio": 0.4451,
            "researchAndDevelopmentExpenses": 7442000000, "sellingGeneralAndAdministrativeExpenses": 6151000000,
            "operatingExpenses": 13593000000, "operatingIncome": 22820000000, "netIncome": 19881000000,
            "eps": 1.26, "epsdiluted": 1.27, "weightedAverageShsOut": 15728000000,
            "weightedAverageShsOutDil": 15813000000,
            "link": "https://www.sec.gov/Archives/edgar/data/320193/.../aapl-20230630.htm",
            "finalLink": "https://www.sec.gov/Archives/edgar/data/320193/.../aapl-20230630.htm"
        }
    ]
    db_manager_fmp.save_financial_statements(sample_income_statements_aapl)

    with duckdb.connect(test_db_path_fmp) as con:
        aapl_stmts_from_db = con.execute("SELECT * FROM income_statements WHERE symbol = 'AAPL' ORDER BY date DESC").fetchdf()
        print(f"從資料庫讀取的 AAPL 損益表:\n{aapl_stmts_from_db.head()}")
        assert len(aapl_stmts_from_db) == 2
        assert aapl_stmts_from_db['revenue'].iloc[0] == 90146000000
        assert aapl_stmts_from_db['period'].iloc[0] == "Q4"

    print("\n--- 測試 4: 更新 Profile 和 Statement (UPSERT) ---")
    updated_profile_aapl = sample_profile_aapl.copy()
    updated_profile_aapl["price"] = 155.0 # 更新價格
    updated_profile_aapl["description"] = "An updated description for Apple Inc."
    db_manager_fmp.save_profile(updated_profile_aapl)

    with duckdb.connect(test_db_path_fmp) as con:
        updated_aapl_profile = con.execute("SELECT price, description FROM company_profiles WHERE symbol = 'AAPL'").fetchdf()
        assert updated_aapl_profile['price'].iloc[0] == 155.0
        assert "updated description" in updated_aapl_profile['description'].iloc[0]

    updated_statement_q4_aapl = sample_income_statements_aapl[0].copy()
    updated_statement_q4_aapl["revenue"] = 90500000000 # 更新營收
    db_manager_fmp.save_financial_statements([updated_statement_q4_aapl]) # 傳入列表

    with duckdb.connect(test_db_path_fmp) as con:
        updated_aapl_q4_stmt = con.execute("SELECT revenue FROM income_statements WHERE symbol = 'AAPL' AND date = '2023-09-30'").fetchdf()
        assert updated_aapl_q4_stmt['revenue'].iloc[0] == 90500000000
        # 確保總數仍然是 2，因為是更新
        total_stmts = con.execute("SELECT COUNT(*) FROM income_statements WHERE symbol = 'AAPL'").fetchone()[0]
        assert total_stmts == 2


    print("\n--- FMP DBManager 測試完畢 ---")
    # os.remove(test_db_path_fmp)
    # print(f"INFO: 已刪除測試資料庫 {test_db_path_fmp}")
