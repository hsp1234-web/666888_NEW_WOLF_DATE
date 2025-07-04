# -*- coding: utf-8 -*-
"""
DuckDB 資料庫管理模組 for Daily Market Analyzer。
負責處理所有與 DuckDB 的互動，例如建立資料表、寫入數據等。
"""
import duckdb
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

class DBManager:
    """
    DuckDB 資料庫管理器。

    提供方法來建立資料庫連線、建立資料表以及高效地寫入 (UPSERT) DataFrame 數據。
    此版本適用於 Daily Market Analyzer，處理包含 'interval' 欄位的數據，並提供查詢功能。
    """
    def __init__(self, db_path: str, duckdb_config: dict | None = None):
        """
        初始化 DBManager。

        Args:
            db_path (str): DuckDB 資料庫檔案的路徑。
            duckdb_config (dict | None, optional): DuckDB 連線的配置選項。預設為 None。
        """
        self.db_path = db_path
        self.duckdb_config = duckdb_config if duckdb_config else {} # 確保是字典以便合併
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"INFO: 已建立資料庫目錄: {db_dir}")

        config_info = f"使用配置: {self.duckdb_config}" if self.duckdb_config else "使用預設配置"
        print(f"INFO: DBManager (Daily Market Analyzer v33.0) 初始化完畢，資料庫路徑: {self.db_path}。{config_info}")


    def create_ohlcv_table(self, table_name: str = "market_ohlcv_analyzer"):
        """
        建立市場 OHLCV（開高低收量）數據表，如果該表尚不存在。
        包含 'interval' 和 'ticker' 欄位。
        主鍵為 (ticker, datetime, interval) 以確保唯一性。
        """
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            datetime TIMESTAMPTZ NOT NULL,
            ticker VARCHAR NOT NULL,
            interval VARCHAR NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume BIGINT,
            PRIMARY KEY (ticker, datetime, interval)
        );
        """
        try:
            with duckdb.connect(database=self.db_path, config=self.duckdb_config) as con:
                con.execute(create_sql)
            print(f"INFO: 資料表 '{table_name}' 已在資料庫 '{self.db_path}' 中準備就緒 (包含 interval 欄位)。")
        except Exception as e:
            print(f"錯誤: 建立資料表 '{table_name}' 失敗: {e}")
            raise

    def upsert_data(self, df: pd.DataFrame, table_name: str):
        """
        使用 DuckDB 的 `INSERT OR REPLACE INTO` 功能高效地將 DataFrame 數據寫入指定資料表。
        此版本預期 DataFrame 已包含 'ticker' 和 'interval' 欄位。
        """
        if df.empty:
            current_ticker = "未知Ticker"
            if 'ticker' in df.columns and not df.empty:
                current_ticker = df['ticker'].iloc[0]
            elif hasattr(df, 'name') and df.name:
                 current_ticker = df.name
            print(f"INFO: 傳入的 DataFrame ({current_ticker}) 為空，無需寫入資料表 '{table_name}'。")
            return

        df_to_insert = df.copy()
        if isinstance(df_to_insert.index, pd.DatetimeIndex):
            df_to_insert = df_to_insert.reset_index()
        df_to_insert.columns = [col.lower() for col in df_to_insert.columns]

        if 'index' in df_to_insert.columns and 'datetime' not in df_to_insert.columns:
            df_to_insert.rename(columns={'index': 'datetime'}, inplace=True)
            print(f"資訊 (DBManager): 將來自索引的 'index' 欄位重命名為 'datetime'。")

        if 'datetime' not in df_to_insert.columns and 'date' in df_to_insert.columns:
            print(f"警告 (DBManager): DataFrame 中缺少 'datetime' 欄位，但找到了 'date' 欄位。將自動重命名 'date' 為 'datetime'。建議上游模組應直接提供 'datetime' 欄位。")
            df_to_insert.rename(columns={'date': 'datetime'}, inplace=True)

        if 'ticker' not in df_to_insert.columns and hasattr(df, 'name') and df.name:
             df_to_insert['ticker'] = df.name

        required_cols = ['datetime', 'ticker', 'interval', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df_to_insert.columns]
        if missing_cols:
            current_ticker_for_error = df_to_insert['ticker'].iloc[0] if 'ticker' in df_to_insert.columns and not df_to_insert.empty else "未知 Ticker"
            print(f"錯誤: DataFrame ({current_ticker_for_error}) 缺少必要欄位: {', '.join(missing_cols)}。無法寫入資料表 '{table_name}'。")
            df_to_insert.info()
            return

        df_to_insert = df_to_insert[required_cols]
        try:
            if not pd.api.types.is_datetime64_any_dtype(df_to_insert['datetime']):
                df_to_insert['datetime'] = pd.to_datetime(df_to_insert['datetime'])
            if df_to_insert['datetime'].dt.tz is None:
                df_to_insert['datetime'] = df_to_insert['datetime'].dt.tz_localize('UTC')
            else:
                df_to_insert['datetime'] = df_to_insert['datetime'].dt.tz_convert('UTC')

            for col in ['open', 'high', 'low', 'close']:
                df_to_insert[col] = pd.to_numeric(df_to_insert[col], errors='raise')
            df_to_insert['volume'] = df_to_insert['volume'].astype('int64')
            df_to_insert['ticker'] = df_to_insert['ticker'].astype(str)
            df_to_insert['interval'] = df_to_insert['interval'].astype(str)
        except Exception as e:
            current_ticker_for_error = df_to_insert['ticker'].iloc[0] if 'ticker' in df_to_insert.columns and not df_to_insert.empty else "未知 Ticker"
            print(f"錯誤: DataFrame ({current_ticker_for_error}) 數據類型轉換失敗: {e}")
            df_to_insert.info()
            return

        try:
            with duckdb.connect(database=self.db_path, config=self.duckdb_config) as con:
                con.register('df_view_to_insert', df_to_insert)
                columns_str = ", ".join(required_cols)
                upsert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns_str}) SELECT {columns_str} FROM df_view_to_insert"
                con.execute(upsert_sql)
                con.unregister('df_view_to_insert')

            current_ticker = df_to_insert['ticker'].iloc[0]
            current_interval = df_to_insert['interval'].iloc[0]
            print(f"INFO: 成功將 {len(df_to_insert)} 筆來自 '{current_ticker}' (顆粒度: {current_interval}) 的數據寫入/更新至資料表 '{table_name}'。")
        except Exception as e:
            current_ticker_for_error = df_to_insert['ticker'].iloc[0] if 'ticker' in df_to_insert.columns and not df_to_insert.empty else "未知 Ticker"
            print(f"錯誤: 寫入數據到資料表 '{table_name}' 失敗 (Ticker: {current_ticker_for_error}): {e}")
            print(f"DEBUG: 嘗試寫入的 DataFrame ({current_ticker_for_error}) info:")
            df_to_insert.info()

    def query_data_for_day(self, ticker: str, date_str: str, table_name: str = "market_ohlcv_analyzer") -> pd.DataFrame:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            start_of_day = f"{date_str} 00:00:00"
            start_of_next_day = (target_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
            query = f"""
            SELECT * FROM {table_name}
            WHERE ticker = ? AND datetime >= CAST(? AS TIMESTAMPTZ) AND datetime < CAST(? AS TIMESTAMPTZ)
            ORDER BY datetime ASC
            """
            with duckdb.connect(database=self.db_path, config=self.duckdb_config) as con:
                result_df = con.execute(query, [ticker, start_of_day, start_of_next_day]).fetchdf()

            if not result_df.empty and 'datetime' in result_df.columns:
                result_df['datetime'] = pd.to_datetime(result_df['datetime'])
                if result_df['datetime'].dt.tz is None:
                    result_df['datetime'] = result_df['datetime'].dt.tz_localize('UTC')
                else:
                    result_df['datetime'] = result_df['datetime'].dt.tz_convert('UTC')
                result_df = result_df.set_index('datetime')
            return result_df
        except Exception as e:
            print(f"錯誤: 查詢 {ticker} 在 {date_str} 的數據失敗: {e}")
            return pd.DataFrame()

    def query_previous_day_close(self, ticker: str, current_date_str: str, table_name: str = "market_ohlcv_analyzer", max_lookback_days: int = 30) -> float | None:
        try:
            current_date_obj = datetime.strptime(current_date_str, "%Y-%m-%d").date()
            for i in range(1, max_lookback_days + 1):
                prev_date_to_check = current_date_obj - timedelta(days=i)
                prev_date_to_check_str = prev_date_to_check.strftime("%Y-%m-%d")
                daily_data_df = self.query_data_for_day(ticker, prev_date_to_check_str, table_name)
                if not daily_data_df.empty:
                    daily_1d_data = daily_data_df[daily_data_df['interval'] == '1d']
                    if not daily_1d_data.empty:
                        return daily_1d_data['close'].iloc[-1]
                    else:
                        return daily_data_df['close'].iloc[-1]
            return None
        except Exception as e:
            print(f"錯誤: 查詢 {ticker} 在 {current_date_str} 之前的收盤價失敗: {e}")
            return None

    def check_cache(self, ticker: str, start_date_str: str, end_date_str: str, interval: str, table_name: str = "market_ohlcv_analyzer") -> tuple[pd.DataFrame, list[str]]:
        """
        檢查快取中指定 ticker、日期範圍和 interval 的數據。
        Args:
            ticker (str): 股票代碼。
            start_date_str (str): 開始日期 (YYYY-MM-DD)。
            end_date_str (str): 結束日期 (YYYY-MM-DD)。
            interval (str): 數據顆粒度 (例如 '1d', '1h', '1m')。
            table_name (str, optional): 數據表名稱。預設為 "market_ohlcv_analyzer"。
        Returns:
            tuple[pd.DataFrame, list[str]]:
                - cached_df (pd.DataFrame): 快取中已存在的數據，按時間升序排列。
                - missing_dates (list[str]): 在請求範圍內，但在快取中缺失的日期字串列表 (YYYY-MM-DD)。
        """
        print(f"DEBUG: check_cache: ticker={ticker}, start={start_date_str}, end={end_date_str}, interval={interval}")
        cached_df = pd.DataFrame()
        missing_dates = []
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError as e:
            print(f"錯誤 (check_cache): 日期格式錯誤: {e}")
            return pd.DataFrame(), []

        requested_dates_set = set()
        current_date = start_date
        while current_date <= end_date:
            requested_dates_set.add(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        query_start_ts = f"{start_date_str} 00:00:00"
        query_end_ts = (end_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        query = f"""
        SELECT * FROM {table_name}
        WHERE ticker = ? AND interval = ?
          AND datetime >= CAST(? AS TIMESTAMPTZ)
          AND datetime < CAST(? AS TIMESTAMPTZ)
        ORDER BY datetime ASC
        """
        try:
            with duckdb.connect(database=self.db_path, config=self.duckdb_config) as con:
                result_df = con.execute(query, [ticker, interval, query_start_ts, query_end_ts]).fetchdf()
            if not result_df.empty and 'datetime' in result_df.columns:
                result_df['datetime'] = pd.to_datetime(result_df['datetime'])
                if result_df['datetime'].dt.tz is None:
                    result_df['datetime'] = result_df['datetime'].dt.tz_localize('UTC')
                else:
                    result_df['datetime'] = result_df['datetime'].dt.tz_convert('UTC')
                cached_df = result_df
                cached_dates_set = set(cached_df['datetime'].dt.strftime('%Y-%m-%d').unique())
                print(f"DEBUG: check_cache: Requested dates set: {sorted(list(requested_dates_set))}")
                print(f"DEBUG: check_cache: Cached dates set for {ticker} ({interval}): {sorted(list(cached_dates_set))}")
                missing_dates_set = requested_dates_set - cached_dates_set
                missing_dates = sorted(list(missing_dates_set))
            else:
                missing_dates = sorted(list(requested_dates_set))
                print(f"DEBUG: check_cache: No data found in cache for {ticker} ({interval}) in range. All requested dates are missing.")
        except Exception as e:
            print(f"錯誤 (check_cache): 查詢快取數據失敗 for {ticker} ({interval}): {e}")
            missing_dates = sorted(list(requested_dates_set))
            cached_df = pd.DataFrame()
        print(f"INFO: check_cache: For {ticker} ({interval}), found {len(cached_df)} cached rows. Missing {len(missing_dates)} dates: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")
        return cached_df, missing_dates

if __name__ == '__main__':
    print("--- DBManager (Daily Market Analyzer) 測試 ---")
    test_db_path = "data_workspace/temp/test_analyzer_market_data.duckdb"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    db_manager = DBManager(test_db_path)
    table_name = "market_ohlcv_analyzer_test"

    print(f"\n--- 測試 1: 建立 {table_name} 資料表 ---")
    db_manager.create_ohlcv_table(table_name=table_name)

    data1 = {
        'datetime': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 10:05:00']).tz_localize('UTC'),
        'ticker': ['TEST_MAIN', 'TEST_MAIN'], 'interval': ['1m', '1m'],
        'open': [100, 101], 'high': [102, 101.5], 'low': [99, 100.5], 'close': [101, 101.2], 'volume': [1000, 1200]
    }
    df_test_main = pd.DataFrame(data1).set_index('datetime')
    db_manager.upsert_data(df_test_main, table_name)
    with duckdb.connect(test_db_path) as con:
        assert con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] == 2

    print("\n--- 測試輔助查詢方法 ---")
    multi_day_data = []
    base_date_dt = datetime.strptime("2024-07-20", "%Y-%m-%d")
    for i in range(5):
        day_dt = base_date_dt + timedelta(days=i)
        multi_day_data.append({
            'datetime': day_dt.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=timezone.utc),
            'ticker': 'AAPL', 'interval': '1d',
            'open': 150.0+i, 'high': 152.5+i, 'low': 149.5+i, 'close': 151.0+i, 'volume': 1000000+i*1000
        })
        multi_day_data.append({
            'datetime': day_dt.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=timezone.utc),
            'ticker': 'MSFT', 'interval': '1d',
            'open': 200.0+i, 'high': 202.5+i, 'low': 199.5+i, 'close': 201.0+i, 'volume': 800000+i*1000
        })
    day_for_minute_data_dt = base_date_dt + timedelta(days=2)
    for min_offset in range(0, 60, 15):
        ts = day_for_minute_data_dt.replace(hour=9, minute=min_offset, second=0, microsecond=0, tzinfo=timezone.utc)
        multi_day_data.append({
            'datetime': ts, 'ticker': 'AAPL', 'interval': '15m',
            'open': 152.0 + min_offset*0.01, 'high': 152.5 + min_offset*0.01,
            'low': 151.5 + min_offset*0.01, 'close': 152.2 + min_offset*0.01, 'volume': 5000+min_offset*10
        })
    df_multi_day_all = pd.DataFrame(multi_day_data)
    df_multi_day_all['datetime'] = pd.to_datetime(df_multi_day_all['datetime'])
    if df_multi_day_all['datetime'].dt.tz is None:
        df_multi_day_all['datetime'] = df_multi_day_all['datetime'].dt.tz_localize('UTC')
    else:
        df_multi_day_all['datetime'] = df_multi_day_all['datetime'].dt.tz_convert('UTC')
    df_multi_day_all = df_multi_day_all.set_index('datetime')
    db_manager.upsert_data(df_multi_day_all, table_name)

    print("\n--- 測試 query_data_for_day ---")
    aapl_2024_07_22_data = db_manager.query_data_for_day(ticker="AAPL", date_str="2024-07-22", table_name=table_name)
    print(f"AAPL 2024-07-22 data (預期 1筆 '1d' + 4筆 '15m'):\n{aapl_2024_07_22_data}")
    assert len(aapl_2024_07_22_data) == 5
    assert '1d' in aapl_2024_07_22_data['interval'].unique()
    assert '15m' in aapl_2024_07_22_data['interval'].unique()
    msft_2024_07_22_data = db_manager.query_data_for_day(ticker="MSFT", date_str="2024-07-22", table_name=table_name)
    print(f"\nMSFT 2024-07-22 data (預期 1筆 '1d'):\n{msft_2024_07_22_data}")
    assert len(msft_2024_07_22_data) == 1
    assert msft_2024_07_22_data['interval'].iloc[0] == '1d'

    print("\n--- 測試 query_previous_day_close ---")
    prev_close_aapl = db_manager.query_previous_day_close(ticker="AAPL", current_date_str="2024-07-23", table_name=table_name)
    print(f"AAPL 前一日 (相對於 2024-07-23) 收盤價: {prev_close_aapl}")
    assert prev_close_aapl == 151.0 + 2
    prev_close_msft_weekend = db_manager.query_previous_day_close(ticker="MSFT", current_date_str="2024-07-21", table_name=table_name)
    print(f"MSFT 前一日 (相對於 2024-07-21) 收盤價: {prev_close_msft_weekend}")
    assert prev_close_msft_weekend == 201.0
    prev_close_way_back = db_manager.query_previous_day_close(ticker="AAPL", current_date_str="2024-07-19", table_name=table_name)
    print(f"AAPL 前一日 (相對於 2024-07-19，預期為 None): {prev_close_way_back}")
    assert prev_close_way_back is None
    print("\n--- DBManager 輔助查詢測試完畢 ---")

    print("\n--- 測試 check_cache ---")
    print("\n案例 1: AAPL, '1d', 2024-07-20 to 2024-07-22 (完全命中)")
    cached_df1, missing_dates1 = db_manager.check_cache(
        ticker="AAPL", start_date_str="2024-07-20", end_date_str="2024-07-22",
        interval="1d", table_name=table_name
    )
    assert not cached_df1.empty, "案例 1 cached_df1 不應為空"
    assert len(cached_df1) == 3, f"案例 1 cached_df1 應有 3 筆數據, 實際: {len(cached_df1)}"
    assert cached_df1['interval'].unique() == ['1d'], "案例 1 interval 應為 '1d'"
    assert not missing_dates1, f"案例 1 missing_dates1 應為空, 實際: {missing_dates1}"
    print(f"案例 1: cached_df1 ({len(cached_df1)} rows), missing_dates1: {missing_dates1}")

    print("\n案例 2: AAPL, '15m', 2024-07-22 to 2024-07-22 (完全命中)")
    cached_df2, missing_dates2 = db_manager.check_cache(
        ticker="AAPL", start_date_str="2024-07-22", end_date_str="2024-07-22",
        interval="15m", table_name=table_name
    )
    assert not cached_df2.empty, "案例 2 cached_df2 不應為空"
    assert len(cached_df2) == 4, f"案例 2 cached_df2 應有 4 筆數據, 實際: {len(cached_df2)}"
    assert cached_df2['interval'].unique() == ['15m'], "案例 2 interval 應為 '15m'"
    assert not missing_dates2, f"案例 2 missing_dates2 應為空, 實際: {missing_dates2}"
    print(f"案例 2: cached_df2 ({len(cached_df2)} rows), missing_dates2: {missing_dates2}")

    print("\n案例 3: AAPL, '1d', 2024-07-23 to 2024-07-25 (部分命中)")
    cached_df3, missing_dates3 = db_manager.check_cache(
        ticker="AAPL", start_date_str="2024-07-23", end_date_str="2024-07-25",
        interval="1d", table_name=table_name
    )
    assert not cached_df3.empty, "案例 3 cached_df3 不應為空"
    assert len(cached_df3) == 2, f"案例 3 cached_df3 應有 2 筆數據, 實際: {len(cached_df3)}"
    assert missing_dates3 == ['2024-07-25'], f"案例 3 missing_dates3 應為 ['2024-07-25'], 實際: {missing_dates3}"
    print(f"案例 3: cached_df3 ({len(cached_df3)} rows), missing_dates3: {missing_dates3}")

    print("\n案例 4: AAPL, '1h', 2024-07-20 to 2024-07-21 (完全未命中 - 不同 interval)")
    cached_df4, missing_dates4 = db_manager.check_cache(
        ticker="AAPL", start_date_str="2024-07-20", end_date_str="2024-07-21",
        interval="1h", table_name=table_name
    )
    assert cached_df4.empty, f"案例 4 cached_df4 應為空, 實際: {len(cached_df4)} rows"
    assert missing_dates4 == ['2024-07-20', '2024-07-21'], f"案例 4 missing_dates4 應為 ['2024-07-20', '2024-07-21'], 實際: {missing_dates4}"
    print(f"案例 4: cached_df4 ({len(cached_df4)} rows), missing_dates4: {missing_dates4}")

    print("\n案例 5: GOOG, '1d', 2024-07-20 to 2024-07-21 (完全未命中 - 不同 ticker)")
    cached_df5, missing_dates5 = db_manager.check_cache(
        ticker="GOOG", start_date_str="2024-07-20", end_date_str="2024-07-21",
        interval="1d", table_name=table_name
    )
    assert cached_df5.empty, f"案例 5 cached_df5 應為空, 實際: {len(cached_df5)} rows"
    assert missing_dates5 == ['2024-07-20', '2024-07-21'], f"案例 5 missing_dates5 應為 ['2024-07-20', '2024-07-21'], 實際: {missing_dates5}"
    print(f"案例 5: cached_df5 ({len(cached_df5)} rows), missing_dates5: {missing_dates5}")

    print("\n案例 6: AAPL, '1d', 2024-07-26 to 2024-07-27 (完全未命中 - 日期範圍超出現有數據)")
    cached_df6, missing_dates6 = db_manager.check_cache(
        ticker="AAPL", start_date_str="2024-07-26", end_date_str="2024-07-27",
        interval="1d", table_name=table_name
    )
    assert cached_df6.empty, f"案例 6 cached_df6 應為空, 實際: {len(cached_df6)} rows"
    assert missing_dates6 == ['2024-07-26', '2024-07-27'], f"案例 6 missing_dates6 應為 ['2024-07-26', '2024-07-27'], 實際: {missing_dates6}"
    print(f"案例 6: cached_df6 ({len(cached_df6)} rows), missing_dates6: {missing_dates6}")

    print("\n案例 7: AAPL, '1d', 2024-07-19 to 2024-07-21 (部分命中 - 開始日期在數據之前)")
    cached_df7, missing_dates7 = db_manager.check_cache(
        ticker="AAPL", start_date_str="2024-07-19", end_date_str="2024-07-21",
        interval="1d", table_name=table_name
    )
    assert not cached_df7.empty, "案例 7 cached_df7 不應為空"
    assert len(cached_df7) == 2, f"案例 7 cached_df7 應有 2 筆數據 (07-20, 07-21), 實際: {len(cached_df7)}"
    assert missing_dates7 == ['2024-07-19'], f"案例 7 missing_dates7 應為 ['2024-07-19'], 實際: {missing_dates7}"
    print(f"案例 7: cached_df7 ({len(cached_df7)} rows), missing_dates7: {missing_dates7}")

    print("\n--- check_cache 測試完畢 ---")

    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        print(f"INFO: 已刪除測試資料庫 {test_db_path}")
