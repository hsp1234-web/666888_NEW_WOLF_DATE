# -*- coding: utf-8 -*-
"""
DuckDB 資料庫管理模組 for Daily Market Analyzer。
負責處理所有與 DuckDB 的互動，例如建立資料表、寫入數據等。
"""
import duckdb
import pandas as pd
import os
from datetime import datetime, timedelta

class DBManager:
    """
    DuckDB 資料庫管理器。

    提供方法來建立資料庫連線、建立資料表以及高效地寫入 (UPSERT) DataFrame 數據。
    此版本適用於 Daily Market Analyzer，處理包含 'interval' 欄位的數據，並提供查詢功能。
    """
    def __init__(self, db_path: str, cache_db_path: str | None = None):
        """
        初始化 DBManager。

        Args:
            db_path (str): 主分析資料庫檔案的路徑。
            cache_db_path (str | None, optional): 快取資料庫檔案的路徑。若為 None，則不啟用快取。
        """
        self.db_path = db_path
        self.cache_db_path = cache_db_path
        self.main_conn = None # 將在需要時建立
        self.cache_conn = None # 將在需要時建立

        # 確保主資料庫目錄存在
        main_db_dir = os.path.dirname(self.db_path)
        if main_db_dir and not os.path.exists(main_db_dir):
            os.makedirs(main_db_dir, exist_ok=True)
            print(f"INFO: 已建立主資料庫目錄: {main_db_dir}")

        print(f"INFO: DBManager 初始化 - 主資料庫路徑: {self.db_path}")

        if self.cache_db_path:
            cache_db_dir = os.path.dirname(self.cache_db_path)
            if cache_db_dir and not os.path.exists(cache_db_dir):
                os.makedirs(cache_db_dir, exist_ok=True)
                print(f"INFO: 已建立快取資料庫目錄: {cache_db_dir}")
            print(f"INFO: DBManager 初始化 - 快取資料庫路徑: {self.cache_db_path}")
            # 為快取資料庫也建立OHLCV表
        conn_cache = self._get_cache_connection()
        if conn_cache:
            self._create_ohlcv_table_on_connection(conn_cache, "market_ohlcv_cache")

        # 為主資料庫建立OHLCV表
        conn_main = self._get_main_connection()
        if conn_main:
            self._create_ohlcv_table_on_connection(conn_main, "market_ohlcv_data")


    def _get_main_connection(self):
        """獲取主資料庫連線。"""
        if not self.db_path: # 如果 db_path 未設定，則無法建立連線
            return None
        if self.main_conn is None or getattr(self.main_conn, 'closed', True):
            self.main_conn = duckdb.connect(self.db_path)
        return self.main_conn

    def _get_cache_connection(self):
        """獲取快取資料庫連線。如果未配置快取路徑，則返回 None。"""
        if not self.cache_db_path:
            return None
        if self.cache_conn is None or getattr(self.cache_conn, 'closed', True):
            self.cache_conn = duckdb.connect(self.cache_db_path)
        return self.cache_conn

    def close_connections(self):
        """關閉所有資料庫連線。"""
        if self.main_conn and not getattr(self.main_conn, 'closed', True):
            self.main_conn.close()
            print(f"INFO: 已關閉主資料庫連線 ({self.db_path})。")
            self.main_conn = None # 重設為 None 以便下次可以重新連接
        if self.cache_conn and not getattr(self.cache_conn, 'closed', True):
            self.cache_conn.close()
            print(f"INFO: 已關閉快取資料庫連線 ({self.cache_db_path})。")
            self.cache_conn = None # 重設為 None

    def _create_ohlcv_table_on_connection(self, connection: duckdb.DuckDBPyConnection, table_name: str):
        """
        在指定的資料庫連線上建立市場 OHLCV 數據表。
        """
        if connection is None:
            # 此處應記錄一個更明確的錯誤或警告，因為預期 connection 是有效的
            print(f"警告: _create_ohlcv_table_on_connection 收到空的連線物件 (目標表: {table_name})。")
            return

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
            connection.execute(create_sql)
            # 嘗試獲取資料庫檔案名以供日誌記錄 (考慮到不同 DuckDB 版本)
            db_identifier = "未知資料庫"
            if connection == self.main_conn and self.db_path:
                db_identifier = self.db_path
            elif connection == self.cache_conn and self.cache_db_path:
                db_identifier = self.cache_db_path
            print(f"INFO: 資料表 '{table_name}' 已在資料庫 '{db_identifier}' 中準備就緒。")
        except Exception as e:
            db_identifier = "未知資料庫"
            if connection == self.main_conn and self.db_path:
                db_identifier = self.db_path
            elif connection == self.cache_conn and self.cache_db_path:
                db_identifier = self.cache_db_path
            print(f"錯誤: 建立資料表 '{table_name}' 於 '{db_identifier}' 失敗: {e}")
            raise # 重新拋出異常，因為表格創建失敗是嚴重問題

    def _upsert_dataframe(self, conn: duckdb.DuckDBPyConnection | None, df: pd.DataFrame, table_name: str, db_identifier: str):
        """
        內部輔助方法：將 DataFrame 數據寫入指定連線的資料表。
        """
        if conn is None:
            print(f"錯誤: 資料庫連線 ({db_identifier}) 未設定。無法對 '{table_name}' 執行 upsert。")
            return

        if df.empty:
            current_ticker = "未知Ticker"
            if 'ticker' in df.columns and not df.empty:
                current_ticker = df['ticker'].iloc[0]
            elif hasattr(df, 'name') and df.name:
                 current_ticker = df.name
            print(f"INFO: 傳入的 DataFrame ({current_ticker}) 為空，無需寫入資料表 '{table_name}' 至 '{db_identifier}'。")
            return

        df_to_insert = df.copy()
        if isinstance(df_to_insert.index, pd.DatetimeIndex):
            df_to_insert = df_to_insert.reset_index()
        df_to_insert.columns = [col.lower() for col in df_to_insert.columns]

        if 'index' in df_to_insert.columns and 'datetime' not in df_to_insert.columns:
            df_to_insert.rename(columns={'index': 'datetime'}, inplace=True)
        if 'datetime' not in df_to_insert.columns and 'date' in df_to_insert.columns:
            df_to_insert.rename(columns={'date': 'datetime'}, inplace=True)
        if 'ticker' not in df_to_insert.columns and hasattr(df, 'name') and df.name:
             df_to_insert['ticker'] = df.name

        required_cols = ['datetime', 'ticker', 'interval', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df_to_insert.columns]
        if missing_cols:
            ticker_info = df_to_insert['ticker'].iloc[0] if 'ticker' in df_to_insert.columns and not df_to_insert.empty else "未知Ticker"
            print(f"錯誤: DataFrame ({ticker_info}) 缺少必要欄位: {', '.join(missing_cols)}。無法寫入 '{db_identifier}' 的資料表 '{table_name}'。")
            df_to_insert.info() # 提供 DataFrame 結構以供調試
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
            ticker_info = df_to_insert['ticker'].iloc[0] if 'ticker' in df_to_insert.columns and not df_to_insert.empty else "未知Ticker"
            print(f"錯誤: DataFrame ({ticker_info}) 數據類型轉換失敗: {e} (資料庫: {db_identifier})")
            df_to_insert.info() # 提供 DataFrame 結構以供調試
            return

        try:
            # 使用傳入的連線物件 conn
            conn.register('df_view_to_insert', df_to_insert)
            columns_str = ", ".join(required_cols)
            upsert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns_str}) SELECT {columns_str} FROM df_view_to_insert"
            conn.execute(upsert_sql)
            conn.unregister('df_view_to_insert') # 及時釋放視圖
            ticker_info = df_to_insert['ticker'].iloc[0]
            interval_info = df_to_insert['interval'].iloc[0]
            print(f"INFO: 成功將 {len(df_to_insert)} 筆 '{ticker_info}' ({interval_info}) 數據寫入/更新至 '{db_identifier}' 的資料表 '{table_name}'。")
        except Exception as e:
            ticker_info = df_to_insert['ticker'].iloc[0] if 'ticker' in df_to_insert.columns and not df_to_insert.empty else "未知Ticker"
            print(f"錯誤: 寫入數據到 '{db_identifier}' 的資料表 '{table_name}' 失敗 (Ticker: {ticker_info}): {e}")
            df_to_insert.info() # 提供 DataFrame 結構以供調試

    def upsert_data(self, df: pd.DataFrame, table_name: str = "market_ohlcv_data"):
        """
        將 DataFrame 數據寫入主分析資料庫的指定資料表。
        """
        self._upsert_dataframe(self._get_main_connection(), df, table_name, self.db_path or "主資料庫")

    def update_cache(self, df: pd.DataFrame, table_name: str = "market_ohlcv_cache") -> None:
        """
        將 DataFrame 數據寫入快取資料庫的指定資料表。
        """
        if not self.cache_db_path: # 如果未配置快取路徑，則不執行任何操作
            print("INFO: 未配置快取資料庫路徑，不執行 update_cache。")
            return
        self._upsert_dataframe(self._get_cache_connection(), df, table_name, self.cache_db_path)

    def check_cache(self, ticker: str, start_date_str: str, end_date_str: str, table_name: str = "market_ohlcv_cache") -> pd.DataFrame | None:
        """
        檢查快取資料庫中指定 ticker 和日期範圍的數據是否完整覆蓋。
        如果完整，返回包含所有快取數據的 DataFrame。
        如果數據不完整或不存在，則返回 None。
        「完整覆蓋」意味著請求的日期範圍內每一天（日曆天）在快取中都有對應的數據記錄。
        """
        conn = self._get_cache_connection()
        if conn is None: # 無快取配置或無法連線
            print("INFO: 快取資料庫未配置或無法連線，check_cache 返回 None。")
            return None

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

            if start_date > end_date:
                print(f"INFO: 快取檢查 ({ticker}, {start_date_str}-{end_date_str}): 開始日期晚於結束日期，視為無效範圍，返回 None。")
                return None

            # 生成請求範圍內的所有日曆天
            expected_dates_in_range = set()
            current_eval_date = start_date
            while current_eval_date <= end_date:
                expected_dates_in_range.add(current_eval_date)
                current_eval_date += timedelta(days=1)

            if not expected_dates_in_range: # 理論上如果 start_date <= end_date，這裡不會為空
                return None

            # 查詢快取中實際存在的、在此日期範圍內的、此 ticker 的數據的日期（去重）
            # DuckDB's date_trunc('day', timestamptz_column) returns a timestamptz. We need to cast to DATE.
            query_existing_dates = f"""
            SELECT DISTINCT CAST(date_trunc('day', datetime) AS DATE) AS unique_date
            FROM {table_name}
            WHERE ticker = ? AND datetime >= ? AND datetime < ?
            """
            # yfinance 的 end date 是 exclusive，所以查詢時 end datetime 應該是 end_date 的後一天 00:00:00
            query_end_datetime_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

            cached_dates_df = conn.execute(query_existing_dates, [ticker, start_date_str, query_end_datetime_str]).fetchdf()

            if cached_dates_df.empty:
                print(f"INFO: 快取檢查 ({ticker}, {start_date_str}-{end_date_str}): 在指定範圍內，快取中無此代號的數據日期記錄。")
                return None

            # 將查詢結果中的日期轉換為 date 物件集合
            actual_cached_dates = set(pd.to_datetime(cached_dates_df['unique_date']).dt.date)

            # 檢查請求範圍內的每一天是否都在快取的日期集合中
            if not expected_dates_in_range.issubset(actual_cached_dates):
                missing_dates = expected_dates_in_range - actual_cached_dates
                print(f"INFO: 快取檢查 ({ticker}, {start_date_str}-{end_date_str}): 數據不完整，缺失日期: {sorted(list(missing_dates))[:5]} (最多顯示5個)。")
                return None

            # 如果所有請求的日期都在快取中，則提取這些數據
            print(f"INFO: 快取檢查 ({ticker}, {start_date_str}-{end_date_str}): 所有請求日期均在快取中有記錄。準備提取數據。")
            query_data = f"""
            SELECT * FROM {table_name}
            WHERE ticker = ? AND datetime >= ? AND datetime < ?
            ORDER BY datetime ASC
            """
            all_cached_data_df = conn.execute(query_data, [ticker, start_date_str, query_end_datetime_str]).fetchdf()

            if all_cached_data_df.empty:
                print(f"警告: 快取檢查 ({ticker}, {start_date_str}-{end_date_str}): 日期覆蓋檢查通過，但實際提取數據為空。")
                return None

            # 標準化返回的 DataFrame 中的 'datetime' 欄位
            if 'datetime' in all_cached_data_df.columns:
                all_cached_data_df['datetime'] = pd.to_datetime(all_cached_data_df['datetime'])
                if all_cached_data_df['datetime'].dt.tz is None: # 確保時區為 UTC
                    all_cached_data_df['datetime'] = all_cached_data_df['datetime'].dt.tz_localize('UTC')
                else:
                    all_cached_data_df['datetime'] = all_cached_data_df['datetime'].dt.tz_convert('UTC')

            print(f"INFO: 快取命中! 從快取資料庫成功為 {ticker} ({start_date_str}-{end_date_str}) 載入 {len(all_cached_data_df)} 筆數據。")
            return all_cached_data_df

        except Exception as e:
            print(f"錯誤: 執行快取檢查 ({ticker}, {start_date_str}-{end_date_str}) 時發生錯誤: {e}")
            return None


    def query_data_for_day(self, ticker: str, date_str: str, table_name: str = "market_ohlcv_data") -> pd.DataFrame:
        """
        查詢指定 ticker 在特定日期的所有 OHLCV 數據。此方法操作主分析資料庫。
        """
        conn = self._get_main_connection()
        if conn is None:
            print(f"錯誤: 無法獲取主資料庫連線。無法對 '{table_name}' 執行 query_data_for_day。")
            return pd.DataFrame()
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            start_of_day = f"{date_str} 00:00:00"
            start_of_next_day = (target_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

            query = f"""
            SELECT * FROM {table_name}
            WHERE ticker = ? AND datetime >= CAST(? AS TIMESTAMPTZ) AND datetime < CAST(? AS TIMESTAMPTZ)
            ORDER BY datetime ASC
            """
            result_df = conn.execute(query, [ticker, start_of_day, start_of_next_day]).fetchdf()

            if not result_df.empty and 'datetime' in result_df.columns:
                result_df['datetime'] = pd.to_datetime(result_df['datetime'])
                if result_df['datetime'].dt.tz is None: # fetchdf 可能返回 naive datetime
                    result_df['datetime'] = result_df['datetime'].dt.tz_localize('UTC')
                else:
                    result_df['datetime'] = result_df['datetime'].dt.tz_convert('UTC')
                result_df = result_df.set_index('datetime')
            return result_df
        except Exception as e:
            print(f"錯誤: 查詢 {ticker} 在 {date_str} 的數據失敗 (主資料庫): {e}")
            return pd.DataFrame()

    def query_previous_day_close(self, ticker: str, current_date_str: str, table_name: str = "market_ohlcv_data", max_lookback_days: int = 30) -> float | None: # 注意預設 table_name
        """
        查詢指定 ticker 在 current_date_str 前一個「有數據的」交易日的最後一筆收盤價。此方法操作主分析資料庫。
        """
        conn = self._get_main_connection()
        if conn is None:
            print(f"錯誤: 無法獲取主資料庫連線。無法執行 query_previous_day_close。")
            return None
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
            print(f"錯誤: 查詢 {ticker} 在 {current_date_str} 之前的收盤價失敗 (主資料庫): {e}")
            return None

if __name__ == '__main__':
    print("--- DBManager (Daily Market Analyzer) 測試 ---")
    test_db_main_path = "data_workspace/temp/test_analyzer_main.duckdb"
    test_db_cache_path = "data_workspace/temp/test_analyzer_cache.duckdb"
    main_table_name = "market_ohlcv_data"
    cache_table_name = "market_ohlcv_cache"

    # 清理舊的測試檔案
    if os.path.exists(test_db_main_path):
        os.remove(test_db_main_path)
        print(f"INFO: 已刪除舊的主測試資料庫: {test_db_main_path}")
    if os.path.exists(test_db_cache_path):
        os.remove(test_db_cache_path)
        print(f"INFO: 已刪除舊的快取測試資料庫: {test_db_cache_path}")

    print("\n--- 測試 1: 初始化 DBManager (主庫 + 快取庫) ---")
    db_manager_full = DBManager(db_path=test_db_main_path, cache_db_path=test_db_cache_path)
    assert os.path.exists(test_db_main_path), "主資料庫檔案未建立"
    assert os.path.exists(test_db_cache_path), "快取資料庫檔案未建立"

    conn_main_check = db_manager_full._get_main_connection()
    assert conn_main_check is not None, "無法獲取主資料庫連線進行表格檢查"
    main_table_exists = conn_main_check.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{main_table_name}'").fetchone()
    assert main_table_exists is not None and main_table_exists[0] == 1, f"主資料庫中未找到表格 {main_table_name}"

    conn_cache_check = db_manager_full._get_cache_connection()
    assert conn_cache_check is not None, "無法獲取快取資料庫連線進行表格檢查"
    cache_table_exists = conn_cache_check.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{cache_table_name}'").fetchone()
    assert cache_table_exists is not None and cache_table_exists[0] == 1, f"快取資料庫中未找到表格 {cache_table_name}"
    db_manager_full.close_connections()
    print("INFO: 測試 1 完成。")

    print("\n--- 測試 2: 初始化 DBManager (僅主庫) ---")
    if os.path.exists(test_db_main_path): os.remove(test_db_main_path)
    if os.path.exists(test_db_cache_path): os.remove(test_db_cache_path)

    db_manager_main_only = DBManager(db_path=test_db_main_path)
    assert os.path.exists(test_db_main_path), "僅主庫初始化時，主資料庫檔案未建立"
    assert not os.path.exists(test_db_cache_path), "僅主庫初始化時，快取資料庫不應被建立"

    conn_main_only_check = db_manager_main_only._get_main_connection()
    assert conn_main_only_check is not None
    main_only_table_exists = conn_main_only_check.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{main_table_name}'").fetchone()
    assert main_only_table_exists is not None and main_only_table_exists[0] == 1
    assert db_manager_main_only._get_cache_connection() is None, "僅主庫初始化時，快取連線應為 None"
    db_manager_main_only.close_connections()
    print("INFO: 測試 2 完成。")

    print("\n--- 測試 3: upsert_data 和 update_cache ---")
    if os.path.exists(test_db_main_path): os.remove(test_db_main_path)
    if os.path.exists(test_db_cache_path): os.remove(test_db_cache_path)
    db_manager = DBManager(db_path=test_db_main_path, cache_db_path=test_db_cache_path)

    conn_m_setup = db_manager._get_main_connection()
    if conn_m_setup: conn_m_setup.execute(f"DELETE FROM {main_table_name}")
    conn_c_setup = db_manager._get_cache_connection()
    if conn_c_setup: conn_c_setup.execute(f"DELETE FROM {cache_table_name}")

    data_sample = {
        'datetime': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 10:05:00']).tz_localize('UTC'),
        'ticker': ['UPS_TICK', 'UPS_TICK'], 'interval': ['5m', '5m'],
        'open': [10, 11], 'high': [12, 11.5], 'low': [9, 10.5], 'close': [11, 11.2], 'volume': [100, 120]
    }
    df_sample = pd.DataFrame(data_sample)

    db_manager.upsert_data(df_sample)
    conn_m_check_ups = db_manager._get_main_connection()
    assert conn_m_check_ups is not None
    count_main = conn_m_check_ups.execute(f"SELECT COUNT(*) FROM {main_table_name} WHERE ticker = 'UPS_TICK'").fetchone()
    assert count_main is not None and count_main[0] == 2

    db_manager.update_cache(df_sample)
    conn_c_check_ups = db_manager._get_cache_connection()
    assert conn_c_check_ups is not None
    count_cache = conn_c_check_ups.execute(f"SELECT COUNT(*) FROM {cache_table_name} WHERE ticker = 'UPS_TICK'").fetchone()
    assert count_cache is not None and count_cache[0] == 2
    print("INFO: 測試 3 完成。")

    print("\n--- 測試 4: check_cache ---")
    cache_data_list = []
    start_c_date = datetime(2023, 2, 1).date()
    for i in range(5):
        day = start_c_date + timedelta(days=i)
        dt_aware = datetime(day.year, day.month, day.day, 10, 0, 0).astimezone(timedelta(0))
        cache_data_list.append({
            'datetime': dt_aware,
            'ticker': 'FULLCVR', 'interval': '1d', 'open': 10+i, 'high': 12+i, 'low': 9+i, 'close': 11+i, 'volume': 1000+i*10
        })
    df_full_cover = pd.DataFrame(cache_data_list)
    db_manager.update_cache(df_full_cover)

    print("  測試 4.1: 完整覆蓋")
    cached_df_full = db_manager.check_cache(ticker='FULLCVR', start_date_str='2023-02-01', end_date_str='2023-02-05')
    assert cached_df_full is not None, "完整覆蓋測試失敗，返回了 None"
    assert len(cached_df_full) == 5, f"完整覆蓋測試失敗，預期5筆，得到{len(cached_df_full)}"
    assert cached_df_full['ticker'].unique()[0] == 'FULLCVR'

    print("  測試 4.2: 部分覆蓋 (結束日期超出)")
    cached_df_partial_end = db_manager.check_cache(ticker='FULLCVR', start_date_str='2023-02-01', end_date_str='2023-02-06')
    assert cached_df_partial_end is None, "部分覆蓋 (結束日期超出) 測試失敗，未返回 None"

    print("  測試 4.3: 部分覆蓋 (開始日期提前)")
    cached_df_partial_start = db_manager.check_cache(ticker='FULLCVR', start_date_str='2023-01-31', end_date_str='2023-02-05')
    assert cached_df_partial_start is None, "部分覆蓋 (開始日期提前) 測試失敗，未返回 None"

    print("  測試 4.4: 範圍內但中間缺失一天")
    conn_c_del_test = db_manager._get_cache_connection()
    if conn_c_del_test:
        conn_c_del_test.execute(f"DELETE FROM {cache_table_name} WHERE ticker = 'FULLCVR' AND CAST(date_trunc('day', datetime) AS DATE) = CAST('2023-02-03' AS DATE)")
    cached_df_missing_middle = db_manager.check_cache(ticker='FULLCVR', start_date_str='2023-02-01', end_date_str='2023-02-05')
    assert cached_df_missing_middle is None, "範圍內但中間缺失一天測試失敗，未返回 None"

    print("  測試 4.5: 代號不存在")
    cached_df_no_ticker = db_manager.check_cache(ticker='NONEXISTENT', start_date_str='2023-02-01', end_date_str='2023-02-05')
    assert cached_df_no_ticker is None, "代號不存在測試失敗，未返回 None"

    print("  測試 4.6: 空日期範圍 (start > end)")
    cached_df_empty_range = db_manager.check_cache(ticker='FULLCVR', start_date_str='2023-02-05', end_date_str='2023-02-01')
    assert cached_df_empty_range is None, "空日期範圍測試失敗，未返回 None"
    print("INFO: 測試 4 完成。")

    print("\n--- 測試 5: query_data_for_day 和 query_previous_day_close (應使用主庫) ---")
    db_manager.upsert_data(df_full_cover[df_full_cover['ticker'] == 'FULLCVR'])

    main_day_data = db_manager.query_data_for_day(ticker='FULLCVR', date_str='2023-02-01')
    assert main_day_data is not None and not main_day_data.empty, "query_data_for_day 未從主庫獲取數據"
    assert main_day_data['close'].iloc[0] == 11.0

    prev_close = db_manager.query_previous_day_close(ticker='FULLCVR', current_date_str='2023-02-03')
    assert prev_close == 11.0 + 1, f"query_previous_day_close 錯誤，預期 {11.0+1}，得到 {prev_close}"
    print("INFO: 測試 5 完成。")

    db_manager.close_connections()
    print("\n--- DBManager (Daily Market Analyzer) 包含快取功能測試完畢 ---")

    if os.path.exists(test_db_main_path):
        os.remove(test_db_main_path)
    if os.path.exists(test_db_cache_path):
        os.remove(test_db_cache_path)
    print(f"INFO: 已刪除所有測試資料庫檔案。")
    base_date_dt = datetime.strptime("2024-07-20", "%Y-%m-%d")

    for i in range(5): # 5 天的數據
        day_dt = base_date_dt + timedelta(days=i)
        # 日線數據 (每天一筆)
        multi_day_data.append({
            'datetime': day_dt.replace(hour=16, minute=0, second=0, microsecond=0).astimezone(timedelta(hours=0)), # 標準化到 UTC 16:00
            'ticker': 'AAPL', 'interval': '1d',
            'open': 150.0+i, 'high': 152.5+i, 'low': 149.5+i, 'close': 151.0+i, 'volume': 1000000+i*1000
        })
        multi_day_data.append({
            'datetime': day_dt.replace(hour=16, minute=0, second=0, microsecond=0).astimezone(timedelta(hours=0)),
            'ticker': 'MSFT', 'interval': '1d',
            'open': 200.0+i, 'high': 202.5+i, 'low': 199.5+i, 'close': 201.0+i, 'volume': 800000+i*1000
        })

    # 特定一天的分鐘線數據 (AAPL, 2024-07-22)
    day_for_minute_data_dt = base_date_dt + timedelta(days=2) # This is 2024-07-22
    for min_offset in range(0, 60, 15): # 09:00, 09:15, 09:30, 09:45 (假設市場開盤時間)
        ts = day_for_minute_data_dt.replace(hour=9, minute=min_offset, second=0, microsecond=0).astimezone(timedelta(hours=0))
        multi_day_data.append({
            'datetime': ts, 'ticker': 'AAPL', 'interval': '15m',
            'open': 152.0 + min_offset*0.01, 'high': 152.5 + min_offset*0.01,
            'low': 151.5 + min_offset*0.01, 'close': 152.2 + min_offset*0.01, 'volume': 5000+min_offset*10
        })

    df_multi_day_all = pd.DataFrame(multi_day_data)
    df_multi_day_all['datetime'] = pd.to_datetime(df_multi_day_all['datetime'])
    # 確保所有 datetime 都是 UTC aware before set_index
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
    # AAPL: 2024-07-22 (i=2) close = 151.0+2 = 153.0
    # MSFT: 2024-07-22 (i=2) close = 201.0+2 = 203.0

    # 查詢 AAPL 2024-07-23 的前一日收盤價 (應為 2024-07-22 的日線收盤價)
    prev_close_aapl = db_manager.query_previous_day_close(ticker="AAPL", current_date_str="2024-07-23", table_name=table_name)
    print(f"AAPL 前一日 (相對於 2024-07-23) 收盤價: {prev_close_aapl}")
    assert prev_close_aapl == 151.0 + 2 # 153.0 (AAPL '1d' close on 2024-07-22)

    # 查詢 MSFT 2024-07-21 (週日) 的前一日收盤價 (應為 2024-07-20 的日線收盤價)
    # 2024-07-20 (i=0) MSFT '1d' close = 201.0
    prev_close_msft_weekend = db_manager.query_previous_day_close(ticker="MSFT", current_date_str="2024-07-21", table_name=table_name)
    print(f"MSFT 前一日 (相對於 2024-07-21) 收盤價: {prev_close_msft_weekend}")
    assert prev_close_msft_weekend == 201.0

    # 查詢一個日期，其前幾天都沒有數據
    prev_close_way_back = db_manager.query_previous_day_close(ticker="AAPL", current_date_str="2024-07-19", table_name=table_name) # 數據從 07-20 開始
    print(f"AAPL 前一日 (相對於 2024-07-19，預期為 None): {prev_close_way_back}")
    assert prev_close_way_back is None

    print("\n--- DBManager 輔助查詢測試完畢 ---")
    # os.remove(test_db_path)
    # print(f"INFO: 已刪除測試資料庫 {test_db_path}")
