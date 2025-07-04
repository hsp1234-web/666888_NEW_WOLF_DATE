import yfinance as yf
import pandas as pd
import duckdb
import os
import argparse
from datetime import datetime, timedelta
import time # 用於日誌時間戳和可能的延遲

# --- 全局變數與設定 ---
DB_PATH = "./workspace/yfinance_cache.duckdb" # 快取資料庫路徑
LOG_LEVEL = "INFO" # 可設定為 DEBUG, INFO, WARNING, ERROR

# --- 日誌記錄函數 ---
def get_log_timestamp(): # 避免與 yfinance 下載的 datetime 衝突
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # 含毫秒

def log_message(level, message, component="ANALYZER"):
    if LOG_LEVEL == "DEBUG" or \
       (LOG_LEVEL == "INFO" and level in ["INFO", "SUCCESS", "WARNING", "ERROR"]) or \
       (LOG_LEVEL == "WARNING" and level in ["WARNING", "ERROR"]) or \
       (LOG_LEVEL == "ERROR" and level == "ERROR"):

        icon = ""
        if level == "SUCCESS": icon = "✅"
        elif level == "WARNING": icon = "⚠️"
        elif level == "ERROR": icon = "❌"
        elif level == "DEBUG": icon = "🐞"

        print(f"[{get_log_timestamp()}] [{component}] [{level}] {icon} {message}")

def log_debug(message, component="ANALYZER"): log_message("DEBUG", message, component)
def log_info(message, component="ANALYZER"): log_message("INFO", message, component)
def log_success(message, component="ANALYZER"): log_message("SUCCESS", message, component)
def log_warning(message, component="ANALYZER"): log_message("WARNING", message, component)
def log_error(message, component="ANALYZER"): log_message("ERROR", message, component)
# --- 日誌記錄函數結束 ---


class DBManager:
    """管理 DuckDB 快取"""
    def __init__(self, db_path):
        self.db_path = db_path
        self.component = "DB_MANAGER"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            self._initialize_schema()
            log_info(f"成功連接到快取資料庫: {self.db_path}", self.component)
        except Exception as e:
            log_error(f"連接或初始化快取資料庫失敗: {e}", self.component)
            self.conn = None # 標記連接失敗

    def _initialize_schema(self):
        """初始化資料庫表結構"""
        if not self.conn: return
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    ticker VARCHAR,
                    datetime TIMESTAMP, -- yfinance返回的索引通常是 Timestamp[ns, UTC] 或 Timestamp[ns]
                    interval VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    dividends DOUBLE, -- 股息
                    stock_splits DOUBLE, -- 股票分割
                    fetched_at TIMESTAMP DEFAULT current_timestamp, -- 數據獲取時間
                    PRIMARY KEY (ticker, interval, datetime) -- datetime 已經是 TIMESTAMP，可以直接做主鍵
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS request_log (
                    request_id UUID DEFAULT uuid() PRIMARY KEY,
                    ticker VARCHAR,
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    requested_interval VARCHAR,
                    actual_interval VARCHAR,
                    status VARCHAR, -- SUCCESS, FAILED_NET, FAILED_CACHE, DOWNGRADED
                    cache_hit BOOLEAN,
                    error_message TEXT,
                    rows_fetched INTEGER,
                    request_time TIMESTAMP DEFAULT current_timestamp
                );
            """)
            log_info("資料庫 Schema 初始化/驗證完畢。", self.component)
        except Exception as e:
            log_error(f"初始化 Schema 失敗: {e}", self.component)
            # 如果 Schema 初始化失敗，可能無法繼續，可以考慮拋出異常或關閉連接

    def get_cached_data(self, ticker, start_dt, end_dt, interval):
        """從快取中獲取數據"""
        if not self.conn: return None

        # DuckDB 的 timestamp 比較可以直接用 Python datetime 對象
        query = """
            SELECT datetime, open, high, low, close, volume, dividends, stock_splits
            FROM stock_data
            WHERE ticker = ? AND interval = ? AND datetime >= ? AND datetime < ?
            ORDER BY datetime;
        """
        # 注意：yfinance 的 end date 是包含的，但 SQL BETWEEN 通常也是包含的。
        # 如果 yf 的 end_date 是 '2025-07-04'，它會取到 '2025-07-04 00:00:00' 的數據（如果 interval 是日或更細）。
        # 為確保一致性，這裡使用 datetime < end_dt + 1 day (對於日以上週期)
        # 或者，如果 start/end 是 datetime 對象，可以直接比較。
        # 假設傳入的 start_dt, end_dt 已經是 datetime.datetime 對象

        try:
            log_debug(f"查詢快取: ticker={ticker}, interval={interval}, start={start_dt}, end={end_dt}", self.component)
            # 調整 end_dt 以匹配 yfinance 的行為 (end date is inclusive)
            # 如果是日級數據，yf.download(end='2024-01-05') 會包含 2024-01-05 的數據。
            # SQL 的 datetime < end_date_exclusive
            end_date_exclusive = end_dt + timedelta(days=1) if interval == '1d' else end_dt

            df = self.conn.execute(query, [ticker, interval, start_dt, end_date_exclusive]).fetchdf()

            if not df.empty:
                log_success(f"CACHE HIT: 找到 {len(df)} 筆 '{ticker}' ({interval}) 的快取記錄。", self.component)
                # 將 'datetime' 欄位設為索引，並確保是 DatetimeIndex
                df['datetime'] = pd.to_datetime(df['datetime'])
                return df.set_index('datetime')
            else:
                log_info(f"CACHE MISS: 未找到 '{ticker}' ({interval}) 在指定範圍內的快取記錄。", self.component)
                return None
        except Exception as e:
            log_error(f"查詢快取失敗: {e}", self.component)
            return None

    def store_data(self, df, ticker, interval):
        """將數據存儲到快取"""
        if not self.conn or df.empty: return

        df_to_store = df.reset_index() # 將 DatetimeIndex 轉回普通欄位 'datetime'
        df_to_store['ticker'] = ticker
        df_to_store['interval'] = interval

        # yfinance 的 auto_adjust=True 會移除 Dividends 和 Stock Splits 欄位，但 close 會是調整後的
        # 如果 auto_adjust=False，才會有 Dividends 和 Stock Splits
        # 為保持欄位一致性，即使沒有也創建空列
        if 'Dividends' not in df_to_store.columns: df_to_store['Dividends'] = 0.0
        if 'Stock Splits' not in df_to_store.columns: df_to_store['Stock Splits'] = 0.0

        # 重命名以匹配資料庫欄位
        df_to_store = df_to_store.rename(columns={
            'Datetime':'datetime', 'Date':'datetime', # 處理可能的索引名稱
            'Open':'open', 'High':'high', 'Low':'low', 'Close':'close', 'Volume':'volume',
            'Dividends': 'dividends', 'Stock Splits': 'stock_splits'
        })

        # 確保欄位順序和存在性，與 stock_data 表定義一致
        expected_db_cols = ['ticker', 'datetime', 'interval', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']
        df_final_to_store = df_to_store[[col for col in expected_db_cols if col in df_to_store.columns]]

        # 添加缺失的預期欄位並設為 None 或 0
        for col in expected_db_cols:
            if col not in df_final_to_store.columns:
                if col in ['open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']:
                    df_final_to_store[col] = 0.0 if col not in ['volume'] else 0
                else: # ticker, datetime, interval 應該存在
                    pass


        try:
            # 使用 INSERT OR REPLACE INTO ... BY NAME 語法，更安全
            # DuckDB 會自動匹配欄位名，不需要嚴格順序
            self.conn.execute("INSERT OR REPLACE INTO stock_data BY NAME SELECT * FROM df_final_to_store;")
            log_success(f"已將 {len(df_final_to_store)} 筆 '{ticker}' ({interval}) 的記錄存入快取。", self.component)
        except Exception as e:
            log_error(f"存儲數據到快取失敗: {e}", self.component)
            log_debug(f"待儲存的 DataFrame 資訊:\n{df_final_to_store.info()}", self.component)
            log_debug(f"待儲存的 DataFrame 樣本:\n{df_final_to_store.head()}", self.component)

    def log_request(self, ticker, start_date, end_date, requested_interval, actual_interval, status, cache_hit, error_message, rows_fetched):
        """記錄請求日誌"""
        if not self.conn: return
        try:
            self.conn.execute("""
                INSERT INTO request_log (ticker, start_date, end_date, requested_interval, actual_interval, status, cache_hit, error_message, rows_fetched)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [ticker, start_date, end_date, requested_interval, actual_interval, status, cache_hit, error_message, rows_fetched])
            log_debug(f"請求日誌已記錄: {ticker}, {requested_interval}, status={status}", self.component)
        except Exception as e:
            log_error(f"記錄請求日誌失敗: {e}", self.component)

    def close(self):
        if self.conn:
            self.conn.close()
            log_info("資料庫連接已關閉。", self.component)


class YFinanceClient:
    """處理 yfinance 數據請求，具備智能降級能力"""
    def __init__(self):
        self.component = "YF_CLIENT"

    def fetch_data(self, ticker_obj, start_dt, end_dt, interval, auto_adjust=True, prepost=False):
        """
        從 yfinance 獲取數據。
        ticker_obj: yf.Ticker() 對象
        start_dt, end_dt: datetime.datetime 對象
        interval: 字符串, e.g., '1m', '1d', '1wk'
        """
        log_info(f"嘗試從網路獲取 {ticker_obj.ticker} 在 {start_dt.strftime('%Y-%m-%d')} 到 {end_dt.strftime('%Y-%m-%d')} 的 '{interval}' 數據...", self.component)

        # yfinance 的 end date 是包含的
        # yfinance 的 start/end 接受 YYYY-MM-DD 格式的字符串或 datetime 對象
        try:
            data = ticker_obj.history(start=start_dt, end=end_dt, interval=interval, auto_adjust=auto_adjust, prepost=prepost)

            if data.empty:
                # 檢查是否因為股票在該期間不存在 (例如，尚未IPO或已下市)
                # yfinance 對於完全無數據的股票，history() 會返回空 DataFrame
                # 有時 ticker.info 可能因網路問題或 API 限制而失敗
                try:
                    info = ticker_obj.info
                    if not info or 'symbol' not in info : # 'quoteType' in info and info['quoteType'] == "NONE"
                        log_warning(f"股票代碼 '{ticker_obj.ticker}' 可能不存在或無有效報價資訊。", self.component)
                        return None, interval, "error_no_such_ticker" # 特殊返回碼
                except Exception as e_info:
                     log_warning(f"獲取 {ticker_obj.ticker} 的 info 失敗: {e_info}. 無法確認股票是否存在。", self.component)


                log_warning(f"未找到 {ticker_obj.ticker} 在請求範圍內的 '{interval}' 數據。", self.component)
                # 返回 None, interval, 和一個狀態碼
                return None, interval, "error_no_data_for_interval"

            # 數據清洗：移除完全是NA的行 (yfinance 有時會返回全NA行)
            data.dropna(how='all', inplace=True)
            if data.empty:
                log_warning(f"移除NA行後，{ticker_obj.ticker} 的 '{interval}' 數據為空。", self.component)
                return None, interval, "error_data_all_na"

            log_success(f"成功獲取 {len(data)} 筆 {ticker_obj.ticker} 的 '{interval}' 數據。", self.component)
            return data, interval, "success" # 成功獲取

        except Exception as e:
            log_error(f"獲取 '{interval}' 數據時發生錯誤 for {ticker_obj.ticker}: {e}", self.component)
            # 這裡可以根據錯誤類型做更細緻的判斷，例如是否為 "No data found, symbol may be delisted"
            if "No data found for this period" in str(e) or "No data found, symbol may be delisted" in str(e):
                 return None, interval, "error_no_data_for_interval"
            return None, interval, f"error_network_or_api: {str(e)}"


    def fetch_with_downgrade(self, ticker_str, start_dt, end_dt, requested_interval):
        """嘗試獲取數據，如果請求的 interval 失敗，則降級到 '1d'"""
        log_info(f"開始數據獲取流程: Ticker={ticker_str}, Interval={requested_interval}", self.component)

        try:
            ticker_obj = yf.Ticker(ticker_str)
            # 先檢查 ticker.info 是否有效，判斷股票是否存在
            # 這一步可能會拋出異常如果股票不存在或網路問題
            if not hasattr(ticker_obj, 'info') or not ticker_obj.info or 'symbol' not in ticker_obj.info:
                 # 嘗試 .fast_info (如果可用)
                try:
                    if not hasattr(ticker_obj, 'fast_info') or not ticker_obj.fast_info or not ticker_obj.fast_info.currency :
                        log_error(f"股票代碼 '{ticker_str}' 無法獲取有效資訊，可能不存在或網路問題。", self.component)
                        return None, requested_interval, "error_invalid_ticker_info"
                except Exception as e_fast_info:
                    log_warning(f"使用 fast_info 檢查 {ticker_str} 失敗: {e_fast_info}", self.component)
                    # 即使 fast_info 失敗，也繼續嘗試 history，讓 history 的錯誤處理來決定
                    pass

        except Exception as e_ticker:
            log_error(f"創建 yf.Ticker('{ticker_str}') 物件失敗: {e_ticker}", self.component)
            return None, requested_interval, "error_ticker_creation_failed"

        # 嘗試原始請求的 interval
        # 對於分鐘級數據，yfinance 有限制：只能獲取最近60天的1m數據，且單次請求最多7天
        # 我們需要在調用前對日期範圍進行調整，如果請求的是1m數據
        # 但這裡我們先按原樣請求，讓 yfinance 自行處理或報錯

        # 特別處理1分鐘數據的日期範圍限制 (yfinance: "1m data not available for startTime=... and endTime=...")
        # yfinance 限制 1m 數據查詢範圍最多 7 天，且必須在最近 60 天內
        # 如果請求的1m數據範圍過大或過舊，直接判定為無法獲取，嘗試降級 (如果不是1d)
        is_1m_too_old_or_wide = False
        if requested_interval == '1m':
            if (datetime.now() - start_dt).days > 60 : # 數據起始日不在60天內
                is_1m_too_old_or_wide = True
                log_warning(f"請求的 1m 數據起始日期 ({start_dt.strftime('%Y-%m-%d')}) 過舊 (超過60天)，將嘗試降級。", self.component)
            elif (end_dt - start_dt).days > 7: # 請求範圍超過7天
                is_1m_too_old_or_wide = True
                log_warning(f"請求的 1m 數據範圍 ({(end_dt - start_dt).days}天) 過大 (超過7天)，將嘗試降級。", self.component)

        data, actual_interval, status_code = None, requested_interval, ""
        if not is_1m_too_old_or_wide:
            data, actual_interval, status_code = self.fetch_data(ticker_obj, start_dt, end_dt, requested_interval)

        if status_code == "success":
            return data, actual_interval, status_code
        elif status_code == "error_no_such_ticker" or status_code == "error_invalid_ticker_info" or status_code == "error_ticker_creation_failed":
            return None, actual_interval, status_code # 股票不存在，無需降級

        # 如果原始請求失敗 (非股票不存在原因) 且不是 '1d'，則嘗試降級到 '1d'
        if requested_interval != '1d':
            log_warning(f"獲取 '{requested_interval}' 數據失敗或因限制無法獲取。狀態: {status_code}。嘗試降級到 '1d'...", self.component)
            # 即使是 is_1m_too_old_or_wide 也會走到這裡嘗試1d
            data_daily, actual_interval_daily, status_code_daily = self.fetch_data(ticker_obj, start_dt, end_dt, '1d')

            if status_code_daily == "success":
                log_success(f"降級成功，已獲取 {ticker_str} 的 '1d' 數據。", self.component)
                return data_daily, actual_interval_daily, "downgraded_to_1d" # 特殊狀態碼表示降級
            else:
                log_error(f"降級到 '1d' 數據也失敗。狀態: {status_code_daily}", self.component)
                return None, '1d', status_code_daily # 返回降級嘗試的狀態
        else: # 如果請求的就是 '1d' 且失敗了
            log_error(f"獲取 '1d' 數據失敗，無可再降級。狀態: {status_code}", self.component)
            return None, requested_interval, status_code # 返回原始失敗狀態

def generate_report_text(df, ticker, requested_interval, actual_interval, start_s, end_s, status_code):
    """生成文本格式的分析報告 (用於日誌輸出)"""
    report_lines = []
    report_lines.append("\n" + "="*60)
    report_lines.append(f"市場數據分析報告 (YFinance)")
    report_lines.append("="*60)
    report_lines.append(f"股票代碼: {ticker}")
    report_lines.append(f"查詢期間: {start_s} 至 {end_s}")
    report_lines.append(f"請求時間顆粒度: {requested_interval}")

    valid_success_statuses = ["success", "downgraded_to_1d", "success_cache"]

    if status_code == "error_no_such_ticker" or status_code == "error_invalid_ticker_info":
        report_lines.append("-" * 60)
        report_lines.append(f"錯誤：股票代碼 '{ticker}' 可能不存在或無法獲取有效市場數據。")
        report_lines.append("="*60 + "\n")
        return "\n".join(report_lines)

    if status_code not in valid_success_statuses: # 修正條件
        report_lines.append("-" * 60)
        report_lines.append(f"錯誤：無法獲取 '{ticker}' 的數據。狀態: {status_code}")
        report_lines.append("="*60 + "\n")
        return "\n".join(report_lines)

    if df is None or df.empty:
        report_lines.append("-" * 60)
        report_lines.append("錯誤：未能獲取到任何數據用於生成報告。")
        report_lines.append(f"最終嘗試時間顆粒度: {actual_interval}")
        report_lines.append("="*60 + "\n")
        return "\n".join(report_lines)

    report_lines.append(f"實際時間顆粒度: {actual_interval}")
    if requested_interval != actual_interval:
        report_lines.append(f"⚠️ 注意: 由於無法獲取 '{requested_interval}' 數據，已自動降級至 '{actual_interval}' 數據。")

    report_lines.append("-" * 60)
    report_lines.append("數據預覽 (前 5 筆):")
    report_lines.append(df.head().to_string())
    report_lines.append("\n數據預覽 (後 5 筆):")
    report_lines.append(df.tail().to_string())
    report_lines.append("-" * 60)
    report_lines.append(f"總筆數: {len(df)}")
    if not df.empty:
        report_lines.append(f"最早數據點: {df.index.min()}")
        report_lines.append(f"最晚數據點: {df.index.max()}")
    report_lines.append("="*60 + "\n")
    return "\n".join(report_lines)


def main(ticker, start_str, end_str, interval):
    log_info(f"--- [主控制器] 接收任務: Ticker={ticker}, Interval={interval}, Start={start_str}, End={end_str} ---")

    db_manager = DBManager(DB_PATH)
    if not db_manager.conn: # 如果資料庫連接失敗，則無法繼續
        log_error("由於資料庫連接失敗，分析器無法繼續執行。", "MAIN")
        return

    final_data = None
    actual_interval = interval
    cache_hit = False
    status_code = "pending"
    error_msg_for_log = None
    rows_fetched_count = 0

    try:
        # 解析日期字符串
        start_dt = datetime.strptime(start_str, '%Y-%m-%d')
        end_dt = datetime.strptime(end_str, '%Y-%m-%d')
        if end_dt < start_dt:
            log_error("結束日期不能早於開始日期。", "MAIN")
            status_code = "error_invalid_date_range"
            raise ValueError("結束日期不能早於開始日期。")

    except ValueError as e_date:
        log_error(f"日期格式錯誤: {e_date}", "MAIN")
        db_manager.log_request(ticker, None, None, interval, interval, "error_invalid_date_format", False, str(e_date), 0)
        db_manager.close()
        return

    # 1. 檢查快取
    # 注意：傳給 get_cached_data 的是 datetime 對象
    cached_data = db_manager.get_cached_data(ticker, start_dt, end_dt, interval)

    if cached_data is not None and not cached_data.empty:
        # 驗證快取數據是否完整覆蓋請求範圍 (這一步驟比較複雜，暫時簡化)
        # 簡單檢查：如果快取的數據量看起來合理 (例如，日數 * 預期每日筆數)
        # 此處假設如果 get_cached_data 返回非空 DataFrame，則認為快取命中且數據可用
        log_success(f"任務 '{ticker}' ({interval}) 數據完全來自快取。", "MAIN")
        final_data = cached_data
        cache_hit = True
        status_code = "success_cache"
        rows_fetched_count = len(final_data)
    else:
        log_info(f"快取未命中或數據不完整 for '{ticker}' ({interval})。嘗試從網路獲取...", "MAIN")
        client = YFinanceClient()
        # 傳給 fetch_with_downgrade 的是 datetime 對象
        fetched_data, actual_fetch_interval, fetch_status = client.fetch_with_downgrade(ticker, start_dt, end_dt, interval)

        actual_interval = actual_fetch_interval # 更新實際使用的 interval
        status_code = fetch_status # 更新狀態碼

        if fetch_status == "success" or fetch_status == "downgraded_to_1d":
            if fetched_data is not None and not fetched_data.empty:
                final_data = fetched_data
                rows_fetched_count = len(final_data)
                # 3. 存入快取 (使用實際獲取的 interval)
                db_manager.store_data(final_data, ticker, actual_interval)
            else: # 即使狀態是 success/downgraded，數據也可能為空
                status_code = "error_fetch_empty_despite_status" if final_data is None else status_code
                error_msg_for_log = f"網路獲取狀態為 {fetch_status} 但數據為空。"
                log_warning(error_msg_for_log, "MAIN")
        else: # 獲取失敗
            error_msg_for_log = f"網路獲取數據失敗，狀態: {fetch_status}"
            log_error(error_msg_for_log, "MAIN")
            # status_code 已經是 fetch_status 了

    # 4. 生成報告 (即使數據為空或獲取失敗，也嘗試生成帶有錯誤訊息的報告)
    report_output = generate_report_text(final_data, ticker, interval, actual_interval, start_str, end_str, status_code)
    print(report_output) # 直接打印到控制台

    # 5. 記錄請求到資料庫
    db_manager.log_request(ticker, start_dt, end_dt, interval, actual_interval, status_code, cache_hit, error_msg_for_log, rows_fetched_count)

    db_manager.close()
    log_info(f"--- [主控制器] 任務完成: Ticker={ticker}, Requested Interval={interval}, Final Status={status_code} ---", "MAIN")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="多重時間尺度 YFinance 數據分析器 (TAINTELLIGENCE)")
    parser.add_argument("--ticker", required=True, help="股票代碼 (例如: TSLA, AAPL)")
    parser.add_argument("--interval", required=True, help="時間週期 (例如: 1m, 5m, 1d, 1wk, 1mo)")
    parser.add_argument("--start", required=True, help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--log_level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="設定日誌級別")

    args = parser.parse_args()
    LOG_LEVEL = args.log_level.upper() # 設定全局日誌級別

    main(args.ticker, args.start, args.end, args.interval)
