# apps/main_executor/log_utils.py
import logging
import sqlite3
import sys # <<<< ADDED IMPORT SYS
import datetime
import threading # 用於線程安全

class SQLiteHandler(logging.Handler):
    """
    一個將日誌記錄到 SQLite 資料庫的 logging handler。
    """
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self._lock = threading.RLock() # 用於 emit 方法的線程安全
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=10) # 增加 timeout
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    level_name TEXT,
                    level_no INTEGER,
                    module TEXT,
                    func_name TEXT,
                    line_no INTEGER,
                    message TEXT,
                    raw_timestamp REAL
                )
            """)
            # 為 hardware_logs 表也創建一個基礎結構，如果 main_executor 會記錄它的話
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hardware_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    cpu_usage REAL,
                    memory_usage REAL,
                    raw_timestamp REAL
                )
            """)
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            # 在 Handler 初始化期間，如果資料庫操作失敗，很難優雅地報告錯誤。
            # 可以考慮打印到 stderr，或者讓應用程式在配置 logger 時捕獲這個異常。
            import sys
            print(f"SQLiteHandler CRITICAL: 無法創建日誌表於 {self.db_path}: {e}", file=sys.stderr, flush=True)
            # 也可以選擇拋出異常，讓調用者處理
            # raise

    def emit(self, record: logging.LogRecord):
        # 使用 RLock 確保對資料庫的寫入是線程安全的
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path, timeout=5)
                cursor = conn.cursor()

                # 準備日誌記錄數據
                log_entry = (
                    datetime.datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], # 格式化時間戳
                    record.levelname,
                    record.levelno,
                    record.module,
                    record.funcName,
                    record.lineno,
                    self.format(record), # 使用 formatter 格式化訊息 (如果 handler 有 formatter)
                                         # 或者直接用 record.getMessage() 獲取原始訊息
                    record.created # 原始的 UNIX 時間戳
                )

                cursor.execute("""
                    INSERT INTO logs (timestamp, level_name, level_no, module, func_name, line_no, message, raw_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, log_entry)
                conn.commit()
                conn.close()
            except sqlite3.Error as e:
                # 如果在 emit 過程中發生錯誤，也應有所處理，避免中斷應用。
                # 例如，可以嘗試回退到打印到 stderr。
                import sys
                sys.stderr.write(f"SQLiteHandler ERROR: 無法寫入日誌到 {self.db_path}: {e}\n")
                sys.stderr.write(f"Log Record that failed: {record.__dict__}\n")
                sys.stderr.flush()
            except Exception as ex: # 捕獲其他潛在錯誤
                import sys
                sys.stderr.write(f"SQLiteHandler UNEXPECTED ERROR: {ex}\n")
                sys.stderr.write(f"Log Record that failed: {record.__dict__}\n")
                sys.stderr.flush()


# 輔助函數：設置日誌記錄器
def setup_logging(log_db_path, level=logging.INFO):
    """
    配置日誌記錄器，將日誌輸出到指定的 SQLite 資料庫。
    """
    logger = logging.getLogger() # 獲取根 logger
    logger.setLevel(level)

    # 移除可能已存在的 handlers，避免重複記錄 (特別是在測試或多次調用時)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    # SQLite Handler
    db_handler = SQLiteHandler(db_path=log_db_path)
    # 可以為 db_handler 設置一個 formatter
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(module)s.%(funcName)s:%(lineno)d] - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    db_handler.setFormatter(formatter)
    logger.addHandler(db_handler)

    # （可選）同時也輸出到控制台，方便調試
    console_handler = logging.StreamHandler(sys.stdout) # 或 sys.stderr
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 返回配置好的 logger 實例，雖然通常直接使用 logging.info() 等即可
    return logger

def log_hardware_metric(db_path, cpu_usage, memory_usage):
    """
    一個簡單的函數，用於將硬體指標記錄到 hardware_logs 表。
    """
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        cursor = conn.cursor()
        raw_ts = time.time()
        ts = datetime.datetime.fromtimestamp(raw_ts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        cursor.execute("""
            INSERT INTO hardware_logs (timestamp, cpu_usage, memory_usage, raw_timestamp)
            VALUES (?, ?, ?, ?)
        """, (ts, cpu_usage, memory_usage, raw_ts))
        conn.commit()
        conn.close()
    except Exception as e:
        # 在記錄硬體指標時出錯，可以選擇打印到 stderr
        import sys
        sys.stderr.write(f"HardwareLog ERROR: 無法寫入硬體日誌到 {db_path}: {e}\n")
        sys.stderr.flush()

# 為了 log_hardware_metric 中的 time.time()
import time
