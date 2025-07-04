# -*- coding: utf-8 -*-
"""
作戰日誌與指紋驗證模組

核心職責：
1.  檔案指紋計算：提供一個函數，能接收一個檔案路徑，並計算該檔案的 SHA-256 雜湊值。
2.  日誌資料庫互動：
    *   建立一個專用的日誌資料庫（例如 pipeline_metadata.duckdb）。
    *   提供「查詢指紋是否存在」的功能。
    *   提供「寫入新指紋及元數據（檔名、大小、處理時間）」的功能。
"""
import hashlib
import duckdb
import os
import datetime

# 預設的資料庫檔案名稱，可以考慮移到設定檔
# DEFAULT_DB_NAME = "pipeline_metadata.duckdb" # 改由 config 讀取
# DEFAULT_TABLE_NAME = "processed_files" # 改由 config 讀取
from . import config # 匯入設定

def calculate_file_fingerprint(file_path: str) -> str:
    """
    計算給定檔案路徑的 SHA-256 雜湊值。

    Args:
        file_path (str): 要計算雜湊值的檔案路徑。

    Returns:
        str: 檔案的 SHA-256 雜湊值 (十六進位字串)。

    Raises:
        FileNotFoundError: 如果檔案不存在。
        IOError: 如果讀取檔案時發生錯誤。
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # 逐塊讀取檔案以處理大檔案
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        # 可以在這裡加入日誌記錄
        raise
    except IOError:
        # 可以在這裡加入日誌記錄
        raise

class MetadataManager:
    """
    管理管線元數據，主要用於記錄和查詢已處理檔案的指紋。
    """
    def __init__(self, db_path: str = None, table_name: str = None, connection: duckdb.DuckDBPyConnection = None):
        """
        初始化 MetadataManager。
        可以傳入一個已存在的 DuckDB 連線，或者依賴 db_path 參數建立新連線。

        Args:
            db_path (str, optional): DuckDB 資料庫檔案的路徑。
                                     若 connection 為 None 且此參數也為 None，則從 config 讀取。
                                     如果提供了 connection，則此參數被忽略。
            table_name (str, optional): 儲存處理記錄的資料表名稱。
                                        若為 None，則從 config 讀取。
            connection (duckdb.DuckDBPyConnection, optional): 一個已存在的 DuckDB 連線物件。
                                                            如果提供，則管理器將使用此連線。
        """
        self.table_name = table_name if table_name is not None else config.PROCESSED_FILES_TABLE_NAME

        if connection:
            self.conn = connection
            # 如果使用外部連線，db_path 可能不相關或不準確，但為了完整性可以嘗試保留
            self.db_path = db_path # 或者可以設為 None 或從連線中獲取（如果 DuckDB API 支援）
        else:
            self.db_path = db_path if db_path is not None else config.DATABASE_FILENAME
            self.conn = duckdb.connect(database=self.db_path, read_only=False)

        self._initialize_database()

    # def _get_connection(self):
    #     """建立並返回一個 DuckDB 連線。""" # 改為使用 self.conn
    #     return duckdb.connect(database=self.db_path, read_only=False)

    def _initialize_database(self):
        """
        初始化資料庫。如果指定的資料表不存在，則建立它。
        使用實例儲存的連線 self.conn。
        """
        try:
            # 使用參數化查詢以增加安全性並避免 SQL 注入 (雖然此處 table_name 來自內部)
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    fingerprint TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    filesize INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    etl_version TEXT
                )
            """)
        except Exception as e:
            # 實際應用中應使用更完善的日誌機制
            print(f"資料庫初始化錯誤: {e}")
            raise

    def check_fingerprint_exists(self, fingerprint: str) -> bool:
        """
        檢查指定的檔案指紋是否已存在於日誌資料庫中。
        使用實例儲存的連線 self.conn。

        Args:
            fingerprint (str): 要查詢的檔案 SHA-256 指紋。

        Returns:
            bool: 如果指紋存在則返回 True，否則返回 False。
        """
        try:
            # 使用參數化查詢
            result = self.conn.execute(
                f"SELECT 1 FROM {self.table_name} WHERE fingerprint = ?",
                [fingerprint]
            ).fetchone()
            return result is not None
        except Exception as e:
            print(f"查詢指紋時發生錯誤: {e}")
            # 根據錯誤處理策略，可能需要返回 False 或重新拋出例外
            return False

    def write_fingerprint(self, fingerprint: str, filename: str, filesize: int, etl_version: str = None) -> bool:
        """
        將新的檔案指紋及其元數據寫入日誌資料庫。
        如果 fingerprint 已存在，則不執行任何操作 (因 ON CONFLICT DO NOTHING)。

        Args:
            fingerprint (str): 檔案的 SHA-256 指紋。
            filename (str): 原始檔案的名稱。
            filesize (int): 檔案的大小 (位元組)。
            etl_version (str, optional): 處理此檔案的 ETL/管線版本。
                                         若為 None，則從 config 讀取預設版本。

        Returns:
            bool: 如果寫入成功或指紋已存在（未執行寫入但無錯誤）則返回 True，否則返回 False。
        """
        resolved_etl_version = etl_version if etl_version is not None else config.DEFAULT_ETL_VERSION
        try:
            # 使用實例儲存的連線 self.conn
            # 使用參數化查詢
            # processed_at 使用資料庫的 DEFAULT CURRENT_TIMESTAMP
            self.conn.execute(
                f"""
                INSERT INTO {self.table_name} (fingerprint, filename, filesize, etl_version)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(fingerprint) DO NOTHING;
                """,
                [fingerprint, filename, filesize, resolved_etl_version]
            )
            return True
        except Exception as e:
            print(f"寫入指紋時發生錯誤: {e}")
            return False

if __name__ == '__main__':
    # 簡易測試與使用範例
    print("正在執行 MetadataManager 簡易測試...")

    # 建立一個虛擬檔案用於測試
    TEST_FILE_DIR = "temp_test_data"
    TEST_FILE_NAME = "sample_test_file.txt"
    TEST_FILE_PATH = os.path.join(TEST_FILE_DIR, TEST_FILE_NAME)

    os.makedirs(TEST_FILE_DIR, exist_ok=True)
    with open(TEST_FILE_PATH, "w") as f:
        f.write("這是 MetadataManager 的測試檔案內容。")

    file_size = os.path.getsize(TEST_FILE_PATH)

    # 1. 計算檔案指紋
    print(f"計算檔案 '{TEST_FILE_PATH}' 的指紋...")
    try:
        fingerprint1 = calculate_file_fingerprint(TEST_FILE_PATH)
        print(f"  指紋: {fingerprint1}")
    except Exception as e:
        print(f"  計算指紋失敗: {e}")
        fingerprint1 = None

    if fingerprint1:
        # 2. 初始化 MetadataManager (使用記憶體內資料庫進行測試)
        # 傳遞 db_path=":memory:" 以使用記憶體資料庫進行測試。
        # table_name 將使用 config 中的預設值 (除非在此處也覆寫)。
        print("初始化 MetadataManager (使用記憶體資料庫)...")
        manager = MetadataManager(db_path=":memory:")

        # 3. 檢查指紋是否存在 (應該不存在)
        print(f"檢查指紋 '{fingerprint1}' 是否存在...")
        exists = manager.check_fingerprint_exists(fingerprint1)
        print(f"  指紋是否存在: {exists} (預期: False)")
        assert not exists, "測試失敗：新指紋不應存在"

        # 4. 寫入指紋
        print(f"寫入指紋 '{fingerprint1}' (檔案: {TEST_FILE_NAME}, 大小: {file_size})...")
        success = manager.write_fingerprint(fingerprint1, TEST_FILE_NAME, file_size, "v_test")
        print(f"  寫入是否成功: {success} (預期: True)")
        assert success, "測試失敗：指紋寫入失敗"

        # 5. 再次檢查指紋是否存在 (應該存在)
        print(f"再次檢查指紋 '{fingerprint1}' 是否存在...")
        exists_after_write = manager.check_fingerprint_exists(fingerprint1)
        print(f"  指紋是否存在: {exists_after_write} (預期: True)")
        assert exists_after_write, "測試失敗：寫入後的指紋應存在"

        # 6. 嘗試寫入重複指紋 (ON CONFLICT DO NOTHING)
        print(f"嘗試再次寫入重複指紋 '{fingerprint1}'...")
        # 這裡可以修改一下檔案名稱或大小，模擬元數據可能更新的情況，但指紋相同
        success_duplicate = manager.write_fingerprint(fingerprint1, "sample_test_file_copy.txt", file_size + 100, "v_test_dup")
        print(f"  重複寫入是否回報成功: {success_duplicate} (預期: True, 因 ON CONFLICT DO NOTHING)")
        assert success_duplicate, "測試失敗：重複指紋寫入應回報成功"

        # 可以額外驗證資料庫中的內容是否未被不期望地更改 (例如，processed_at 時間戳)
        # 但 ON CONFLICT DO NOTHING 通常意味著原始記錄被保留

        # 7. 測試不同檔案
        TEST_FILE_NAME_2 = "another_sample.txt"
        TEST_FILE_PATH_2 = os.path.join(TEST_FILE_DIR, TEST_FILE_NAME_2)
        with open(TEST_FILE_PATH_2, "w") as f:
            f.write("這是另一個不同的測試檔案。")
        file_size_2 = os.path.getsize(TEST_FILE_PATH_2)

        print(f"計算檔案 '{TEST_FILE_PATH_2}' 的指紋...")
        fingerprint2 = calculate_file_fingerprint(TEST_FILE_PATH_2)
        print(f"  指紋: {fingerprint2}")
        assert fingerprint1 != fingerprint2, "測試失敗：不同檔案應有不同指紋"

        print(f"寫入指紋 '{fingerprint2}'...")
        success2 = manager.write_fingerprint(fingerprint2, TEST_FILE_NAME_2, file_size_2, "v_test_2")
        assert success2, "測試失敗：第二個指紋寫入失敗"

        exists2 = manager.check_fingerprint_exists(fingerprint2)
        assert exists2, "測試失敗：第二個指紋寫入後應存在"

        print("所有簡易測試執行完畢。")

    # 清理測試檔案
    try:
        os.remove(TEST_FILE_PATH)
        os.remove(TEST_FILE_PATH_2)
        os.rmdir(TEST_FILE_DIR)
        print("臨時測試檔案已清理。")
    except OSError as e:
        print(f"清理測試檔案時發生錯誤: {e}")

    print("--- MetadataManager 測試結束 ---")
