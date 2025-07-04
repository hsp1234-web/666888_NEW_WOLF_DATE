import duckdb
import pandas as pd
import subprocess
import os

# --- 常量定義 ---
RAW_DB_PATH_ON_DISK = "temp_raw.db"
ANALYTICS_DB_PATH = ":memory:" # 讓 run.py 自己處理記憶體分析資料庫
TRANSFORMER_SCRIPT_PATH = "apps/taifex_data_transformer/run.py"
LOG_LEVEL = "INFO"

# 測試用的 CSV 內容，包含 "成交日期"
TEST_CSV_CONTENT = """\
"成交日期","契約代碼","到期月份(週別)","履約價","買賣權","開盤價","最高價","最低價","收盤價","結算價","成交量","未沖銷契約數","交易時段"
"2023/10/01","TX","202310","","","16000","16050","15950","16020","16020","15000","70000","一般"
"2023/10/01","TXO","202310W1","16000","Call","50","60","40","55","55","500","2000","一般"
"2023/10/01","TXO","202310W1","16000","Put","30","35","20","22","22","400","1500","一般"
"""

def setup_raw_db_with_test_data():
    """
    創建一個 DuckDB 資料庫檔案，包含帶有測試數據的 raw_import_log 表。
    """
    # 如果臨時資料庫已存在，先刪除
    if os.path.exists(RAW_DB_PATH_ON_DISK):
        os.remove(RAW_DB_PATH_ON_DISK)

    con = None
    try:
        con = duckdb.connect(database=RAW_DB_PATH_ON_DISK, read_only=False)
        print(f"成功連接到臨時原始數據庫: {RAW_DB_PATH_ON_DISK}")

        # 創建 raw_import_log 表
        con.execute("""
            CREATE TABLE raw_import_log (
                source_file VARCHAR,
                member_file VARCHAR,
                file_content_as_text TEXT,
                imported_at TIMESTAMP DEFAULT current_timestamp
            );
        """)
        print("成功創建 raw_import_log 表。")

        # 準備要插入的數據
        data_to_insert = [
            ("test_source_20231001.csv", "member_A_20231001.csv", TEST_CSV_CONTENT),
        ]

        # 插入數據
        for row_data in data_to_insert:
            con.execute("INSERT INTO raw_import_log (source_file, member_file, file_content_as_text) VALUES (?, ?, ?)", row_data)
        print(f"成功插入 {len(data_to_insert)} 筆測試數據到 raw_import_log。")

        # 驗證插入
        # result = con.execute("SELECT COUNT(*) FROM raw_import_log").fetchone()
        # print(f"raw_import_log 表中現有 {result[0] if result else 'N/A'} 筆記錄。")
        # content_check = con.execute("SELECT file_content_as_text FROM raw_import_log LIMIT 1").fetchone()
        # print(f"抽樣檢查內容: {content_check[0][:100] if content_check else 'N/A'}...")


    except Exception as e:
        print(f"設置原始數據庫時出錯: {e}")
        raise
    finally:
        if con:
            con.close()
            print(f"已關閉臨時原始數據庫連接: {RAW_DB_PATH_ON_DISK}")

def run_transformer_and_verify():
    """
    執行數據轉換腳本並進行驗證。
    """
    print(f"\n準備執行數據轉換器: {TRANSFORMER_SCRIPT_PATH}...")
    command = [
        "python", TRANSFORMER_SCRIPT_PATH,
        "--raw-db-path", RAW_DB_PATH_ON_DISK,
        "--analytics-db-path", ANALYTICS_DB_PATH, # 使用 :memory:
        "--log-level", LOG_LEVEL
    ]

    print(f"執行指令: {' '.join(command)}")

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate()

        print("\n--- 轉換器標準輸出 ---")
        print(stdout)
        print("--- 轉換器標準輸出結束 ---")

        if stderr:
            print("\n--- 轉換器標準錯誤 ---")
            print(stderr)
            print("--- 轉換器標準錯誤結束 ---")

        if process.returncode != 0:
            print(f"\n數據轉換器執行失敗，返回碼: {process.returncode}")
            return False

        # 驗證標準：
        # 1. 日誌中不得再出現 "[ERROR] '交易日期' column not found" 的錯誤訊息。
        # 2. 執行結束時，日誌中的驗證訊息必須顯示一個大於 0 的數字。

        error_pattern = "[ERROR] '交易日期' column not found"
        success_pattern_prefix = "[INFO] 驗證：共 "
        success_pattern_suffix = " 筆乾淨數據被載入到分析資料庫。"

        found_error = error_pattern in stdout or error_pattern in stderr
        found_success_log = False
        inserted_rows = 0

        # 直接在 stdout 中搜索模式
        import re
        match = re.search(r"\[INFO\] 驗證：共 (\d+) 筆乾淨數據被載入到分析資料庫。", stdout)

        if match:
            inserted_rows = int(match.group(1))
            if inserted_rows > 0:
                found_success_log = True
            print(f"從日誌中解析到載入筆數: {inserted_rows}")
        else:
            print(f"未能在日誌中找到符合 '{success_pattern_prefix}...{success_pattern_suffix}' 模式的行。")


        if not found_error and found_success_log:
            print(f"\n✅ 驗收通過: 未發現 '交易日期' 欄位未找到的錯誤，且成功載入 {inserted_rows} 筆數據。")
            return True
        else:
            if found_error:
                print(f"\n❌ 驗收失敗: 在日誌中發現錯誤 '{error_pattern}'。")
            if not found_success_log:
                print(f"\n❌ 驗收失敗: 未找到成功的載入日誌，或載入筆數為 0。目前解析到的筆數: {inserted_rows}。")
            return False

    except FileNotFoundError:
        print(f"錯誤: 找不到 Python 解釋器或轉換腳本 {TRANSFORMER_SCRIPT_PATH}。請確保它們在 PATH 中或路徑正確。")
        return False
    except Exception as e:
        print(f"執行轉換器時發生未預期錯誤: {e}")
        return False
    finally:
        # 清理臨時資料庫檔案
        if os.path.exists(RAW_DB_PATH_ON_DISK):
            # os.remove(RAW_DB_PATH_ON_DISK)
            # print(f"已清理臨時原始數據庫: {RAW_DB_PATH_ON_DISK}")
            # 暫時不刪除，方便調試
            print(f"臨時原始數據庫保留在: {RAW_DB_PATH_ON_DISK} (供調試)")


if __name__ == "__main__":
    print("--- 開始執行 ELT 測試流程 ---")
    try:
        setup_raw_db_with_test_data()
        success = run_transformer_and_verify()
        if success:
            print("\n--- ELT 測試流程成功結束 ---")
        else:
            print("\n--- ELT 測試流程失敗 ---")
            exit(1) # 以失敗狀態碼退出
    except Exception as e:
        print(f"ELT 測試流程遭遇頂層錯誤: {e}")
        exit(1) # 以失敗狀態碼退出
