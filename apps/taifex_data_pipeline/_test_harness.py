# -*- coding: utf-8 -*-
# 整合測試腳本 v35.2 - 驗證高速載入器與指紋機制
import os
import sys
import subprocess
import shutil
import zipfile
import duckdb

# --- 測試配置 ---
TEST_WORKSPACE = "/tmp/test_harness_v35_p2"
RAW_DATA_DIR = os.path.join(TEST_WORKSPACE, "raw_files")
DB_DIR = os.path.join(TEST_WORKSPACE, "databases")
METADATA_DB_PATH = os.path.join(DB_DIR, "pipeline_metadata.duckdb")
RAW_DB_PATH = os.path.join(DB_DIR, "raw_taifex.duckdb")

# 獲取 run.py 和 manager.py 的絕對路徑
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    PIPELINE_RUN_SCRIPT = os.path.join(current_dir, "run.py")
except Exception as e:
    print(f"路徑設定錯誤: {e}")
    sys.exit(1)


def print_header(title):
    print("\n" + "="*80)
    print(f"🧪  {title}")
    print("="*80)

def setup_test_environment():
    """清理並建立一個乾淨的測試環境"""
    print_header("1. 建立測試環境")
    if os.path.exists(TEST_WORKSPACE):
        shutil.rmtree(TEST_WORKSPACE)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)

    # 建立虛假數據檔案
    # File A: 包含 2 個 CSV 成員
    with zipfile.ZipFile(os.path.join(RAW_DATA_DIR, "file_A.zip"), 'w') as zf:
        zf.writestr("futures.csv", "col1,col2\nfuture1,100")
        zf.writestr("options.csv", "colA,colB\noptionA,200")

    # File B: 只有 1 個 CSV 成員
    with open(os.path.join(RAW_DATA_DIR, "file_B.csv"), 'w') as f:
        f.write("header1,header2\nsingle_row,300")

    print(f"✅ 測試環境 '{TEST_WORKSPACE}' 建立完畢。")

def run_pipeline(input_files: list) -> subprocess.CompletedProcess:
    """執行高速載入器腳本"""
    cmd = [
        sys.executable, PIPELINE_RUN_SCRIPT,
        "--input-files"] + input_files + [
        "--db-output-dir", DB_DIR,
        "--db-name", os.path.basename(RAW_DB_PATH),
        "--metadata-db-path", METADATA_DB_PATH,
        "--log-level", "INFO"
    ]
    print(f"🚀 執行指令: {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')

def verify_db_state(db_path, table, expected_rows, description, fingerprint_to_check=None, expected_etl_version=None):
    """驗證資料庫狀態，可選檢查特定指紋的 ETL 版本"""
    print(f"🔍 {description}")
    if not os.path.exists(db_path):
        print(f"❌ 驗證失敗: 資料庫檔案不存在 {db_path}")
        return False

    conn = duckdb.connect(db_path, read_only=True)
    success = True
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count == expected_rows:
            print(f"✅ 驗證成功: 表格 '{table}' 中有 {count} 筆記錄，符合預期。")
        else:
            print(f"❌ 驗證失敗: 表格 '{table}' 中有 {count} 筆記錄，預期為 {expected_rows}。")
            # 打印表格內容以供除錯
            print("目前表格內容：")
            print(conn.execute(f"SELECT * FROM {table}").fetchdf())
            success = False

        if fingerprint_to_check and expected_etl_version:
            etl_version_in_db = conn.execute(
                f"SELECT etl_version FROM {table} WHERE fingerprint = ?", [fingerprint_to_check]
            ).fetchone()
            if etl_version_in_db and etl_version_in_db[0] == expected_etl_version:
                print(f"✅ 驗證成功: 指紋 '{fingerprint_to_check[:8]}...' 的 ETL 版本為 '{etl_version_in_db[0]}'，符合預期。")
            elif not etl_version_in_db:
                print(f"❌ 驗證失敗: 在表格 '{table}' 中找不到指紋 '{fingerprint_to_check[:8]}...'。")
                success = False
            else:
                print(f"❌ 驗證失敗: 指紋 '{fingerprint_to_check[:8]}...' 的 ETL 版本為 '{etl_version_in_db[0]}'，預期為 '{expected_etl_version}'。")
                success = False

        return success
    except Exception as e:
        print(f"❌ 驗證失敗: 查詢資料庫時發生錯誤: {e}")
        return False
    finally:
        conn.close()

def get_file_fingerprint(file_path):
    """計算檔案的 SHA256 指紋 - 與 manager.py 中的邏輯保持一致"""
    import hashlib
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception:
        return None

def main():
    all_tests_passed = True
    try:
        setup_test_environment()

        file_a_path = os.path.join(RAW_DATA_DIR, "file_A.zip")
        file_b_path = os.path.join(RAW_DATA_DIR, "file_B.csv")

        # --- 首次執行測試 ---
        print_header("2. 首次執行 - 處理新檔案")
        files_to_process_1 = [file_a_path, file_b_path]
        result1 = run_pipeline(files_to_process_1)
        print("--- stdout ---")
        print(result1.stdout)
        print("--- stderr ---")
        print(result1.stderr)
        if not (result1.returncode == 0 and \
                verify_db_state(METADATA_DB_PATH, "processed_files", 2, "驗證「作戰日誌」是否記錄了 2 個檔案", fingerprint_to_check=get_file_fingerprint(file_a_path), expected_etl_version="v35.0-loader") and \
                verify_db_state(RAW_DB_PATH, "raw_import_log", 3, "驗證「原始數據艙」是否包含了 3 筆原始數據 (2 from zip, 1 from csv)")):
            all_tests_passed = False
            print("❌ 首次執行測試失敗")


        # --- 重複處理驗證 ---
        if all_tests_passed:
            print_header("3. 重複執行 - 驗證指紋跳過機制")
            result2 = run_pipeline(files_to_process_1) # 再次處理相同的檔案
            print("--- stdout ---")
            print(result2.stdout)
            print("--- stderr ---")
            print(result2.stderr)
            if not (result2.returncode == 0 and \
                    "偵測到已處理檔案" in result2.stdout and \
                    "跳過: file_A.zip" in result2.stdout and \
                    "跳過: file_B.csv" in result2.stdout and \
                    verify_db_state(METADATA_DB_PATH, "processed_files", 2, "驗證「作戰日誌」總筆數是否仍為 2") and \
                    verify_db_state(RAW_DB_PATH, "raw_import_log", 3, "驗證「原始數據艙」總筆數是否仍為 3")):
                all_tests_passed = False
                print("❌ 重複處理驗證失敗")

        # --- 增量處理驗證 ---
        if all_tests_passed:
            print_header("4. 增量執行 - 處理新檔案與修改過的檔案")

            # 獲取 file_B 的原始指紋，用於稍後驗證其 etl_version 未變
            original_fingerprint_b = get_file_fingerprint(file_b_path)

            # 修改 file_A.zip (重新寫入，指紋會改變)
            with zipfile.ZipFile(file_a_path, 'w') as zf:
                zf.writestr("new_member.csv", "new_data,400")
            modified_fingerprint_a = get_file_fingerprint(file_a_path)

            # 新增 file_C.csv
            file_c_path = os.path.join(RAW_DATA_DIR, "file_C.csv")
            with open(file_c_path, 'w') as f:
                f.write("another,500")
            fingerprint_c = get_file_fingerprint(file_c_path)

            files_to_process_3 = [file_a_path, file_b_path, file_c_path]
            result3 = run_pipeline(files_to_process_3)
            print("--- stdout ---")
            print(result3.stdout)
            print("--- stderr ---")
            print(result3.stderr)

            if not (result3.returncode == 0 and \
                    "跳過: file_B.csv" in result3.stdout and \
                    "處理新檔案: file_A.zip" in result3.stdout and \
                    "處理新檔案: file_C.csv" in result3.stdout and \
                    verify_db_state(METADATA_DB_PATH, "processed_files", 4, "驗證「作戰日誌」總筆數是否為 4 (A舊, B, A新, C)") and \
                    verify_db_state(METADATA_DB_PATH, "processed_files", 4, "檢查 file_A (修改後) 的 ETL 版本", fingerprint_to_check=modified_fingerprint_a, expected_etl_version="v35.0-loader") and \
                    verify_db_state(METADATA_DB_PATH, "processed_files", 4, "檢查 file_B (未修改) 的 ETL 版本", fingerprint_to_check=original_fingerprint_b, expected_etl_version="v35.0-loader") and \
                    verify_db_state(METADATA_DB_PATH, "processed_files", 4, "檢查 file_C (新增) 的 ETL 版本", fingerprint_to_check=fingerprint_c, expected_etl_version="v35.0-loader") and \
                    verify_db_state(RAW_DB_PATH, "raw_import_log", 5, "驗證「原始數據艙」總筆數是否為 5 (2舊A + 1舊B + 1新A + 1新C)")):
                all_tests_passed = False
                print("❌ 增量處理驗證失敗")

    except Exception as e:
        import traceback
        print(f"測試腳本執行期間發生未預期的嚴重錯誤: {e}")
        traceback.print_exc()
        all_tests_passed = False
    finally:
        # --- 清理 ---
        print_header("5. 清理測試環境")
        if os.path.exists(TEST_WORKSPACE):
            shutil.rmtree(TEST_WORKSPACE)
            print(f"✅ 測試工作區 '{TEST_WORKSPACE}' 已成功刪除。")
        else:
            print(f"ℹ️ 測試工作區 '{TEST_WORKSPACE}' 未找到，可能已被清理。")

        if all_tests_passed:
            print("\n" + "#"*30)
            print("✅  高速載入器（含指紋驗證）整合測試成功！")
            print("#"*30)
            sys.exit(0)
        else:
            print("\n" + "!"*30)
            print("❌  高速載入器（含指紋驗證）整合測試失敗。")
            print("!"*30)
            sys.exit(1)

if __name__ == "__main__":
    main()
