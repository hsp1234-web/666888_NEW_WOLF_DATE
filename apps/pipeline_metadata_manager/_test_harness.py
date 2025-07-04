# -*- coding: utf-8 -*-
"""
整合測試腳本 (實彈整合演習) for apps.pipeline_metadata_manager

此腳本自動化地驗證 pipeline_metadata_manager 模組的核心功能：
1.  建立虛假的日誌資料庫和數據檔案。
2.  首次寫入測試：計算指紋並寫入資料庫，驗證寫入成功。
3.  重複寫入測試：嘗試寫入相同檔案，驗證 ON CONFLICT 機制生效，總數未增加。
4.  查詢驗證測試：驗證能正確查詢到已存在的指紋，且查不到不存在的指紋。
"""
import os
import sys
import tempfile
import duckdb
import shutil

# 將 apps 目錄添加到 sys.path，以便能夠匯入 manager
# 假設此腳本位於 apps/pipeline_metadata_manager/ 中
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
APPS_DIR = os.path.dirname(SCRIPT_DIR) # 退回到 apps/
sys.path.insert(0, APPS_DIR)
sys.path.insert(0, os.path.dirname(APPS_DIR)) # 退回到專案根目錄，以便 apps.pipeline_metadata_manager 如此的匯入

from pipeline_metadata_manager import manager as metadata_manager_module
from pipeline_metadata_manager.manager import MetadataManager, calculate_file_fingerprint

# 測試用的表格名稱，可以與 manager.py 中的 config.PROCESSED_FILES_TABLE_NAME 一致
TEST_TABLE_NAME = "processed_files"

def run_tests():
    """執行所有整合測試案例。"""
    temp_dir = None
    test_db_path = None
    test_file_path = None
    conn_for_verification = None
    exit_code = 0

    print("🚀 開始執行 pipeline_metadata_manager 整合測試...")

    try:
        # 1. 測試環境設定
        print("\n[階段 1/5] 設定測試環境...")
        temp_dir = tempfile.mkdtemp(prefix="test_md_harness_")
        test_db_path = os.path.join(temp_dir, "test_metadata_harness.duckdb")
        test_file_path = os.path.join(temp_dir, "sample_test_data_harness.txt")

        print(f"  臨時目錄: {temp_dir}")
        print(f"  測試資料庫路徑: {test_db_path}")
        print(f"  測試資料檔案路徑: {test_file_path}")

        with open(test_file_path, "w", encoding="utf-8") as f:
            f.write("這是整合測試用的虛假檔案內容。")

        file_size = os.path.getsize(test_file_path)
        print(f"  虛假資料檔案 '{os.path.basename(test_file_path)}' 已建立，大小: {file_size} bytes。")

        # 為了讓 MetadataManager 使用這個特定的測試資料庫，我們不傳入 connection
        # 而是傳入 db_path，並讓 manager 自行管理其內部連線。
        # 或者，我們可以先建立一個連線，然後傳給 MetadataManager
        # 這裡選擇後者，以便後續直接用此 conn_for_verification 進行 SQL 查詢

        conn_for_verification = duckdb.connect(test_db_path)
        print(f"  已連線至測試資料庫: {test_db_path}")

        # 傳遞外部連線給 Manager
        # 注意：manager 內部的 table_name 應與此處的 TEST_TABLE_NAME 匹配
        # manager 的 config.py 中的 PROCESSED_FILES_TABLE_NAME 預設是 "processed_files"
        # 我們在 manager 初始化時明確指定 table_name，以確保一致性
        meta_manager = MetadataManager(connection=conn_for_verification, table_name=TEST_TABLE_NAME)
        print("  MetadataManager 已使用外部連線初始化。")


        # 2. 首次寫入測試
        print("\n[階段 2/5] 執行首次寫入測試...")
        fingerprint1 = calculate_file_fingerprint(test_file_path)
        print(f"  計算得到檔案指紋: {fingerprint1}")

        success_write = meta_manager.write_fingerprint(
            fingerprint=fingerprint1,
            filename=os.path.basename(test_file_path),
            filesize=file_size,
            etl_version="test_harness_v1"
        )
        assert success_write, "首次寫入指紋失敗！"
        print(f"  指紋 '{fingerprint1}' 首次寫入操作回報成功。")

        # 驗證資料庫
        count_after_first_write = conn_for_verification.execute(
            f"SELECT COUNT(*) FROM {TEST_TABLE_NAME} WHERE fingerprint = ?", [fingerprint1]
        ).fetchone()[0]
        assert count_after_first_write == 1, \
            f"首次寫入後，資料庫中指紋 '{fingerprint1}' 的計數應為 1，實際為 {count_after_first_write}"
        print(f"  SQL 驗證：資料庫中指紋 '{fingerprint1}' 的計數為 1。首次寫入成功！")

        # 額外驗證寫入的內容
        record = conn_for_verification.execute(
            f"SELECT filename, filesize, etl_version FROM {TEST_TABLE_NAME} WHERE fingerprint = ?", [fingerprint1]
        ).fetchone()
        assert record is not None, "無法從資料庫中檢索到寫入的記錄"
        assert record[0] == os.path.basename(test_file_path), f"檔名不匹配: 預期 {os.path.basename(test_file_path)}, 得到 {record[0]}"
        assert record[1] == file_size, f"檔案大小不匹配: 預期 {file_size}, 得到 {record[1]}"
        assert record[2] == "test_harness_v1", f"ETL 版本不匹配: 預期 test_harness_v1, 得到 {record[2]}"
        print(f"  SQL 驗證：寫入的檔名、大小、ETL 版本均正確。")


        # 3. 重複寫入測試
        print("\n[階段 3/5] 執行重複寫入測試...")
        # 獲取重複寫入前的總行數
        total_rows_before_duplicate_write = conn_for_verification.execute(
            f"SELECT COUNT(*) FROM {TEST_TABLE_NAME}"
        ).fetchone()[0]

        success_duplicate_write = meta_manager.write_fingerprint(
            fingerprint=fingerprint1, # 相同的指紋
            filename=os.path.basename(test_file_path) + "_dupe", # 嘗試用不同檔名，但不應影響
            filesize=file_size + 10, # 嘗試用不同大小
            etl_version="test_harness_v_dupe"
        )
        assert success_duplicate_write, "重複寫入操作應回報成功 (因 ON CONFLICT DO NOTHING)"
        print(f"  指紋 '{fingerprint1}' 重複寫入操作回報成功。")

        # 驗證資料庫總行數未增加
        total_rows_after_duplicate_write = conn_for_verification.execute(
            f"SELECT COUNT(*) FROM {TEST_TABLE_NAME}"
        ).fetchone()[0]
        assert total_rows_after_duplicate_write == total_rows_before_duplicate_write, \
            f"重複寫入後，資料庫總行數應為 {total_rows_before_duplicate_write}，實際為 {total_rows_after_duplicate_write}。ON CONFLICT 可能未生效。"
        print(f"  SQL 驗證：資料庫總行數 ({total_rows_after_duplicate_write}) 未增加。ON CONFLICT DO NOTHING 機制生效！")

        # 驗證原始記錄未被修改
        record_after_dupe = conn_for_verification.execute(
            f"SELECT filename, filesize, etl_version FROM {TEST_TABLE_NAME} WHERE fingerprint = ?", [fingerprint1]
        ).fetchone()
        assert record_after_dupe[0] == os.path.basename(test_file_path), f"重複寫入後，檔名被修改: 預期 {os.path.basename(test_file_path)}, 得到 {record_after_dupe[0]}"
        assert record_after_dupe[1] == file_size, f"重複寫入後，檔案大小被修改: 預期 {file_size}, 得到 {record_after_dupe[1]}"
        assert record_after_dupe[2] == "test_harness_v1", f"重複寫入後，ETL 版本被修改: 預期 test_harness_v1, 得到 {record_after_dupe[2]}"
        print(f"  SQL 驗證：原始記錄的檔名、大小、ETL 版本均未被修改。")


        # 4. 查詢驗證測試
        print("\n[階段 4/5] 執行查詢驗證測試...")
        exists1 = meta_manager.check_fingerprint_exists(fingerprint1)
        assert exists1, f"查詢已存在的指紋 '{fingerprint1}' 應返回 True，實際為 False。"
        print(f"  查詢已存在的指紋 '{fingerprint1}'：成功找到 (True)。")

        non_existent_fingerprint = "this_fingerprint_does_not_exist_in_the_db_12345"
        exists_non_existent = meta_manager.check_fingerprint_exists(non_existent_fingerprint)
        assert not exists_non_existent, \
            f"查詢不存在的指紋 '{non_existent_fingerprint}' 應返回 False，實際為 True。"
        print(f"  查詢不存在的指紋 '{non_existent_fingerprint}'：正確回報找不到 (False)。")

        print("✅ pipeline_metadata_manager 整合測試成功")

    except AssertionError as e:
        print(f"\n❌ 測試失敗: {e}", file=sys.stderr)
        exit_code = 1
    except Exception as e:
        print(f"\n❌ 測試執行期間發生未預期的錯誤: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        # 5. 清理測試環境
        print("\n[階段 5/5] 清理測試環境...")
        if conn_for_verification:
            try:
                conn_for_verification.close()
                print("  測試資料庫連線已關閉。")
            except Exception as e:
                print(f"  關閉資料庫連線時發生錯誤: {e}", file=sys.stderr)

        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                print(f"  臨時目錄 '{temp_dir}' 已成功刪除。")
            except Exception as e:
                print(f"  刪除臨時目錄 '{temp_dir}' 時發生錯誤: {e}", file=sys.stderr)

        print("🏁 整合測試執行完畢。")
        sys.exit(exit_code)

if __name__ == "__main__":
    # 確保使用的是 manager 模組內的 config (如果 manager.py 內部有用到 config 的話)
    # metadata_manager_module.config.DATABASE_FILENAME = "dummy_for_test_harness_run.db"
    # metadata_manager_module.config.PROCESSED_FILES_TABLE_NAME = TEST_TABLE_NAME
    # 上述設定方式是如果 manager.py 直接從 config 讀取全域變數。
    # 但我們已經將 table_name 傳入 MetadataManager 的 __init__，
    # 且 db_path 也透過傳入 connection 或 db_path 參數來控制，所以不太需要直接修改 config 模組。

    run_tests()
