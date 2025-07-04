# -*- coding: utf-8 -*-
# 終極整合測試腳本 v35.2 - 「創世紀」全鏈路演習
import os
import sys
import subprocess
import shutil
import zipfile
import duckdb
import pandas as pd
from datetime import date

# --- 測試配置 ---
TEST_WORKSPACE = "/tmp/test_harness_v35_genesis"
RAW_FILES_DIR = os.path.join(TEST_WORKSPACE, "raw_files_for_elt")
DB_DIR = os.path.join(TEST_WORKSPACE, "databases_elt")
METADATA_DB_PATH = os.path.join(DB_DIR, "pipeline_metadata.duckdb")
RAW_DB_PATH = os.path.join(DB_DIR, "raw_taifex.duckdb")
ANALYTICS_DB_PATH = os.path.join(DB_DIR, "taifex_historical.duckdb")

# --- 自動尋找專案腳本路徑 ---
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    METADATA_MANAGER_SCRIPT = os.path.join(project_root, "apps", "pipeline_metadata_manager", "manager.py")
    LOADER_SCRIPT = os.path.join(project_root, "apps", "taifex_data_pipeline", "run.py")
    TRANSFORMER_SCRIPT = os.path.join(project_root, "apps", "taifex_data_transformer", "run.py")
except Exception as e:
    print(f"路徑設定錯誤: {e}")
    sys.exit(1)


def print_header(title):
    print("\n" + "="*80)
    print(f"⚔️  {title}")
    print("="*80)

def run_script(script_path, args, title):
    """執行指定的後端腳本"""
    print_header(title)
    cmd = [sys.executable, script_path] + args
    print(f"🚀 執行指令: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    print("--- STDOUT ---")
    print(result.stdout)
    if result.stderr:
        print("--- STDERR ---")
        print(result.stderr)
    assert result.returncode == 0, f"{title} 執行失敗！"
    print(f"✅ {title} 執行成功。")
    return result

def verify_db_state(db_path, table, expected_rows, description):
    """驗證資料庫狀態"""
    print(f"🔍 {description}")
    if not os.path.exists(db_path):
        print(f"❌ 驗證失敗: 資料庫檔案不存在 {db_path}")
        return False

    conn = duckdb.connect(db_path, read_only=True)
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count == expected_rows:
            print(f"✅ 驗證成功: 表格 '{table}' 中有 {count} 筆記錄，符合預期。")
            return True
        else:
            print(f"❌ 驗證失敗: 表格 '{table}' 中有 {count} 筆記錄，預期為 {expected_rows}。")
            print(conn.execute(f"SELECT * FROM {table}").fetchdf())
            return False
    except Exception as e:
        print(f"❌ 驗證失敗: 查詢資料庫時發生錯誤: {e}")
        return False
    finally:
        conn.close()

def main():
    try:
        # 階段 0 & 1: 建立環境與作戰資源
        print_header("階段 1: 建立演習戰場與作戰資源")
        if os.path.exists(TEST_WORKSPACE):
            shutil.rmtree(TEST_WORKSPACE)
        os.makedirs(RAW_FILES_DIR, exist_ok=True)
        os.makedirs(DB_DIR, exist_ok=True)

        zip_path_A = os.path.join(RAW_FILES_DIR, "Options_20250102.zip")
        with zipfile.ZipFile(zip_path_A, 'w') as zf:
            csv_content = (
                "交易日期,契約代碼,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,,,,,,交易時段\n"
                "2025/01/02,TXO,202501W1,18000.0,Call,150.5,160,140,155,1025,155,500,,,,,,一般\n"
                "2025/01/02,TXO,202501W1,18100.0,Put,80,90.5,75,88,\"2,510\",88,\"800\",,,,,一般\n"
            ).encode('big5', errors='ignore')
            zf.writestr("Daily_2025_01_02.csv", csv_content)

        csv_path_B = os.path.join(RAW_FILES_DIR, "Futures_20250103.csv")
        with open(csv_path_B, 'w', encoding='ms950') as f:
            f.write("交易日期,契約代碼,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,交易時段\n")
            f.write("2025/01/03,MTX,202501,,期貨,17500,17510,17450,17480,\"3,000\",17480,\"1,500\",一般\n")
        print(f"✅ 戰場 '{TEST_WORKSPACE}' 建立完畢。")
        all_files = [zip_path_A, csv_path_B]

        # 階段 2: 首次執行高速載入器
        loader_args = [
            "--input-files"] + all_files + [
            "--db-output-dir", DB_DIR,
            "--metadata-db-path", METADATA_DB_PATH
        ]
        run_script(LOADER_SCRIPT, loader_args, "階段 2: 首次火力偵察 (執行高速載入器)")
        assert verify_db_state(METADATA_DB_PATH, "processed_files", 2, "驗證「作戰日誌」記錄了 2 個檔案")
        assert verify_db_state(RAW_DB_PATH, "raw_import_log", 2, "驗證「原始數據艙」包含了 2 筆原始數據")

        # 階段 3: 重複執行驗證
        run_script(LOADER_SCRIPT, loader_args, "階段 3: 反覆火力偵察 (驗證重複處理)")
        assert verify_db_state(METADATA_DB_PATH, "processed_files", 2, "驗證「作戰日誌」總筆數仍為 2")
        assert verify_db_state(RAW_DB_PATH, "raw_import_log", 2, "驗證「原始數據艙」總筆數仍為 2")

        # 階段 4: 執行數據轉換器
        transformer_args = [
            "--raw-db-path", RAW_DB_PATH,
            "--analytics-db-path", ANALYTICS_DB_PATH
        ]
        run_script(TRANSFORMER_SCRIPT, transformer_args, "階段 4: 發起總攻 (執行數據轉換器)")

        # 階段 5: 最終驗證
        print_header("階段 5: 最終戰果驗證")
        final_success = verify_db_state(ANALYTICS_DB_PATH, "daily_ohlc", 3, "驗證「分析數據庫」最終數據")

        if final_success:
            print("\n" + "#"*40)
            print("✅  v35.2「創世紀」全鏈路整合演習成功！")
            print("#"*40)
        else:
            print("\n" + "!"*40)
            print("❌  v35.2「創世紀」全鏈路整合演習失敗！")
            print("!"*40)
            sys.exit(1)

    finally:
        # 階段 6: 清理戰場
        print_header("階段 6: 清理演習戰場")
        if os.path.exists(TEST_WORKSPACE):
            shutil.rmtree(TEST_WORKSPACE)
            print(f"✅ 測試工作區 '{TEST_WORKSPACE}' 已成功刪除。")

if __name__ == "__main__":
    main()
