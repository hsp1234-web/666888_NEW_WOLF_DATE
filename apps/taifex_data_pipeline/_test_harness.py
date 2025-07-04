import subprocess
import os
import sys
import shutil
import tempfile
import zipfile
import csv
import duckdb
from datetime import datetime

# --- 設定區 ---
PYTHON_EXECUTABLE = sys.executable
PIPELINE_MODULE_PATH = "apps.taifex_data_pipeline.run" # 執行 run.py 作為模組
# TEST_WORKSPACE = "/tmp/test_historical_pipeline_v32_2" # 使用固定路徑或臨時目錄
TEST_WORKSPACE = tempfile.mkdtemp(prefix="test_pipeline_") # 每次創建唯一的臨時目錄

TEST_DATA_DIR = os.path.join(TEST_WORKSPACE, "source_data")
TEST_DB_DIR = os.path.join(TEST_WORKSPACE, "databases")
TEST_DB_NAME = "test_output_v32_2.duckdb"
REQUIREMENTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "requirements.txt"))

# --- 路徑自我校正 (確保能找到 apps 模組) ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- 模擬數據生成 ---
def create_mock_csv_data(file_path: str, num_rows: int, is_option: bool = False):
    """創建一個模擬的選擇權或期貨CSV檔案"""
    headers_futures = ['交易日期', '契約', '到期月份(週別)', '開盤價', '最高價', '最低價', '收盤價', '成交量', '結算價', '未沖銷契約量', '交易時段']
    headers_options = ['交易日期', '契約', '到期月份(週別)', '履約價', '買賣權', '開盤價', '最高價', '最低價', '收盤價', '成交量', '結算價', '未沖銷契約量', '交易時段']

    headers = headers_options if is_option else headers_futures

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(num_rows):
            date_str = datetime(2024, 7, (i % 28) + 1).strftime('%Y/%m/%d')
            product_id = "TXO" if is_option else "TXF"
            expiry = "202407W4" if (i % 2 == 0) else "202408"

            row_data = [
                date_str,
                product_id,
                expiry,
            ]
            if is_option:
                row_data.extend([
                    str(18000 + i * 100), # 履約價
                    "Call" if i % 2 == 0 else "Put", # 買賣權
                ])

            row_data.extend([
                str(18000 + i), # 開盤價
                str(18050 + i), # 最高價
                str(17950 + i), # 最低價
                str(18020 + i), # 收盤價
                str(100 + i * 10), # 成交量
                str(18025 + i), # 結算價
                str(5000 + i * 50), # 未沖銷契約量
                "一般" # 交易時段
            ])
            writer.writerow(row_data)
    print(f"    模擬CSV檔案已創建: {file_path} ({num_rows} 行)")

def create_mock_zip_file(zip_path: str, csv_file_name_in_zip: str, num_rows: int, is_option: bool = False):
    """創建一個包含模擬CSV檔案的ZIP檔案"""
    # 在臨時位置創建CSV
    temp_csv_dir = os.path.join(os.path.dirname(zip_path), "temp_csv_for_zip")
    os.makedirs(temp_csv_dir, exist_ok=True)
    temp_csv_path = os.path.join(temp_csv_dir, csv_file_name_in_zip)

    create_mock_csv_data(temp_csv_path, num_rows, is_option)

    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(temp_csv_path, arcname=csv_file_name_in_zip)
    print(f"    模擬ZIP檔案已創建: {zip_path} (內含 {csv_file_name_in_zip}, {num_rows} 行)")
    shutil.rmtree(temp_csv_dir) # 清理臨時CSV

# --- 測試流程 ---
print(f"--- 開始執行 taifex_data_pipeline v32.2 整合測試 ---")
print(f"--- 測試工作區: {TEST_WORKSPACE} ---")

# 1. 清理並建立測試環境
print(f"\n1. 清理並建立測試工作區...")
if os.path.exists(TEST_DB_DIR): # 只清理 DB 目錄，避免 source_data 被重複創建的腳本意外刪除
    shutil.rmtree(TEST_DB_DIR)
os.makedirs(TEST_DATA_DIR, exist_ok=True)
os.makedirs(TEST_DB_DIR, exist_ok=True)
print(f"    測試資料目錄: {TEST_DATA_DIR}")
print(f"    測試資料庫目錄: {TEST_DB_DIR}")


# 2. 建立模擬數據檔案
print("\n2. 建立模擬輸入數據檔案...")
mock_csv_path1 = os.path.join(TEST_DATA_DIR, "futures_daily_20240701.csv")
mock_csv_path2 = os.path.join(TEST_DATA_DIR, "options_daily_20240702.csv")
mock_zip_path1 = os.path.join(TEST_DATA_DIR, "TXF_Daily_2024.zip") # 模擬期交所的命名
mock_zip_path2 = os.path.join(TEST_DATA_DIR, "TXO_Daily_2024.zip")

create_mock_csv_data(mock_csv_path1, num_rows=10, is_option=False)
create_mock_csv_data(mock_csv_path2, num_rows=15, is_option=True)
create_mock_zip_file(mock_zip_path1, "FuturesDaily_20240703.csv", num_rows=20, is_option=False)
create_mock_zip_file(mock_zip_path2, "OptionsDaily_20240704.csv", num_rows=25, is_option=True)

input_files_for_pipeline = [mock_csv_path1, mock_csv_path2, mock_zip_path1, mock_zip_path2]
expected_total_rows = 10 + 15 + 20 + 25

# 3. 安裝依賴 (如果需要，通常在 CI 環境中這步是分開的)
print(f"\n3. 檢查依賴文件: {REQUIREMENTS_PATH}")
if not os.path.exists(REQUIREMENTS_PATH):
    print(f"❌ 錯誤: requirements.txt 未在預期路徑找到: {REQUIREMENTS_PATH}")
    sys.exit(1)
# 實際安裝步驟通常在外部執行，這裡只做示意或確認
# print(f"   (跳過實際 pip install -r，假設環境已準備好或由 CI 處理)")
print(f"   執行 pip install -r {REQUIREMENTS_PATH}...")
pip_result = subprocess.run([PYTHON_EXECUTABLE, "-m", "pip", "install", "-r", REQUIREMENTS_PATH], capture_output=True, text=True, encoding='utf-8', errors='replace')
if pip_result.returncode != 0:
    print("❌ 依賴安裝失敗!")
    print("STDOUT:")
    print(pip_result.stdout)
    print("STDERR:")
    print(pip_result.stderr)
    # sys.exit(1) # 暫時不退出，允許在本地環境中即使有些警告也繼續，但在 CI 中應嚴格
    print("⚠️ 依賴安裝過程中有非零返回碼，但測試將繼續...")
else:
    print("✅ 依賴安裝成功 (或已滿足)。")
if pip_result.stdout: print(f"Pip STDOUT (部分):\n{pip_result.stdout[:500]}...")


# 4. 執行數據精煉廠
print("\n4. 執行數據精煉廠 (taifex_data_pipeline)...")
cmd = [
    PYTHON_EXECUTABLE, "-m", PIPELINE_MODULE_PATH,
    "--input-files", *input_files_for_pipeline,
    "--db-output-dir", TEST_DB_DIR,
    "--db-name", TEST_DB_NAME,
    "--log-level", "INFO" # 使用 INFO 級別以獲得更簡潔的日誌，DEBUG 用於詳細除錯
]
print(f"    執行指令: {' '.join(cmd)}")
print(f"    執行目錄 (cwd): {project_root}")

pipeline_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace', cwd=project_root)
pipeline_stdout, pipeline_stderr = pipeline_process.communicate()
return_code = pipeline_process.returncode

print("\n--- 管線執行日誌 ---")
print("STDOUT:")
print(pipeline_stdout)
print("\nSTDERR:")
print(pipeline_stderr)
print("--- 日誌結束 ---")

if return_code != 0:
    print(f"❌ 管線執行失敗，返回碼: {return_code}")
    if "ModuleNotFoundError" in pipeline_stderr or "ModuleNotFoundError" in pipeline_stdout:
        print("❌ 偵測到 ModuleNotFoundError。請檢查依賴。")
    # shutil.rmtree(TEST_WORKSPACE) # 執行失敗時保留工作區以便偵錯
    # print(f"測試工作區 {TEST_WORKSPACE} 已保留以供偵錯。")
    sys.exit(1)
print(f"✅ 管線執行成功，返回碼: {return_code}")

# 5. 驗證數據庫結果
print("\n5. 驗證數據庫結果...")
db_full_path = os.path.join(TEST_DB_DIR, TEST_DB_NAME)
if not os.path.exists(db_full_path):
    print(f"❌ 測試失敗：資料庫檔案未創建於 {db_full_path}")
    # shutil.rmtree(TEST_WORKSPACE)
    # print(f"測試工作區 {TEST_WORKSPACE} 已保留以供偵錯。")
    sys.exit(1)

try:
    con = duckdb.connect(database=db_full_path, read_only=True)

    # 檢查目標表格是否存在
    target_table_name = "raw_taifex_data"
    tables_result = con.execute("SHOW TABLES;").fetchall()
    available_tables = [table[0] for table in tables_result]
    print(f"    資料庫中可用的表格: {available_tables}")
    if target_table_name not in available_tables:
        print(f"❌ 測試失敗：目標表格 '{target_table_name}' 未在資料庫中找到。")
        con.close()
        # shutil.rmtree(TEST_WORKSPACE)
        # print(f"測試工作區 {TEST_WORKSPACE} 已保留以供偵錯。")
        sys.exit(1)
    print(f"✅ 目標表格 '{target_table_name}' 已找到。")

    # 檢查總行數
    count_result = con.execute(f"SELECT COUNT(*) FROM {target_table_name};").fetchone()
    if count_result is None:
        print(f"❌ 測試失敗：無法從 '{target_table_name}' 讀取行數。")
        con.close()
        # shutil.rmtree(TEST_WORKSPACE)
        # print(f"測試工作區 {TEST_WORKSPACE} 已保留以供偵錯。")
        sys.exit(1)

    actual_rows_in_db = count_result[0]
    print(f"    資料庫中 '{target_table_name}' 的實際行數: {actual_rows_in_db}")
    print(f"    預期總行數 (來自所有模擬檔案): {expected_total_rows}")

    if actual_rows_in_db == expected_total_rows:
        print(f"✅ 數據驗證成功：資料庫中的行數 ({actual_rows_in_db}) 與預期 ({expected_total_rows}) 相符！")
    else:
        print(f"❌ 數據驗證失敗：資料庫中的行數 ({actual_rows_in_db}) 與預期 ({expected_total_rows}) 不符。")
        # 顯示一些數據樣本以供偵錯
        sample_data = con.execute(f"SELECT source_file, COUNT(*) as count FROM {target_table_name} GROUP BY source_file;").fetchall()
        print(f"    按 source_file 分組的數據行數: {sample_data}")
        con.close()
        # shutil.rmtree(TEST_WORKSPACE)
        # print(f"測試工作區 {TEST_WORKSPACE} 已保留以供偵錯。")
        sys.exit(1)

    con.close()

except Exception as e:
    print(f"❌ 驗證數據庫時發生錯誤: {e}")
    # shutil.rmtree(TEST_WORKSPACE)
    # print(f"測試工作區 {TEST_WORKSPACE} 已保留以供偵錯。")
    sys.exit(1)

# 清理測試工作區
print(f"\n6. 清理測試工作區: {TEST_WORKSPACE}")
try:
    shutil.rmtree(TEST_WORKSPACE)
    print(f"    測試工作區已成功刪除。")
except Exception as e_clean:
    print(f"    ⚠️ 清理測試工作區時發生錯誤: {e_clean}")


print("\n--- 整合測試執行完畢 (v32.2) ---")
sys.exit(0)
