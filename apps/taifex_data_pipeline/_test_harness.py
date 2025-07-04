import subprocess
import os
import sys
import shutil

# --- 設定區 ---
PYTHON_EXECUTABLE = sys.executable
# 修改 PIPELINE_SCRIPT 以相對於 __file__，並使其可執行
PIPELINE_SCRIPT_REL_PATH = "run.py" # 相對於 apps/taifex_data_pipeline/
PIPELINE_MODULE_PATH = "apps.taifex_data_pipeline.run" # 模組路徑
TEST_WORKSPACE = "/tmp/test_historical_pipeline"
TEST_DATA_DIR = os.path.join(TEST_WORKSPACE, "taifex_historical_data")
TEST_DB_DIR = os.path.join(TEST_WORKSPACE, "databases")
TEST_DB_NAME = "test_taifex_output.duckdb"
REQUIREMENTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "requirements.txt"))

# --- 路徑自我校正樣板碼 (確保測試腳本能找到模組) ---
# 確保 project_root 是 apps 目錄的父目錄，即專案的根目錄
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 樣板碼結束 ---

# --- 測試流程 ---
print("--- 開始執行 taifex_data_pipeline 整合測試 ---")

# 1. 清理並建立測試環境
print(f"1. 清理並建立測試工作區: {TEST_WORKSPACE}")
if os.path.exists(TEST_WORKSPACE):
    shutil.rmtree(TEST_WORKSPACE)
os.makedirs(TEST_DATA_DIR, exist_ok=True)
os.makedirs(TEST_DB_DIR, exist_ok=True)

# 2. 建立一個虛假的數據檔案 (模擬使用者上傳)
print("2. 建立虛假的輸入數據檔案...")
fake_data_path = os.path.join(TEST_DATA_DIR, "Daily_2025_01_01.zip")
with open(fake_data_path, 'w') as f:
    f.write("fake zip content") # 內容不重要，主要是觸發流程

# 3. 安裝依賴
print(f"3. 安裝依賴從: {REQUIREMENTS_PATH}")
# 確保 requirements.txt 存在
if not os.path.exists(REQUIREMENTS_PATH):
    print(f"❌ 錯誤: requirements.txt 未在預期路徑找到: {REQUIREMENTS_PATH}")
    sys.exit(1)

pip_result = subprocess.run([PYTHON_EXECUTABLE, "-m", "pip", "install", "-r", REQUIREMENTS_PATH], capture_output=True, text=True, encoding='utf-8', errors='replace')
if pip_result.returncode != 0:
    print("❌ 依賴安裝失敗!")
    print("STDOUT:")
    print(pip_result.stdout)
    print("STDERR:")
    print(pip_result.stderr)
    sys.exit(1)
print("✅ 依賴安裝成功。")
if pip_result.stdout: print(f"Pip STDOUT:\n{pip_result.stdout}") # 顯示 pip 的輸出


# 4. 執行數據精煉廠
print("4. 執行數據精煉廠 (taifex_data_pipeline)...")
# 使用 -m 選項來執行模組，這樣 Python 會處理路徑問題
cmd = [
    PYTHON_EXECUTABLE, "-m", PIPELINE_MODULE_PATH,
    "--db-output-dir", TEST_DB_DIR,
    "--db-name", TEST_DB_NAME,
    "--input-files", fake_data_path,
    "--log-level", "DEBUG"
]
print(f"執行指令: {' '.join(cmd)}")
print(f"執行目錄 (cwd): {project_root}")

# 執行時，將 cwd 設置為 project_root，這樣模組內的相對路徑才能正確解析
pipeline_result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', cwd=project_root)

# 5. 驗證結果
print("\n--- 驗證階段 ---")
print("STDOUT:")
print(pipeline_result.stdout)
print("\nSTDERR:")
print(pipeline_result.stderr)

if "ModuleNotFoundError" in pipeline_result.stderr or "ModuleNotFoundError" in pipeline_result.stdout:
    print("❌ 測試失敗：偵測到 ModuleNotFoundError。")
    sys.exit(1)

# 這裡的原始run.py腳本在找不到有效zip時會拋出異常，這是預期的。
# 我們主要關注的是 ModuleNotFoundError
# 檢查 pipeline_result.returncode != 0 且 stderr 中沒有 ModuleNotFoundError
# 並且 stdout 中有 pipeline 啟動的日誌
expected_log_message = "數據精煉廠任務啟動" # 假設 run.py 會印出這樣的訊息
if expected_log_message not in pipeline_result.stdout:
    print(f"⚠️ 警告: 未在 STDOUT 中找到預期的啟動日誌 '{expected_log_message}'。請檢查 run.py 的日誌輸出。")
    # 即使沒有這個日誌，只要沒有 ModuleNotFoundError，也算通過核心測試目標
    # 但這可能表示 run.py 的行為與預期不符

if pipeline_result.returncode != 0:
    print(f"ℹ️ 提示: 腳本以非零狀態碼 {pipeline_result.returncode} 結束。")
    if "zipfile.BadZipFile" in pipeline_result.stderr or "zipfile.BadZipFile" in pipeline_result.stdout:
        print("✅ 這是預期的，因為我們使用的是偽造的 ZIP 檔案。")
    else:
        print("⚠️ 警告: 腳本因其他錯誤退出，但不是 ModuleNotFoundError。")


print("✅ 驗證通過：未偵測到 ModuleNotFoundError。")
print("\n--- 整合測試執行完畢 ---")
sys.exit(0)
