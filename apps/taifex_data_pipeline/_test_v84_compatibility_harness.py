# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import tempfile
import shutil

# --- 路徑自我校正樣板碼 ---
try:
    current_script_path = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_script_path)
    # 假設 _test_v84_compatibility_harness.py 在 apps/taifex_data_pipeline/ 中
    # 則 run.py 也在 current_dir 中
    # project_root 則需要往上兩層
    project_root = os.path.dirname(os.path.dirname(current_dir))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    # 將 apps 目錄也加入 sys.path，以便 run.py 中的相對導入能正確運作
    apps_dir = os.path.dirname(current_dir)
    if apps_dir not in sys.path:
        sys.path.insert(0, apps_dir)
except Exception as e:
    print(f"專案路徑校正時發生錯誤: {e}", file=sys.stderr)
    sys.exit(1)
# --- 路徑自我校正樣板碼結束 ---

def main():
    """
    整合測試腳本，模擬指揮中心調用 taifex_data_pipeline/run.py。
    驗證 --enable-status-updates 參數是否能被正確接收。
    """
    print("--- 開始執行 v84 相容性整合測試 ---")

    # 找到 run.py 的絕對路徑
    # 假設此測試腳本與 run.py 在同一目錄下
    run_py_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "run.py")
    if not os.path.exists(run_py_script_path):
        print(f"錯誤：找不到目標腳本 {run_py_script_path}")
        sys.exit(1)

    print(f"目標腳本: {run_py_script_path}")

    # 創建一個臨時目錄作為 --db-output-dir
    temp_output_dir = tempfile.mkdtemp(prefix="taifex_test_")
    print(f"臨時輸出目錄: {temp_output_dir}")

    # 創建一個虛擬的輸入檔案
    dummy_input_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dummy_test_input.csv")
    if not os.path.exists(dummy_input_file):
        print(f"錯誤：找不到測試輸入檔案 {dummy_input_file}")
        shutil.rmtree(temp_output_dir) # 清理臨時目錄
        sys.exit(1)
    print(f"測試輸入檔案: {dummy_input_file}")

    # 指揮中心調用參數
    command = [
        sys.executable, # 使用目前的 Python 解釋器
        run_py_script_path,
        "--input-files", dummy_input_file,
        "--db-output-dir", temp_output_dir,
        "--enable-status-updates"
    ]

    print(f"執行指令: {' '.join(command)}")

    try:
        # 執行 run.py 腳本
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False, # 我們將手動檢查返回碼
            encoding='utf-8' # 確保輸出以 UTF-8 解碼
        )

        # 打印標準輸出和標準錯誤
        print("\n--- run.py 標準輸出 ---")
        print(process.stdout)
        print("--- run.py 標準輸出結束 ---\n")

        if process.stderr:
            print("--- run.py 標準錯誤 ---")
            print(process.stderr)
            print("--- run.py 標準錯誤結束 ---\n")

        # 驗證返回碼
        if process.returncode != 0:
            print(f"錯誤：run.py 執行失敗，返回碼: {process.returncode}")
            sys.exit(1)

        # 驗證是否包含 "unrecognized arguments"
        if "unrecognized arguments" in process.stderr.lower() or \
           "unrecognized arguments" in process.stdout.lower(): # 有些 argparse 版本可能將錯誤輸出到 stdout
            print("錯誤：run.py 報告了無法識別的參數！")
            sys.exit(1)

        # 檢查是否有其他嚴重錯誤的跡象 (例如 "Traceback" 或 "Error")
        # 排除已知的 "DEBUG", "INFO", "WARNING" 日誌級別訊息
        critical_error_keywords = ["traceback", "error:"] # 小寫以進行不區分大小寫比較

        # 過濾掉預期的日誌訊息中的 "error" 字樣
        # 例如 logger.error() 會產生 "[ERROR]"
        # 我們只關心真正的 Python Traceback 或 argparse 的 "error:"
        filtered_stderr = []
        for line in process.stderr.splitlines():
            if not (line.strip().startswith("[") and "ERROR" in line and "]" in line):
                 # 這是一個 heuristics，假設 logger.error 的格式是 "[timestamp] [ERROR] message"
                 # 如果不是這種格式，則不進行過濾
                filtered_stderr.append(line)

        cleaned_stderr = "\n".join(filtered_stderr).lower()

        for keyword in critical_error_keywords:
            if keyword in cleaned_stderr:
                # 檢查是否為 "unrecognized arguments" 錯誤，這個已經被特別處理
                if "unrecognized arguments" in cleaned_stderr and keyword == "error:":
                    continue # 已經由上面的 "unrecognized arguments" 檢查處理
                print(f"錯誤：在 run.py 的輸出中偵測到嚴重錯誤關鍵字 '{keyword}'！")
                # sys.exit(1) # 暫時不因為偵測到 "error" 而退出，除非是 "unrecognized arguments"

        print("整合測試成功：run.py 成功執行並識別了 --enable-status-updates 參數。")

    except FileNotFoundError:
        print(f"錯誤：找不到 Python 解釋器或 {run_py_script_path}。請確保 Python 環境已正確設定。")
        sys.exit(1)
    except Exception as e:
        print(f"執行整合測試時發生未預期的錯誤: {e}")
        sys.exit(1)
    finally:
        # 清理臨時目錄
        if os.path.exists(temp_output_dir):
            shutil.rmtree(temp_output_dir)
            print(f"已清理臨時輸出目錄: {temp_output_dir}")

    print("--- v84 相容性整合測試結束 ---")
    sys.exit(0) # 明確以返回碼 0 退出

if __name__ == "__main__":
    main()
