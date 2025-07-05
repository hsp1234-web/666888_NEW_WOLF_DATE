# apps/mission_runner/_test_run.py
import unittest
import subprocess
import json
import base64
import os
import sys

# 確保測試可以找到 mission_runner/run.py
# 假設 _test_run.py 和 run.py 在同一個 apps/mission_runner 目錄下
MISSION_RUNNER_SCRIPT = os.path.join(os.path.dirname(__file__), "run.py")

# 輔助函數：執行 mission_runner 並獲取結果
def run_mission_runner(params_dict):
    """
    執行 mission_runner 腳本並返回其 stdout, stderr 和返回碼。
    params_dict: 一個 Python 字典，將被轉換為 JSON 並 Base64 編碼。
    """
    try:
        params_json = json.dumps(params_dict)
        params_b64 = base64.b64encode(params_json.encode('utf-8')).decode('utf-8')

        cmd = [sys.executable, MISSION_RUNNER_SCRIPT, "--mission-params", params_b64]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(timeout=15) # 設定超時以防腳本卡住
        return_code = process.returncode

        return stdout, stderr, return_code
    except Exception as e:
        return "", f"測試執行本身發生錯誤: {e}", -99 # 特殊返回碼表示測試框架錯誤

class TestMissionRunner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # 創建/覆蓋虛假的下游腳本，用於測試 mission_runner 的參數傳遞
        # 這些腳本沒有外部依賴，只打印參數並退出

        # 獲取 apps 目錄的路徑
        # MISSION_RUNNER_SCRIPT = apps/mission_runner/run.py
        # current_dir = apps/mission_runner
        # apps_dir = apps
        current_dir = os.path.dirname(MISSION_RUNNER_SCRIPT)
        apps_dir = os.path.dirname(current_dir)

        fake_analyzer_script_path = os.path.join(apps_dir, "daily_market_analyzer", "run.py")
        fake_taifex_script_path = os.path.join(apps_dir, "taifex_data_pipeline", "run.py")

        os.makedirs(os.path.dirname(fake_analyzer_script_path), exist_ok=True)
        os.makedirs(os.path.dirname(fake_taifex_script_path), exist_ok=True)

        fake_analyzer_content = """
# apps/daily_market_analyzer/run.py (虛假腳本 FOR TESTING)
import argparse
import sys
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假市場分析器 FOR TESTING")
    parser.add_argument("--tickers", help="分析的股票代碼")
    parser.add_argument("--start-date", help="分析開始日期")
    parser.add_argument("--end-date", help="分析結束日期")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據")
    # 模擬接收任意其他參數
    parser.add_argument('--extra-args', nargs='*', help='其他可能的參數')

    args, unknown = parser.parse_known_args() # 使用 parse_known_args 以更具彈性

    output_data = {"script": "daily_market_analyzer", "args": vars(args)}
    print(json.dumps(output_data))
    sys.exit(0)
"""
        with open(fake_analyzer_script_path, "w", encoding="utf-8") as f:
            f.write(fake_analyzer_content)
        # print(f"DEBUG: Created fake analyzer at {fake_analyzer_script_path}")

        fake_taifex_content = """
# apps/taifex_data_pipeline/run.py (虛假腳本 FOR TESTING)
import argparse
import sys
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假台指期數據管線 FOR TESTING")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據")
    # 模擬接收任意其他參數
    parser.add_argument('--extra-args', nargs='*', help='其他可能的參數')

    args, unknown = parser.parse_known_args() # 使用 parse_known_args

    output_data = {"script": "taifex_data_pipeline", "args": vars(args)}
    print(json.dumps(output_data))
    sys.exit(0)
"""
        with open(fake_taifex_script_path, "w", encoding="utf-8") as f:
            f.write(fake_taifex_content)
        # print(f"DEBUG: Created fake taifex at {fake_taifex_script_path}")

        # 檢查 mission_runner.py 是否存在
        if not os.path.isfile(MISSION_RUNNER_SCRIPT):
            raise FileNotFoundError(f"找不到 mission_runner 腳本: {MISSION_RUNNER_SCRIPT}")
        # Python 腳本通常不需要執行權限，由解釋器執行


    def test_case_1_standard_analysis_flow(self):
        """
        測試案例 1：標準分析流程
        驗證 mission_runner 能夠正確調用 daily_market_analyzer/run.py，
        並傳遞了正確的 --tickers, --start-date 等參數。
        """
        params = {
            "REPOSITORY_URL": "git@test.com:repo.git",
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "標準分析流程",
            "ANALYSIS_TICKERS": "AAPL,MSFT",
            "ANALYSIS_START_DATE": "2023-01-01",
            "ANALYSIS_END_DATE": "2023-01-31",
            "FORCE_REPO_REFRESH": False,
            "FORCE_DATA_REFRESH": False,
            "LOGGING_MODE": "標準模式"
        }
        stdout, stderr, return_code = run_mission_runner(params)

        print(f"\n[測試案例 1 STDOUT]:\n{stdout}")
        print(f"[測試案例 1 STDERR]:\n{stderr}")
        self.assertEqual(return_code, 0, f"標準分析流程應成功執行。STDERR: {stderr}")

        try:
            # 假設 daily_market_analyzer/run.py 將其參數作為 JSON 打印到 stdout 的第一行
            # 且 mission_runner 本身不向 stdout 打印任何內容（所有日誌到 stderr）
            self.assertTrue(stdout, "stdout 不應為空，期望下游腳本的輸出。")
            downstream_output_str = stdout.splitlines()[0]
            downstream_output = json.loads(downstream_output_str)

            self.assertEqual(downstream_output.get("script"), "daily_market_analyzer")
            received_args = downstream_output.get("args", {})

            self.assertEqual(received_args.get("tickers"), params["ANALYSIS_TICKERS"])
            self.assertEqual(received_args.get("start_date"), params["ANALYSIS_START_DATE"])
            self.assertEqual(received_args.get("end_date"), params["ANALYSIS_END_DATE"])
            # FORCE_DATA_REFRESH 在此案例中為 False，所以下游不應收到 --force-data-refresh
            # 虛假腳本將 force_data_refresh 設置為 False (如果未提供該 flag) 或 True (如果提供了)
            self.assertEqual(received_args.get("force_data_refresh"), False)

        except (json.JSONDecodeError, IndexError) as e:
            self.fail(f"解析下游腳本輸出失敗: {e}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
        except KeyError as e:
            self.fail(f"下游腳本輸出中缺少預期的鍵: {e}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")

    def test_case_2_elt_load_flow(self):
        """
        測試案例 2：ELT 載入流程
        驗證 mission_runner 能夠正確調用 taifex_data_pipeline/run.py。
        """
        params = {
            "REPOSITORY_URL": "git@test.com:repo.git",
            "TARGET_BRANCH": "develop",
            "EXECUTION_MODE": "ELT第一階段：載入",
            "FORCE_DATA_REFRESH": False
            # 其他 ELT 特定參數可根據需要添加
        }
        stdout, stderr, return_code = run_mission_runner(params)

        print(f"\n[測試案例 2 STDOUT]:\n{stdout}")
        print(f"[測試案例 2 STDERR]:\n{stderr}")
        self.assertEqual(return_code, 0, f"ELT 載入流程應成功執行。STDERR: {stderr}")

        try:
            self.assertTrue(stdout, "stdout 不應為空，期望下游腳本的輸出。")
            downstream_output_str = stdout.splitlines()[0]
            downstream_output = json.loads(downstream_output_str)

            self.assertEqual(downstream_output.get("script"), "taifex_data_pipeline")
            received_args = downstream_output.get("args", {})

            # 在此測試案例中，FORCE_DATA_REFRESH 傳入的是 False
            # 虛假腳本 taifex_data_pipeline/run.py 應將 force_data_refresh 記錄為 False
            self.assertEqual(received_args.get("force_data_refresh"), False,
                             "FORCE_DATA_REFRESH 應為 False 或未在下游腳本參數中設置為 True")

        except (json.JSONDecodeError, IndexError) as e:
            self.fail(f"解析下游腳本輸出失敗: {e}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
        except KeyError as e:
            self.fail(f"下游腳本輸出中缺少預期的鍵: {e}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")

    def test_case_3_force_refresh_flag(self):
        """
        測試案例 3：強制刷新功能
        驗證 mission_runner 在調用下游服務時，正確附加了 --force-data-refresh 旗標。
        這裡我們將針對「標準分析流程」進行測試，並檢查其輸出。
        """
        params = {
            "REPOSITORY_URL": "git@test.com:repo.git",
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "標準分析流程",
            "ANALYSIS_TICKERS": "GOOG",
            "ANALYSIS_START_DATE": "2023-02-01",
            "ANALYSIS_END_DATE": "2023-02-10",
            "FORCE_DATA_REFRESH": True # 關鍵測試點
        }
        stdout, stderr, return_code = run_mission_runner(params)

        print(f"\n[測試案例 3 STDOUT]:\n{stdout}")
        print(f"[測試案例 3 STDERR]:\n{stderr}")
        self.assertEqual(return_code, 0, f"強制刷新流程應成功執行。STDERR: {stderr}")

        try:
            self.assertTrue(stdout, "stdout 不應為空，期望下游腳本的輸出。")
            downstream_output_str = stdout.splitlines()[0]
            downstream_output = json.loads(downstream_output_str)

            self.assertEqual(downstream_output.get("script"), "daily_market_analyzer")
            received_args = downstream_output.get("args", {})

            # 關鍵驗證：FORCE_DATA_REFRESH 在此案例中為 True
            # 虛假腳本 daily_market_analyzer/run.py 應將 force_data_refresh 記錄為 True
            self.assertEqual(received_args.get("force_data_refresh"), True,
                             "下游腳本接收到的 force_data_refresh 參數應為 True")

        except (json.JSONDecodeError, IndexError) as e:
            self.fail(f"解析下游腳本輸出失敗: {e}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
        except KeyError as e:
            self.fail(f"下游腳本輸出中缺少預期的鍵: {e}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")

    def test_case_4_invalid_execution_mode(self):
        """
        測試案例 4：無效參數處理
        模擬前端傳入一個包含未知 EXECUTION_MODE 的指令包。
        驗證 mission_runner 能夠優雅地失敗，返回非零退出碼，並打印出清晰的錯誤訊息。
        """
        params = {
            "REPOSITORY_URL": "git@test.com:repo.git",
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "不存在的模式ABC",
        }
        stdout, stderr, return_code = run_mission_runner(params)

        print(f"\n[測試案例 4 STDOUT]:\n{stdout}") # 預期為空
        print(f"[測試案例 4 STDERR]:\n{stderr}") # 預期包含錯誤訊息
        self.assertNotEqual(return_code, 0, "未知 EXECUTION_MODE 時應返回非零退出碼。")
        self.assertTrue(stderr, "stderr 不應為空，期望有錯誤訊息。")
        self.assertIn("[MISSION_RUNNER_ERROR] 未知的執行模式: 不存在的模式ABC", stderr, "錯誤訊息未包含預期的內容。")
        self.assertEqual(stdout, "", "stdout 應為空，因為不應執行任何下游腳本。")

    def test_missing_required_params(self):
        """
        額外測試：測試缺少必要參數 (例如 REPOSITORY_URL) 的情況
        """
        params = {
            # "REPOSITORY_URL": "git@test.com:repo.git", # 故意遺漏
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "標準分析流程",
        }
        stdout, stderr, return_code = run_mission_runner(params)
        self.assertNotEqual(return_code, 0, "缺少 REPOSITORY_URL 時應返回非零退出碼。")
        self.assertIn("[MISSION_RUNNER_ERROR] 必要參數 'REPOSITORY_URL' 未提供或為空。", stderr)

        params_empty_repo_url = {
            "REPOSITORY_URL": "",
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "標準分析流程",
        }
        stdout, stderr, return_code = run_mission_runner(params_empty_repo_url)
        self.assertNotEqual(return_code, 0, "REPOSITORY_URL 為空時應返回非零退出碼。")
        self.assertIn("[MISSION_RUNNER_ERROR] 必要參數 'REPOSITORY_URL' 未提供或為空。", stderr)

    def test_default_analysis_tickers(self):
        """
        額外測試：測試 ANALYSIS_TICKERS 未提供時，是否使用預設值。
        """
        params = {
            "REPOSITORY_URL": "git@test.com:repo.git",
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "標準分析流程",
            # "ANALYSIS_TICKERS": "", # 故意不提供或提供空值
            "ANALYSIS_START_DATE": "2023-01-01",
            "ANALYSIS_END_DATE": "2023-01-31",
        }
        stdout, stderr, return_code = run_mission_runner(params)
        self.assertEqual(return_code, 0, f"使用預設 Tickers 時應成功。STDERR: {stderr}")
        self.assertIn("[MISSION_RUNNER_INFO] 'ANALYSIS_TICKERS' 未提供或為空，使用預設值: NQ=F,ES=F,^VIX", stderr)

        # 驗證下游腳本是否收到了預設的 tickers
        # 這需要下游腳本將其收到的參數打印到 stdout，以便此處解析
        try:
            # 假設下游腳本的第一行 stdout 是 JSON 字串
            # 注意：如果 mission_runner 本身也有 stdout 輸出，需要調整解析方式
            # 目前 mission_runner 的 INFO/ERROR 訊息都打到 stderr
            if stdout:
                # 假設下游腳本的輸出是單行的 JSON
                # 如果有多行，或混合了其他非 JSON 輸出，這裡需要調整
                first_line_stdout = stdout.splitlines()[0] if stdout.splitlines() else ""
                downstream_output = json.loads(first_line_stdout)
                self.assertEqual(downstream_output.get("args", {}).get("tickers"), "NQ=F,ES=F,^VIX")
            else:
                self.fail("下游腳本沒有任何 stdout 輸出，無法驗證 tickers。")
        except (json.JSONDecodeError, IndexError) as e:
            self.fail(f"解析下游腳本輸出失敗: {e}\nSTDOUT:\n{stdout}")


if __name__ == "__main__":
    unittest.main()
