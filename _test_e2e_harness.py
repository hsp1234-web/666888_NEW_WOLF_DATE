import unittest
import os
import shutil
import tempfile
import subprocess
import json
import base64
import sys
import sqlite3
# import duckdb # 先註解掉，等實際用到時再引入，避免不必要的依賴檢查

# 專案根目錄，假設 _test_e2e_harness.py 位於根目錄
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
APPS_DIR = os.path.join(PROJECT_ROOT, "apps")
MISSION_RUNNER_SCRIPT = os.path.join(APPS_DIR, "mission_runner", "run.py")
ACTION_HANDLER_SCRIPT = os.path.join(APPS_DIR, "action_handler", "run.py")

class TestE2EHarness(unittest.TestCase):

    def setUp(self):
        """在每次測試前執行，創建臨時工作區。"""
        #創建一個唯一的臨時目錄
        self.workspace_dir = tempfile.mkdtemp(prefix="e2e_test_ws_")
        print(f"\n[E2E Setup] Created workspace: {self.workspace_dir}")

        # 定義工作區內的標準路徑 (這些是預期下游腳本會使用的路徑)
        # 下游腳本需要被配置為使用這些路徑
        self.db_path_yfinance_cache = os.path.join(self.workspace_dir, "yfinance_cache.duckdb")
        self.db_path_logs = os.path.join(self.workspace_dir, "logs.sqlite")
        self.db_path_raw_taifex = os.path.join(self.workspace_dir, "raw_taifex.duckdb")
        self.db_path_taifex_historical = os.path.join(self.workspace_dir, "taifex_historical.duckdb")
        self.reports_dir = os.path.join(self.workspace_dir, "reports")
        self.snapshot_output_dir = self.workspace_dir # 日誌快照直接輸出到工作區根目錄

        os.makedirs(self.reports_dir, exist_ok=True)

        # 為了讓虛假腳本能夠被 mission_runner 找到，我們需要確保它們存在
        # 這裡我們假設 apps/mission_runner/_test_run.py 中的 setUpClass 已經處理了
        # daily_market_analyzer, taifex_data_pipeline, fake_streaming_script 的創建。
        # 如果 _test_e2e_harness.py 要獨立運行，則需要在此處複製或調用該創建邏輯。
        # 目前，我假設 _test_run.py 中的虛假腳本是可用的。
        # 後續步驟會明確如何配置這些虛假腳本使用 self.workspace_dir 中的路徑。

    def tearDown(self):
        """在每次測試後執行，刪除臨時工作區。"""
        if os.path.exists(self.workspace_dir):
            shutil.rmtree(self.workspace_dir)
            print(f"\n[E2E Teardown] Removed workspace: {self.workspace_dir}")

    def _encode_mission_params(self, params_dict):
        """將 Python 字典編碼為 Base64 字串。"""
        params_json = json.dumps(params_dict)
        return base64.b64encode(params_json.encode('utf-8')).decode('utf-8')

    def _run_script(self, script_path, args_list, expected_return_code=0, timeout=60):
        """通用腳本執行輔助函數。"""
        cmd = [sys.executable, script_path] + args_list
        print(f"\n[E2E Test] Executing: {' '.join(cmd)}")
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, # 分別捕獲 stdout 和 stderr
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            stdout, stderr = process.communicate(timeout=timeout)
            return_code = process.returncode

            if return_code != expected_return_code:
                print(f"[E2E Test] STDOUT:\n{stdout}")
                print(f"[E2E Test] STDERR:\n{stderr}")
            self.assertEqual(return_code, expected_return_code,
                             f"腳本 {os.path.basename(script_path)} 執行失敗或返回碼不匹配。"
                             f"\n預期返回碼: {expected_return_code}, 實際: {return_code}"
                             f"\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return stdout, stderr, return_code
        except subprocess.TimeoutExpired:
            self.fail(f"腳本 {os.path.basename(script_path)} 執行超時 ({timeout}秒)。")
        except Exception as e:
            self.fail(f"執行腳本 {os.path.basename(script_path)} 時發生未預期錯誤: {e}")


    def _run_mission_runner(self, mission_params_dict, expected_return_code=0, timeout=60):
        """執行 mission_runner 並返回其 stdout, stderr 和返回碼。"""
        # 確保 mission_params_dict 包含必要的 REPOSITORY_URL 和 TARGET_BRANCH
        mission_params_dict.setdefault("REPOSITORY_URL", "fake_repo_url")
        mission_params_dict.setdefault("TARGET_BRANCH", "fake_branch")

        # 為了讓下游腳本使用 workspace 中的路徑，我們需要將這些路徑加入 mission_params
        # 這假設 mission_runner 和下游腳本會識別並使用這些特定命名的參數
        mission_params_dict["DB_PATH_LOGS"] = self.db_path_logs # 用於日誌記錄
        mission_params_dict["DB_PATH_YFINANCE_CACHE"] = self.db_path_yfinance_cache # 用於標準分析
        mission_params_dict["DB_PATH_RAW_TAIFEX"] = self.db_path_raw_taifex # 用於 ELT 載入
        mission_params_dict["DB_PATH_TAIFEX_HISTORICAL"] = self.db_path_taifex_historical # 用於 ELT 轉換
        mission_params_dict["REPORTS_DIR"] = self.reports_dir # 用於報告生成 (如果有的話)
        # 其他特定於流程的資料庫路徑也應在此處添加，如果 mission_params 是它們的唯一來源

        params_b64 = self._encode_mission_params(mission_params_dict)
        args = ["--mission-params", params_b64]
        return self._run_script(MISSION_RUNNER_SCRIPT, args, expected_return_code, timeout)

    def _run_action_handler(self, action_args_list, expected_return_code=0, timeout=30):
        """執行 action_handler 並返回其 stdout, stderr 和返回碼。"""
        # action_args_list 應該是像 ["--action=create_log_snapshot", "--log-db-path=...", ...]
        return self._run_script(ACTION_HANDLER_SCRIPT, action_args_list, expected_return_code, timeout)

    # --- 佔位符測試方法 ---
    def test_placeholder_e2e_test(self):
        """一個佔位符測試，確保框架基本運作。"""
        print(f"\n[E2E Test] Workspace for placeholder test: {self.workspace_dir}")
        # 這裡可以執行一個非常簡單的 mission_runner 命令，例如無效模式，預期失敗
        # 或者一個成功的串流測試，只檢查返回碼
        test_params = {
            "EXECUTION_MODE": "串流測試模式", # 假設這個模式存在且其虛假腳本無害
            "STREAMING_TEST_MESSAGE_COUNT": 1,
            "STREAMING_TEST_EXIT_CODE": 0
        }
        stdout, stderr, code = self._run_mission_runner(test_params, expected_return_code=0)
        self.assertIn("串流訊息 #1", stdout) # stdout 來自 mission_runner -> fake_streaming_script
        # stderr 可能包含 mission_runner 的 INFO/DEBUG 訊息
        print(f"[E2E Test] Placeholder test stdout: {stdout.strip()}")
        print(f"[E2E Test] Placeholder test stderr: {stderr.strip()}")
        print("[E2E Test] Placeholder test completed.")

    def test_standard_analysis_flow(self):
        """
        測試標準分析流程：
        - mission_runner 返回碼為 0。
        - 在指定的 yfinance_cache 路徑下有模擬數據寫入。
        - 在指定的 logs 路徑下有模擬日誌條目。
        """
        print("[E2E Test] Running test_standard_analysis_flow")
        tickers = "NQ=F,ES=F"
        start_date = "2024-01-01"
        end_date = "2024-01-05"

        mission_params = {
            "EXECUTION_MODE": "標準分析流程",
            "ANALYSIS_TICKERS": tickers,
            "ANALYSIS_START_DATE": start_date,
            "ANALYSIS_END_DATE": end_date,
            # DB_PATH_LOGS 和 DB_PATH_YFINANCE_CACHE 會由 _run_mission_runner 自動注入
        }

        # Act
        stdout, stderr, return_code = self._run_mission_runner(mission_params, expected_return_code=0)

        # Assert
        # 1. 驗證 yfinance_cache 檔案內容
        self.assertTrue(os.path.exists(self.db_path_yfinance_cache), "yfinance_cache 檔案未創建")
        with open(self.db_path_yfinance_cache, "r", encoding="utf-8") as f_cache:
            cache_content = f_cache.read()

        expected_cache_snippet = f"YFINANCE_CACHE_DUMMY_DATA: For {tickers}, data from {start_date} to {end_date} written."
        self.assertIn(expected_cache_snippet, cache_content, "yfinance_cache 內容不符合預期")
        print(f"[E2E Assert] Verified yfinance_cache content at: {self.db_path_yfinance_cache}")

        # 2. 驗證 logs 檔案內容
        self.assertTrue(os.path.exists(self.db_path_logs), "日誌檔案未創建")
        with open(self.db_path_logs, "r", encoding="utf-8") as f_logs:
            logs_content = f_logs.read()

        expected_log_snippet = f"DAILY_MARKET_ANALYZER_LOG: Tickers {tickers} processed. Log DB: {self.db_path_logs}. Success."
        self.assertIn(expected_log_snippet, logs_content, "日誌檔案內容不符合預期")
        print(f"[E2E Assert] Verified logs content at: {self.db_path_logs}")

        # 3. 驗證 stdout 是否包含虛假腳本的打印訊息 (可選，但有助於調試)
        self.assertIn(f"[FAKE ANALYZER] Logged to {self.db_path_logs}", stdout)
        self.assertIn(f"[FAKE ANALYZER] Wrote to cache {self.db_path_yfinance_cache}", stdout)
        print("[E2E Test] test_standard_analysis_flow completed successfully.")

    def test_elt_load_and_transform_flow(self):
        """
        測試 ELT 載入和轉換流程：
        - 依次執行 load 和 transform 步驟，返回碼均為 0。
        - 驗證 raw_taifex 和 taifex_historical 檔案被創建且內容符合預期。
        """
        print("[E2E Test] Running test_elt_load_and_transform_flow")

        # Arrange
        # 1. 準備虛假原始數據檔案
        fake_raw_data_filename = "fake_taifex_daily_data.csv"
        self.fake_raw_data_filepath = os.path.join(self.workspace_dir, fake_raw_data_filename)
        with open(self.fake_raw_data_filepath, "w", encoding="utf-8") as f_raw:
            f_raw.write("date,contract,open,high,low,close,volume\n")
            f_raw.write("20240101,TXF,18000,18050,17950,18020,1000\n")
        print(f"[E2E Arrange] Created fake raw data file: {self.fake_raw_data_filepath}")

        # 2. 準備 "ELT 載入" 指令包
        load_params = {
            "EXECUTION_MODE": "ELT第一階段：載入", # 與 mission_runner.py 中的 EXECUTION_MODE 匹配
            "ELT_PIPELINE_STEP": "load",
            "ELT_INPUT_FILE_PATH": self.fake_raw_data_filepath,
            # DB_PATH_RAW_TAIFEX 和 DB_PATH_LOGS 會由 _run_mission_runner 自動注入
        }

        # Act - Load step
        print("[E2E Act] Executing ELT Load step...")
        load_stdout, load_stderr, load_return_code = self._run_mission_runner(load_params, expected_return_code=0)

        # Assert - Load step
        self.assertTrue(os.path.exists(self.db_path_raw_taifex), "raw_taifex 檔案未在 load 步驟後創建")
        with open(self.db_path_raw_taifex, "r", encoding="utf-8") as f_raw_db:
            raw_db_content = f_raw_db.read()
        expected_raw_db_snippet = f"RAW_TAIFEX_DATA: Loaded from {self.fake_raw_data_filepath}"
        self.assertIn(expected_raw_db_snippet, raw_db_content, "raw_taifex 檔案內容不符合預期")
        self.assertIn(f"[FAKE TAIFEX PIPELINE] Loaded data from {self.fake_raw_data_filepath} to {self.db_path_raw_taifex}", load_stdout)
        print(f"[E2E Assert] Verified raw_taifex content at: {self.db_path_raw_taifex}")

        # 3. 準備 "ELT 轉換" 指令包
        transform_params = {
            "EXECUTION_MODE": "ELT第一階段：載入", # 假設轉換也用同一個 EXECUTION_MODE，通過 pipeline_step 區分
            "ELT_PIPELINE_STEP": "transform",
            # DB_PATH_RAW_TAIFEX, DB_PATH_TAIFEX_HISTORICAL, DB_PATH_LOGS 會自動注入
        }

        # Act - Transform step
        print("[E2E Act] Executing ELT Transform step...")
        transform_stdout, transform_stderr, transform_return_code = self._run_mission_runner(transform_params, expected_return_code=0)

        # Assert - Transform step
        self.assertTrue(os.path.exists(self.db_path_taifex_historical), "taifex_historical 檔案未在 transform 步驟後創建")
        with open(self.db_path_taifex_historical, "r", encoding="utf-8") as f_hist_db:
            hist_db_content = f_hist_db.read()
        expected_hist_db_snippet = f"HISTORICAL_TAIFEX_DATA: Transformed from {self.db_path_raw_taifex}"
        self.assertIn(expected_hist_db_snippet, hist_db_content, "taifex_historical 檔案內容不符合預期")
        self.assertIn(f"[FAKE TAIFEX PIPELINE] Transformed data from {self.db_path_raw_taifex} to {self.db_path_taifex_historical}", transform_stdout)
        print(f"[E2E Assert] Verified taifex_historical content at: {self.db_path_taifex_historical}")

        # Assert - Logs for both steps
        self.assertTrue(os.path.exists(self.db_path_logs), "日誌檔案未創建 (ELT)")
        with open(self.db_path_logs, "r", encoding="utf-8") as f_logs:
            logs_content = f_logs.read()
        self.assertIn(f"TAIFEX_PIPELINE_LOG: Step load. Log DB: {self.db_path_logs}", logs_content)
        self.assertIn(f"TAIFEX_PIPELINE_LOG: Step transform. Log DB: {self.db_path_logs}", logs_content)
        print(f"[E2E Assert] Verified ELT logs content at: {self.db_path_logs}")
        print("[E2E Test] test_elt_load_and_transform_flow completed successfully.")

    def test_action_handler_create_snapshot(self):
        """
        測試 Action Handler 的 create_log_snapshot 功能：
        - 先執行一個任務以生成日誌。
        - 調用 action_handler 創建日誌快照。
        - 驗證快照檔案被創建且內容正確。
        """
        print("[E2E Test] Running test_action_handler_create_snapshot")

        # Arrange: 執行一個任務以生成日誌
        initial_log_mission_params = {
            "EXECUTION_MODE": "標準分析流程",
            "ANALYSIS_TICKERS": "FOR_LOGGING_TEST",
            "ANALYSIS_START_DATE": "2024-02-01",
            "ANALYSIS_END_DATE": "2024-02-02",
            # DB_PATH_LOGS 會被 _run_mission_runner 自動注入
        }
        print("[E2E Arrange] Generating initial logs...")
        self._run_mission_runner(initial_log_mission_params, expected_return_code=0)

        # 確保日誌檔案已創建且包含一些內容 (由 fake_daily_market_analyzer 寫入)
        self.assertTrue(os.path.exists(self.db_path_logs), "初始日誌檔案未創建")
        with open(self.db_path_logs, "r", encoding="utf-8") as f_initial_logs:
            initial_log_content = f_initial_logs.read()
        self.assertIn("DAILY_MARKET_ANALYZER_LOG: Tickers FOR_LOGGING_TEST processed", initial_log_content)
        print(f"[E2E Arrange] Initial logs generated at: {self.db_path_logs}")

        # Act: 調用 action_handler 創建快照
        action_args = [
            "--action=create_log_snapshot",
            f"--log-db-path={self.db_path_logs}",
            f"--output-dir={self.snapshot_output_dir}" # self.snapshot_output_dir 就是 self.workspace_dir
        ]
        print(f"[E2E Act] Calling action_handler with args: {action_args}")
        ah_stdout, ah_stderr, ah_return_code = self._run_action_handler(action_args, expected_return_code=0)

        # Assert: 驗證快照
        # 1. 查找生成的快照檔案
        snapshot_files = [f for f in os.listdir(self.snapshot_output_dir) if f.startswith("log_snapshot_") and f.endswith(".txt")]
        self.assertEqual(len(snapshot_files), 1, f"應只生成一個快照檔案，但找到了 {len(snapshot_files)} 個: {snapshot_files}")
        snapshot_filepath = os.path.join(self.snapshot_output_dir, snapshot_files[0])
        print(f"[E2E Assert] Found snapshot file: {snapshot_filepath}")

        # 2. 讀取並驗證快照內容
        with open(snapshot_filepath, "r", encoding="utf-8") as f_snapshot:
            snapshot_content = f_snapshot.read()

        self.assertIn("Fake log snapshot content:", snapshot_content, "快照內容缺少預期的標頭")
        # 虛假 action_handler 會將 self.db_path_logs (作為文本文件) 的內容包含進去
        self.assertIn("Simulated read from:", snapshot_content)
        self.assertIn("DAILY_MARKET_ANALYZER_LOG: Tickers FOR_LOGGING_TEST processed", snapshot_content,
                      "快照內容未包含先前生成的日誌訊息")
        print(f"[E2E Assert] Verified snapshot content in: {snapshot_filepath}")
        print("[E2E Test] test_action_handler_create_snapshot completed successfully.")

    def test_high_frequency_hardware_logging(self):
        """
        測試在高頻硬體日誌記錄功能：
        - 執行一個會持續一段時間的任務。
        - 驗證日誌檔案中是否包含多條模擬的硬體日誌記錄。
        """
        print("[E2E Test] Running test_high_frequency_hardware_logging")

        num_hw_logs_to_generate = 5
        # 總時長約 message_count * 0.1s (主延時) + num_hw_logs * hw_log_interval (硬體日誌間隔)
        # 這裡 message_count 設為 num_hw_logs，確保有足夠的主循環來觸發硬體日誌
        message_count = num_hw_logs_to_generate
        hw_log_interval = 0.05 # 50ms

        mission_params = {
            "EXECUTION_MODE": "串流測試模式",
            "STREAMING_TEST_MESSAGE_COUNT": message_count,
            "STREAMING_TEST_NUM_HW_LOGS": num_hw_logs_to_generate,
            "STREAMING_TEST_HW_LOG_INTERVAL": hw_log_interval,
            "STREAMING_TEST_EXIT_CODE": 0,
            # DB_PATH_LOGS 會由 _run_mission_runner 自動注入
        }

        # Act
        print(f"[E2E Act] Executing streaming task to generate {num_hw_logs_to_generate} HW logs...")
        stdout, stderr, return_code = self._run_mission_runner(mission_params, expected_return_code=0)

        # Assert
        self.assertTrue(os.path.exists(self.db_path_logs), "日誌檔案未創建 (HW Logging)")

        hw_log_entries_found = 0
        with open(self.db_path_logs, "r", encoding="utf-8") as f_logs:
            for line in f_logs:
                if "HARDWARE_LOG_ENTRY:" in line:
                    hw_log_entries_found += 1

        # 由於虛假腳本的設計是在每個 message_count 循環內寫入一個硬體日誌 (如果 i <= num_hw_logs)
        # 所以預期生成的硬體日誌數量應該等於 num_hw_logs_to_generate
        self.assertEqual(hw_log_entries_found, num_hw_logs_to_generate,
                         f"預期找到 {num_hw_logs_to_generate} 條硬體日誌，實際找到 {hw_log_entries_found} 條。\n"
                         f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}\n"
                         f"Log file content ({self.db_path_logs}):\n{open(self.db_path_logs, 'r', encoding='utf-8').read()}")

        print(f"[E2E Assert] Found {hw_log_entries_found} hardware log entries as expected in {self.db_path_logs}")
        print("[E2E Test] test_high_frequency_hardware_logging completed successfully.")

    def test_force_refresh_options(self):
        """
        測試 FORCE_REPO_REFRESH 和 FORCE_DATA_REFRESH 選項的矩陣組合。
        - FORCE_REPO_REFRESH=True 時，mission_runner 應打印特定日誌。
        - FORCE_DATA_REFRESH=True 時，下游虛假腳本應打印特定日誌。
        """
        print("[E2E Test] Running test_force_refresh_options")
        import itertools

        base_mission_params = {
            "EXECUTION_MODE": "標準分析流程", # 使用一個會觸發 FORCE_DATA_REFRESH 日誌的模式
            "ANALYSIS_TICKERS": "REFRESH_TEST",
            "ANALYSIS_START_DATE": "2024-03-01",
            "ANALYSIS_END_DATE": "2024-03-01",
        }

        refresh_options = [True, False]
        # itertools.product 會生成 (True, True), (True, False), (False, True), (False, False)
        param_combinations = itertools.product(refresh_options, refresh_options)

        for force_repo, force_data in param_combinations:
            with self.subTest(force_repo_refresh=force_repo, force_data_refresh=force_data):
                print(f"\n[E2E SubTest] Testing with FORCE_REPO_REFRESH={force_repo}, FORCE_DATA_REFRESH={force_data}")

                current_params = base_mission_params.copy()
                current_params["FORCE_REPO_REFRESH"] = force_repo
                current_params["FORCE_DATA_REFRESH"] = force_data

                # Act
                stdout, stderr, return_code = self._run_mission_runner(current_params, expected_return_code=0)

                # Assert for FORCE_REPO_REFRESH
                repo_refresh_log_msg = "[MISSION_RUNNER_INFO] FORCE_REPO_REFRESH=True，模擬執行倉庫刷新操作（例如移除舊目錄）。"
                if force_repo:
                    self.assertIn(repo_refresh_log_msg, stderr,
                                  f"預期在 STDERR 中找到 '{repo_refresh_log_msg}' 當 FORCE_REPO_REFRESH=True")
                    print(f"[E2E Assert] Verified REPO_REFRESH log in STDERR for FORCE_REPO_REFRESH={force_repo}")
                else:
                    self.assertNotIn(repo_refresh_log_msg, stderr,
                                     f"不應在 STDERR 中找到 '{repo_refresh_log_msg}' 當 FORCE_REPO_REFRESH=False")
                    print(f"[E2E Assert] Verified NO REPO_REFRESH log in STDERR for FORCE_REPO_REFRESH={force_repo}")

                # Assert for FORCE_DATA_REFRESH (來自 fake_daily_market_analyzer 的 stdout)
                data_refresh_log_msg = "[FAKE ANALYZER] 強制刷新數據已啟用 (FORCE_DATA_REFRESH=True)"
                if force_data:
                    self.assertIn(data_refresh_log_msg, stdout,
                                  f"預期在 STDOUT 中找到 '{data_refresh_log_msg}' 當 FORCE_DATA_REFRESH=True")
                    print(f"[E2E Assert] Verified DATA_REFRESH log in STDOUT for FORCE_DATA_REFRESH={force_data}")
                else:
                    # 注意：如果 FORCE_DATA_REFRESH=False，虛假腳本不會打印任何關於它的訊息，所以我們檢查的是“不包含”
                    # 但如果虛假腳本在 False 時打印了不同的訊息，則需要調整此斷言
                    self.assertNotIn(data_refresh_log_msg, stdout,
                                     f"不應在 STDOUT 中找到 '{data_refresh_log_msg}' 當 FORCE_DATA_REFRESH=False")
                    print(f"[E2E Assert] Verified NO DATA_REFRESH log in STDOUT for FORCE_DATA_REFRESH={force_data}")

        print("[E2E Test] test_force_refresh_options completed successfully.")


if __name__ == "__main__":
    # 為了讓 _test_e2e_harness.py 可以獨立運行並創建虛假腳本，
    # 我們需要一個方法來觸發虛假腳本的創建。
    # 一個簡單的方法是，如果直接運行此文件，則先調用一個設置函數。
    # 或者，依賴於一個共享的初始化模組。
    # 目前，我們假設執行此測試前，虛假腳本已由其他方式準備好
    # (例如，手動運行 apps/mission_runner/_test_run.py 一次以生成它們，或者在 CI 流程中先執行它)
    # 更好的做法是在 TestE2EHarness.setUpClass 中創建所有依賴的虛假腳本。

    # 確保在運行 E2E 測試前，所有依賴的虛假腳本都已生成
    print("[E2E Main] Ensuring fake scripts are set up...")
    from apps.mission_runner._test_run import TestMissionRunner
    try:
        TestMissionRunner.setUpClass() # 這會創建/覆蓋虛假腳本
        print("[E2E Main] Fake scripts setup complete via TestMissionRunner.setUpClass().")
    except Exception as e:
        print(f"[E2E Main ERROR] Failed to set up fake scripts: {e}")
        # 根據情況決定是否退出
        # sys.exit(1)

    unittest.main()
