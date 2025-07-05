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
ACTION_HANDLER_SCRIPT = os.path.join(APPS_DIR, "action_handler", "run.py") # 將被移除或更新
MAIN_EXECUTOR_SCRIPT = os.path.join(APPS_DIR, "main_executor", "run.py")
DASHBOARD_API_SCRIPT = os.path.join(APPS_DIR, "dashboard_api", "run.py")


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

    def _params_dict_to_list(self, params_dict):
        """將參數字典轉換為 argparse 列表。"""
        args_list = []
        for key, value in params_dict.items():
            arg_name = f"--{key.replace('_', '-')}"
            if isinstance(value, bool): # 處理布林型參數
                if value: # 如果為 True，只添加旗標本身
                    args_list.append(arg_name)
                # 如果為 False，則不添加該旗標 (argparse 預設行為)
            elif value is not None: # 非 None 值，添加旗標和值
                args_list.extend([arg_name, str(value)])
            # None 值將被忽略，不添加到參數列表
        return args_list

    def _run_main_executor(self, params_dict, expected_return_code=0, timeout=180): # 增加超時
        """執行 main_executor 並返回其 stdout, stderr 和返回碼。"""
        # 自動注入指向工作區的日誌路徑，這是 main_executor 的必要參數
        params_dict.setdefault("db_path_logs", self.db_path_logs)

        # 其他特定於 main_executor 可能需要的、且應指向工作區的路徑，
        # 測試調用方應在 params_dict 中明確提供，例如：
        # params_dict.setdefault("db_path_yfinance_cache", self.db_path_yfinance_cache)
        # params_dict.setdefault("db_path_raw_taifex", self.db_path_raw_taifex)
        # params_dict.setdefault("db_path_taifex_historical", self.db_path_taifex_historical)
        # params_dict.setdefault("reports_dir", self.reports_dir) # 如果 main_executor 用到

        args_list = self._params_dict_to_list(params_dict)
        return self._run_script(MAIN_EXECUTOR_SCRIPT, args_list, expected_return_code, timeout)

    def _run_dashboard_api(self, params_dict, expected_return_code=0, timeout=30):
        """執行 dashboard_api 並返回其 stdout, stderr 和返回碼。"""
        # 自動注入指向工作區的日誌路徑
        params_dict.setdefault("db_path_logs", self.db_path_logs)
        args_list = self._params_dict_to_list(params_dict)
        # Dashboard API 的輸出是 JSON，直接返回 stdout 給調用者解析
        stdout, stderr, return_code = self._run_script(DASHBOARD_API_SCRIPT, args_list, expected_return_code, timeout)
        return stdout, stderr, return_code # 返回三元組以保持一致性

    # --- v70.0 核心測試方法 ---
    def test_full_mission_and_dashboard_verification(self):
        """
        v70.0 終極驗收測試：
        1. 執行主任務 (main_executor)。
        2. 驗證任務副作用 (日誌庫、產出檔案)。
        3. 獲取儀表板數據 (dashboard_api)。
        4. 驗證 API 數據與日誌庫內容一致。
        """
        print("[E2E Test] Running test_full_mission_and_dashboard_verification")

        # --- Arrange ---
        # 準備 main_executor 的參數
        main_executor_params = {
            "execution_mode": "standard_analysis",
            "tickers": "AAPL,GOOG",
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "force_data_refresh": False,
            # db_path_logs, db_path_yfinance_cache 等會由 _run_main_executor 自動或按需設置
            # 確保傳遞 yfinance_cache 路徑，因為 standard_analysis 模式的 main_executor parser 需要它
            "db_path_yfinance_cache": self.db_path_yfinance_cache,
        }
        expected_main_executor_log_message_part = "開始執行標準分析流程。標的: AAPL,GOOG"
        expected_main_executor_success_message = "模式 standard_analysis 成功執行完畢。"

        # --- Act 1: 執行主任務 ---
        print("[E2E Test] Act 1: Executing main_executor...")
        executor_stdout, executor_stderr, executor_return_code = self._run_main_executor(
            main_executor_params,
            expected_return_code=0
        )
        # executor_stdout 通常是空的，因為 main_executor 主要通過 logging 模組記錄到資料庫和控制台（如果配置了）
        # executor_stderr 可能包含 logging 到控制台的 DEBUG/INFO 訊息 (如果 setup_logging 中配置了 StreamHandler 到 stderr)
        # 或者，如果 setup_logging 的 StreamHandler 指向 stdout，那 executor_stdout 就會有內容。
        # 根據我 log_utils.py 的實現，StreamHandler 指向 sys.stdout。
        # 所以 executor_stdout 會包含日誌。

        # --- Assert 1: 驗證任務副作用 ---
        print("[E2E Test] Assert 1: Verifying main_executor side-effects...")
        # 1.1 驗證日誌資料庫是否創建且包含預期日誌
        self.assertTrue(os.path.exists(self.db_path_logs), f"日誌資料庫 {self.db_path_logs} 未創建。")

        conn = sqlite3.connect(self.db_path_logs)
        cursor = conn.cursor()

        # 檢查是否有 "開始執行標準分析流程" 日誌
        cursor.execute("SELECT COUNT(*) FROM logs WHERE message LIKE ?", (f"%{expected_main_executor_log_message_part}%",))
        log_count = cursor.fetchone()[0]
        self.assertGreaterEqual(log_count, 1, f"未在日誌資料庫中找到預期的啟動日誌訊息 '{expected_main_executor_log_message_part}'")
        print(f"[E2E Assert] Found main_executor start log message.")

        # 檢查是否有 "成功執行完畢" 日誌
        cursor.execute("SELECT COUNT(*) FROM logs WHERE message LIKE ?", (f"%{expected_main_executor_success_message}%",))
        success_log_count = cursor.fetchone()[0]
        self.assertGreaterEqual(success_log_count, 1, f"未在日誌資料庫中找到預期的成功日誌訊息 '{expected_main_executor_success_message}'")
        print(f"[E2E Assert] Found main_executor success log message.")

        # 檢查是否有硬體日誌
        cursor.execute("SELECT COUNT(*) FROM hardware_logs")
        hw_log_count = cursor.fetchone()[0]
        self.assertGreaterEqual(hw_log_count, 1, "未找到任何硬體日誌記錄。")
        print(f"[E2E Assert] Found {hw_log_count} hardware log entries.")

        conn.close()

        # 1.2 驗證其他預期產出檔案 (例如 yfinance_cache) - 這裡 main_executor 是模擬執行，所以不會真的創建
        # 如果 main_executor 真的調用了會創建 yfinance_cache 的邏輯，則可以取消註釋：
        # self.assertTrue(os.path.exists(self.db_path_yfinance_cache),
        #                 f"YFinance 快取資料庫 {self.db_path_yfinance_cache} 未創建。")
        # print(f"[E2E Assert] Verified yfinance_cache existence (if applicable).")


        # --- Act 2: 獲取儀表板數據 ---
        print("[E2E Test] Act 2: Getting dashboard data...")
        dashboard_api_params = {
            "log_history_limit": 3
            # db_path_logs 會由 _run_dashboard_api 自動注入
        }
        api_stdout, api_stderr, api_return_code = self._run_dashboard_api(
            dashboard_api_params,
            expected_return_code=0
        )

        # --- Assert 2: 驗證 API 數據 ---
        print("[E2E Test] Assert 2: Verifying dashboard_api data...")
        self.assertTrue(api_stdout, "Dashboard API 沒有任何輸出 (stdout)。")

        try:
            dashboard_json = json.loads(api_stdout)
        except json.JSONDecodeError as e:
            self.fail(f"無法解析 Dashboard API 的 JSON 輸出: {e}\nAPI STDOUT:\n{api_stdout}")

        self.assertIsNone(dashboard_json.get("error"),
                          f"Dashboard API 返回了錯誤訊息: {dashboard_json.get('error')}")

        # 驗證 system_status.latest_event 是否與日誌中的成功訊息相關
        # 由於 main_executor 的日誌格式是 "[asctime] [LEVEL] [module.func:lineno] - message"
        # 而 dashboard_api 直接返回 message，所以可以部分匹配
        self.assertIn(expected_main_executor_success_message,
                      dashboard_json.get("system_status", {}).get("latest_event", ""),
                      "Dashboard API 的 latest_event 與預期的 main_executor 成功日誌不符。")
        print(f"[E2E Assert] Verified dashboard latest_event.")

        # 驗證 hardware_monitor 是否有數據 (main_executor 至少記錄了一次)
        self.assertIsNotNone(dashboard_json.get("hardware_monitor", {}).get("cpu_percent"),
                             "Dashboard API 未返回 CPU 使用率。")
        self.assertIsNotNone(dashboard_json.get("hardware_monitor", {}).get("ram_percent"),
                             "Dashboard API 未返回 RAM 使用率。")
        print(f"[E2E Assert] Verified dashboard hardware_monitor data presence.")

        # 驗證 log_history
        log_history = dashboard_json.get("log_history", [])
        self.assertGreaterEqual(len(log_history), 1, "Dashboard API 的 log_history 為空。")
        # 檢查 log_history 中是否包含成功訊息
        found_success_in_history = any(expected_main_executor_success_message in log_entry for log_entry in log_history)
        self.assertTrue(found_success_in_history,
                        f"Dashboard API 的 log_history 未包含預期的 main_executor 成功日誌 '{expected_main_executor_success_message}'.\nLog History:\n{log_history}")
        print(f"[E2E Assert] Verified dashboard log_history content.")

        print("[E2E Test] test_full_mission_and_dashboard_verification completed successfully.")


if __name__ == "__main__":
    # 為了讓 _test_e2e_harness.py 可以獨立運行並創建虛假腳本，
    # 我們需要一個方法來觸發虛假腳本的創建。
    # 一個簡單的方法是，如果直接運行此文件，則先調用一個設置函數。
    # 或者，依賴於一個共享的初始化模組。
    # 目前，我們假設執行此測試前，虛假腳本已由其他方式準備好
    # (例如，手動運行 apps/mission_runner/_test_run.py 一次以生成它們，或者在 CI 流程中先執行它)
    # 更好的做法是在 TestE2EHarness.setUpClass 中創建所有依賴的虛假腳本。

    # v70.0 不再依賴 apps.mission_runner._test_run 來生成虛假腳本
    # print("[E2E Main] Ensuring fake scripts are set up...")
    # from apps.mission_runner._test_run import TestMissionRunner # 已廢棄
    # try:
        # TestMissionRunner.setUpClass() # 這會創建/覆蓋虛假腳本
        # print("[E2E Main] Fake scripts setup complete via TestMissionRunner.setUpClass().")
    # except Exception as e:
        # print(f"[E2E Main ERROR] Failed to set up fake scripts: {e}")
        # sys.exit(1)

    unittest.main()
