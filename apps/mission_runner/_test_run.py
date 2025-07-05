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

    def _extract_downstream_json_output(self, full_stdout, expected_tag=None):
        """
        從 mission_runner 的完整 stdout 中提取下游腳本的 JSON 輸出。
        假設 JSON 是身份標籤之前的最後一個 '{...}' 行，或者如果沒有標籤，則是日誌之間的最後一個 '{...}' 行。
        """
        actual_downstream_stdout = ""
        in_downstream_output = False
        for line in full_stdout.splitlines():
            if "下游服務即時戰報結束" in line:
                in_downstream_output = False
            if in_downstream_output:
                actual_downstream_stdout += line + "\n"
            if "下游服務即時戰報開始" in line:
                in_downstream_output = True

        if not actual_downstream_stdout.strip():
            self.fail(f"未能從 mission_runner stdout 中提取到下游腳本的輸出。\n完整 STDOUT:\n{full_stdout}")

        json_line = ""
        lines = actual_downstream_stdout.strip().splitlines()

        if not lines:
             self.fail(f"提取的下游腳本輸出為空。\n完整 STDOUT:\n{full_stdout}")

        # 優先嘗試從標籤前一行獲取 JSON
        if expected_tag and lines[-1].strip() == expected_tag:
            if len(lines) > 1 and lines[-2].strip().startswith("{") and lines[-2].strip().endswith("}"):
                json_line = lines[-2]
            # 如果標籤是唯一一行，或者標籤前沒有 JSON，則可能 JSON 是更早的獨立行
            elif lines[0].strip().startswith("{") and lines[0].strip().endswith("}"): # 檢查第一行是不是JSON
                 json_line = lines[0]

        if not json_line:
            # 如果沒有標籤或上述策略失敗，則從後往前找第一個看起來像 JSON 的行
            for line in reversed(lines):
                stripped_line = line.strip()
                if stripped_line.startswith("{") and stripped_line.endswith("}"):
                    try:
                        json.loads(stripped_line) # 驗證是否為有效 JSON
                        json_line = stripped_line
                        break
                    except json.JSONDecodeError:
                        continue # 不是有效的 JSON，繼續找

        if not json_line:
            self.fail(f"無法從下游腳本輸出中找到 JSON 行。\n提取的下游 STDOUT:\n{actual_downstream_stdout}\n完整 STDOUT:\n{full_stdout}")

        try:
            return json.loads(json_line)
        except json.JSONDecodeError as e:
            self.fail(f"解析下游腳本 JSON 輸出失敗: {e}\n完整 STDOUT:\n{full_stdout}\n提取的下游 STDOUT:\n{actual_downstream_stdout}\n嘗試解析的 JSON Line: '{json_line}'")


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
import os # <<<< ADDED IMPORT OS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假市場分析器 FOR TESTING")
    parser.add_argument("--tickers", help="分析的股票代碼")
    parser.add_argument("--start-date", help="分析開始日期")
    parser.add_argument("--end-date", help="分析結束日期")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據")
    parser.add_argument("--db-path-logs", help="日誌資料庫路徑")
    parser.add_argument("--db-path-yfinance-cache", help="YFinance 快取資料庫路徑")
    # 模擬接收任意其他參數
    parser.add_argument('--extra-args', nargs='*', help='其他可能的參數')

    args, unknown = parser.parse_known_args()

    if args.force_data_refresh:
        print("[FAKE ANALYZER] 強制刷新數據已啟用 (FORCE_DATA_REFRESH=True)")

    # 模擬資料庫操作和日誌記錄
    if args.db_path_logs:
        with open(args.db_path_logs, "a", encoding="utf-8") as f_log:
            f_log.write(f"DAILY_MARKET_ANALYZER_LOG: Tickers {args.tickers} processed. Log DB: {args.db_path_logs}. Success.\\n")
        print(f"[FAKE ANALYZER] Logged to {args.db_path_logs}")

    if args.db_path_yfinance_cache:
        with open(args.db_path_yfinance_cache, "a", encoding="utf-8") as f_cache:
            f_cache.write(f"YFINANCE_CACHE_DUMMY_DATA: For {args.tickers}, data from {args.start_date} to {args.end_date} written.\\n")
        print(f"[FAKE ANALYZER] Wrote to cache {args.db_path_yfinance_cache}")

    output_data = {"script": "daily_market_analyzer", "args": vars(args), "cwd": os.getcwd()}
    print(json.dumps(output_data)) # 主要用於 _test_run.py 自身的斷言
    print("[FAKE_ANALYZER_EXECUTION_COMPLETE]", flush=True)
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
import os # <<<< ADDED IMPORT OS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假台指期數據管線 FOR TESTING")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據")
    parser.add_argument("--db-path-logs", help="日誌資料庫路徑")
    parser.add_argument("--db-path-raw-taifex", help="原始 Taifex 資料庫路徑")
    parser.add_argument("--db-path-taifex-historical", help="歷史 Taifex 資料庫路徑")
    parser.add_argument("--input-file-path", help="ELT 載入步驟的輸入檔案路徑")
    parser.add_argument("--pipeline-step", choices=['load', 'transform'], help="ELT 管線步驟 (load 或 transform)")
    # 模擬接收任意其他參數
    parser.add_argument('--extra-args', nargs='*', help='其他可能的參數')

    args, unknown = parser.parse_known_args()

    if args.force_data_refresh:
        print("[FAKE TAIFEX PIPELINE] 強制刷新數據已啟用 (FORCE_DATA_REFRESH=True)")

    if args.db_path_logs:
        with open(args.db_path_logs, "a", encoding="utf-8") as f_log:
            f_log.write(f"TAIFEX_PIPELINE_LOG: Step {args.pipeline_step}. Log DB: {args.db_path_logs}.\\n")
        print(f"[FAKE TAIFEX PIPELINE] Logged to {args.db_path_logs}")

    if args.pipeline_step == 'load':
        if args.input_file_path and args.db_path_raw_taifex:
            # 模擬從 input_file_path 讀取並寫入 db_path_raw_taifex
            with open(args.db_path_raw_taifex, "a", encoding="utf-8") as f_db:
                f_db.write(f"RAW_TAIFEX_DATA: Loaded from {args.input_file_path}.\\n")
            print(f"[FAKE TAIFEX PIPELINE] Loaded data from {args.input_file_path} to {args.db_path_raw_taifex}")
        else:
            print("[FAKE TAIFEX PIPELINE ERROR] Load step missing input_file_path or db_path_raw_taifex.", file=sys.stderr)
            sys.exit(1)
    elif args.pipeline_step == 'transform':
        if args.db_path_raw_taifex and args.db_path_taifex_historical:
            # 模擬從 db_path_raw_taifex 讀取並寫入 db_path_taifex_historical
            with open(args.db_path_taifex_historical, "a", encoding="utf-8") as f_db:
                f_db.write(f"HISTORICAL_TAIFEX_DATA: Transformed from {args.db_path_raw_taifex}.\\n")
            print(f"[FAKE TAIFEX PIPELINE] Transformed data from {args.db_path_raw_taifex} to {args.db_path_taifex_historical}")
        else:
            print("[FAKE TAIFEX PIPELINE ERROR] Transform step missing db_path_raw_taifex or db_path_taifex_historical.", file=sys.stderr)
            sys.exit(1)

    output_data = {"script": "taifex_data_pipeline", "args": vars(args), "cwd": os.getcwd()}
    print(json.dumps(output_data)) # 主要用於 _test_run.py 自身的斷言
    if args.pipeline_step == 'load':
        print("[FAKE_PIPELINE_EXECUTION_COMPLETE_LOAD]", flush=True)
    elif args.pipeline_step == 'transform':
        print("[FAKE_PIPELINE_EXECUTION_COMPLETE_TRANSFORM]", flush=True)
    else: # 如果沒有 pipeline_step (雖然不太可能，因為 argparse 有 choices) 或其他情況
        print("[FAKE_PIPELINE_EXECUTION_COMPLETE_UNKNOWN_STEP]", flush=True)
    sys.exit(0)
"""
        with open(fake_taifex_script_path, "w", encoding="utf-8") as f:
            f.write(fake_taifex_content)
        # print(f"DEBUG: Created fake taifex at {fake_taifex_script_path}")

        # 創建用於串流測試的虛假腳本
        fake_streaming_script_path = os.path.join(apps_dir, "fake_streaming_script", "run.py") # 假設一個新目錄
        os.makedirs(os.path.dirname(fake_streaming_script_path), exist_ok=True)
        fake_streaming_content = """
# apps/fake_streaming_script/run.py (虛假腳本 FOR STREAMING TEST)
import sys
import time
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假串流腳本 FOR TESTING")
    parser.add_argument("--exit-code", type=int, default=0, help="腳本的退出碼")
    parser.add_argument("--message-count", type=int, default=3, help="要打印的訊息數量")
    parser.add_argument("--db-path-logs", help="日誌資料庫路徑 (用於模擬硬體日誌)")
    parser.add_argument("--num-hw-logs", type=int, default=0, help="要生成的模擬硬體日誌數量")
    parser.add_argument("--hw-log-interval", type=float, default=0.05, help="模擬硬體日誌之間的間隔（秒）")
    args = parser.parse_args()

    for i in range(1, args.message_count + 1):
        print(f"串流訊息 #{i}")
        sys.stdout.flush() # 確保立即刷新緩衝區

        # 在打印串流訊息之間模擬寫入硬體日誌
        if args.db_path_logs and args.num_hw_logs > 0 :
            # 假設每次打印串流訊息後，都嘗試寫入一部分硬體日誌
            # 為了簡化，這裡就在每次主循環時寫入一次（如果 num_hw_logs 允許）
            # 或者可以設計成更均勻分佈
            if i <= args.num_hw_logs: # 簡單控制寫入次數
                 with open(args.db_path_logs, "a", encoding="utf-8") as f_log:
                    timestamp = time.time()
                    # 模擬 hardware_logs 表的格式 (timestamp, cpu_usage, memory_usage)
                    # 這裡用簡化的文本格式
                    f_log.write(f"HARDWARE_LOG_ENTRY: {timestamp}, cpu_usage_fake_{(i*10)%100}, memory_usage_fake_{(i*5)%80}\\n")
                 print(f"[FAKE STREAMING SCRIPT] Wrote HW log entry to {args.db_path_logs}")
                 if args.hw_log_interval > 0:
                    time.sleep(args.hw_log_interval)


        if i < args.message_count: #不在最後一次打印後睡眠
            time.sleep(0.1) # 模擬主要工作的耗時操作

    if args.exit_code != 0:
        print(f"串流腳本將以錯誤碼 {args.exit_code} 退出。", file=sys.stderr, flush=True)
        # 不打印成功標籤，因為它失敗了
    else:
        print("[FAKE_STREAMING_SCRIPT_EXECUTION_COMPLETE]", flush=True)

    sys.exit(args.exit_code)
"""
        with open(fake_streaming_script_path, "w", encoding="utf-8") as f:
            f.write(fake_streaming_content)
        # print(f"DEBUG: Created fake streaming script at {fake_streaming_script_path}")

        # 創建虛假的 action_handler/run.py
        fake_action_handler_script_path = os.path.join(apps_dir, "action_handler", "run.py")
        os.makedirs(os.path.dirname(fake_action_handler_script_path), exist_ok=True)
        fake_action_handler_content = """
# apps/action_handler/run.py (虛假腳本 FOR E2E TESTING)
import argparse
import sys
import os
import datetime
import sqlite3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假 Action Handler FOR E2E TESTING")
    parser.add_argument("--action", required=True, choices=['create_log_snapshot'])
    parser.add_argument("--log-db-path", help="日誌資料庫路徑 (用於 create_log_snapshot)")
    parser.add_argument("--output-dir", help="快照輸出目錄 (用於 create_log_snapshot)")
    # 可以添加更多 action 和它們的參數

    args = parser.parse_args()

    if args.action == 'create_log_snapshot':
        if not args.log_db_path or not args.output_dir:
            print("[FAKE ACTION HANDLER ERROR] create_log_snapshot 需要 --log-db-path 和 --output-dir", file=sys.stderr)
            sys.exit(1)

        snapshot_filename = f"log_snapshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        snapshot_filepath = os.path.join(args.output_dir, snapshot_filename)

        try:
            # 模擬從日誌數據庫讀取日誌
            # 為了簡單起見，如果日誌DB是SQLite，我們嘗試連接並讀取；否則，就寫入一些虛擬日誌。
            log_content = "Fake log snapshot content:\\n"
            if os.path.exists(args.log_db_path):
                try:
                    # 這裡假設 logs.sqlite 包含一個名為 'logs' 的表，且有 'message' 欄位
                    # 實際的 E2E 測試中，這個 logs.sqlite 是由其他虛假腳本填充的
                    # 為了讓這個虛假 action_handler 能工作，我們不需要真的去讀取 SQLite，
                    # 只需要模擬寫入快照檔案即可。
                    # 但如果 test_action_handler_create_snapshot 要驗證內容，
                    # 則這裡的虛假腳本和填充日誌的虛假腳本需要協同。
                    # 簡單起見，我們這裡寫入固定的幾行 + args.log_db_path 的內容（如果是文字檔）
                    log_content += f"Simulated read from: {args.log_db_path}\\n"
                    if os.path.isfile(args.log_db_path): # 如果日誌DB是個文字檔（例如之前虛假腳本寫的）
                         with open(args.log_db_path, "r", encoding="utf-8") as f_db_content:
                            log_content += f_db_content.read()

                except Exception as e:
                    log_content += f"Error reading log db (this is a fake handler, so this is ok): {e}\\n"
            else:
                log_content += "Log database not found, using placeholder content.\\n"

            with open(snapshot_filepath, "w", encoding="utf-8") as f_snapshot:
                f_snapshot.write(log_content)
            print(f"[FAKE ACTION HANDLER] Created log snapshot: {snapshot_filepath}")
            print("[FAKE_ACTION_HANDLER_EXECUTION_COMPLETE]", flush=True)
            sys.exit(0)
        except Exception as e:
            print(f"[FAKE ACTION HANDLER ERROR] Failed to create snapshot: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"[FAKE ACTION HANDLER ERROR] Unknown action: {args.action}", file=sys.stderr)
        sys.exit(1)
"""
        with open(fake_action_handler_script_path, "w", encoding="utf-8") as f:
            f.write(fake_action_handler_content)
        # print(f"DEBUG: Created fake action_handler script at {fake_action_handler_script_path}")

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
            downstream_output = self._extract_downstream_json_output(stdout, expected_tag="[FAKE_ANALYZER_EXECUTION_COMPLETE]")

            self.assertEqual(downstream_output.get("script"), "daily_market_analyzer")
            received_args = downstream_output.get("args", {})

            self.assertEqual(received_args.get("tickers"), params["ANALYSIS_TICKERS"])
            self.assertEqual(received_args.get("start_date"), params["ANALYSIS_START_DATE"])
            self.assertEqual(received_args.get("end_date"), params["ANALYSIS_END_DATE"])
            self.assertEqual(received_args.get("force_data_refresh"), False)

        except KeyError as e: # _extract_downstream_json_output 內部會處理 JSONDecodeError
            self.fail(f"下游腳本輸出中缺少預期的鍵: {e}\n完整 STDOUT:\n{stdout}")

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
            # 這裡的 fake_taifex_data_pipeline 在 ELT mode (無 step) 時打印 FAKE_PIPELINE_EXECUTION_COMPLETE_UNKNOWN_STEP
            downstream_output = self._extract_downstream_json_output(stdout, expected_tag="[FAKE_PIPELINE_EXECUTION_COMPLETE_UNKNOWN_STEP]")

            self.assertEqual(downstream_output.get("script"), "taifex_data_pipeline")
            received_args = downstream_output.get("args", {})

            self.assertEqual(received_args.get("force_data_refresh"), False,
                             "FORCE_DATA_REFRESH 應為 False 或未在下游腳本參數中設置為 True")

        except KeyError as e:
            self.fail(f"下游腳本輸出中缺少預期的鍵: {e}\n完整 STDOUT:\n{stdout}")

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
            # fake_daily_market_analyzer 會先打印 FORCE_DATA_REFRESH 訊息，然後 JSON，然後身份標籤
            downstream_output = self._extract_downstream_json_output(stdout, expected_tag="[FAKE_ANALYZER_EXECUTION_COMPLETE]")

            self.assertEqual(downstream_output.get("script"), "daily_market_analyzer")
            received_args = downstream_output.get("args", {})

            self.assertEqual(received_args.get("force_data_refresh"), True,
                             "下游腳本接收到的 force_data_refresh 參數應為 True")

            # 額外驗證 stdout 中確實包含了 FORCE_DATA_REFRESH 的日誌訊息
            self.assertIn("[FAKE ANALYZER] 強制刷新數據已啟用 (FORCE_DATA_REFRESH=True)", stdout)

        except KeyError as e:
            self.fail(f"下游腳本輸出中缺少預期的鍵: {e}\n完整 STDOUT:\n{stdout}")

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
            downstream_output = self._extract_downstream_json_output(stdout, expected_tag="[FAKE_ANALYZER_EXECUTION_COMPLETE]")
            self.assertEqual(downstream_output.get("args", {}).get("tickers"), "NQ=F,ES=F,^VIX")
        except KeyError as e:
             self.fail(f"下游腳本輸出中缺少預期的鍵 'tickers': {e}\n完整 STDOUT:\n{stdout}")


    def test_streaming_output_and_return_code(self):
        """
        測試案例：驗證 mission_runner 是否能即時串流下游腳本的輸出，
                  並正確傳遞下游腳本的返回碼。
        """
        # 測試 1: 成功執行，多條訊息，返回碼 0
        success_params = {
            "REPOSITORY_URL": "git@test.com:repo.git", # 必要參數
            "TARGET_BRANCH": "main",                  # 必要參數
            "EXECUTION_MODE": "串流測試模式",
            "STREAMING_TEST_EXIT_CODE": 0,
            "STREAMING_TEST_MESSAGE_COUNT": 3
        }
        stdout_success, stderr_success, return_code_success = run_mission_runner(success_params)

        # 驗證返回碼
        self.assertEqual(return_code_success, 0, f"串流測試 (成功) 應返回 0。STDERR: {stderr_success}")

        # 驗證串流輸出
        # stdout_success 應包含所有來自 fake_streaming_script 的 print 輸出
        # stderr_success 應包含 mission_runner 的 INFO/DEBUG (如果有的話) 以及 fake_streaming_script 的 stderr (如果有的話)
        # 由於我們將 stderr=subprocess.STDOUT, fake_streaming_script 的 stderr 也會出現在 stdout_success

        expected_lines_success = [
            "串流訊息 #1",
            "串流訊息 #2",
            "串流訊息 #3"
        ]
        # 移除空行並比較
        # 從 stdout_success 提取下游腳本的實際輸出
        downstream_stdout_success = ""
        in_downstream_output_success = False
        for line in stdout_success.splitlines():
            if "下游服務即時戰報結束" in line:
                in_downstream_output_success = False
            if in_downstream_output_success:
                downstream_stdout_success += line + "\n"
            if "下游服務即時戰報開始" in line:
                in_downstream_output_success = True

        actual_lines_success = [line for line in downstream_stdout_success.strip().splitlines() if line.strip() and "[FAKE_STREAMING_SCRIPT_EXECUTION_COMPLETE]" not in line]

        self.assertEqual(len(actual_lines_success), len(expected_lines_success),
                         f"預期輸出 {len(expected_lines_success)} 行，實際 {len(actual_lines_success)} 行。\n完整 STDOUT:\n{stdout_success}\n提取的下游 STDOUT:\n{downstream_stdout_success}")

        for i, expected_line in enumerate(expected_lines_success):
            self.assertEqual(expected_line, actual_lines_success[i], # 改為assertEqual以精確匹配
                          f"第 {i+1} 行輸出不匹配。\n預期: '{expected_line}'\n實際: '{actual_lines_success[i]}'\n提取的下游 STDOUT:\n{downstream_stdout_success}")

        # 測試 2: 失敗執行，返回碼非零
        failure_exit_code = 5
        failure_message_count = 2
        failure_params = {
            "REPOSITORY_URL": "git@test.com:repo.git",
            "TARGET_BRANCH": "main",
            "EXECUTION_MODE": "串流測試模式",
            "STREAMING_TEST_EXIT_CODE": failure_exit_code,
            "STREAMING_TEST_MESSAGE_COUNT": failure_message_count
        }
        stdout_failure, stderr_failure, return_code_failure = run_mission_runner(failure_params)

        # 驗證返回碼
        self.assertEqual(return_code_failure, failure_exit_code,
                         f"串流測試 (失敗) 應返回 {failure_exit_code}。STDERR: {stderr_failure}")

        # 驗證串流輸出 (仍然應該能看到 fake_streaming_script 在退出前的 stdout/stderr)
        expected_lines_failure = [
            "串流訊息 #1",
            "串流訊息 #2",
            f"串流腳本將以錯誤碼 {failure_exit_code} 退出。"
        ]

        # 從 stdout_failure 提取下游腳本的實際輸出
        downstream_stdout_failure = ""
        in_downstream_output_failure = False
        for line in stdout_failure.splitlines():
            if "下游服務即時戰報結束" in line:
                in_downstream_output_failure = False
            if in_downstream_output_failure:
                downstream_stdout_failure += line + "\n"
            if "下游服務即時戰報開始" in line:
                in_downstream_output_failure = True

        actual_lines_failure = [line for line in downstream_stdout_failure.strip().splitlines() if line.strip()]

        # 檢查提取的下游輸出是否包含了預期的訊息行
        for expected_line in expected_lines_failure:
            self.assertTrue(any(expected_line in actual_line for actual_line in actual_lines_failure),
                            f"預期在下游 STDOUT 中找到 '{expected_line}'，但未找到。\n提取的下游 STDOUT:\n{downstream_stdout_failure}\n完整 STDOUT:\n{stdout_failure}")

        # 驗證 mission_runner 自身的錯誤訊息 (它會打印到 mission_runner 的 stderr)
        # 注意：mission_runner.py 中使用的是 "執行失敗，返回碼:"
        expected_stderr_msg = f"[MISSION_RUNNER_FAILURE] 下游任務 '/app/apps/fake_streaming_script/run.py' 執行失敗，返回碼: {failure_exit_code}"
        self.assertIn(expected_stderr_msg,
                      stderr_failure,
                      f"預期的 mission_runner 錯誤訊息未在 STDERR 中找到。\n預期包含: '{expected_stderr_msg}'\n實際 STDERR:\n{stderr_failure}")


if __name__ == "__main__":
    unittest.main()
