# apps/mission_runner/run.py
import argparse
import json
import base64
import subprocess
import sys
import os # 用於檢查腳本是否存在

def build_analyzer_args(validated_params): # 改用 validated_params 以清晰表明來源
    # 待辦事項：根據實際下游腳本的需要，精確調整參數名稱和格式
    args = []
    # ANALYSIS_TICKERS 在 validated_params 中總是有值 (來自輸入或預設值)
    args.extend(["--tickers", validated_params["ANALYSIS_TICKERS"]])

    if validated_params.get("ANALYSIS_START_DATE"):
        args.extend(["--start-date", validated_params["ANALYSIS_START_DATE"]])
    if validated_params.get("ANALYSIS_END_DATE"):
        args.extend(["--end-date", validated_params["ANALYSIS_END_DATE"]])

    # 處理布林型旗標參數
    if validated_params.get("FORCE_DATA_REFRESH", False): # 確保即使 validated_params 中沒有也安全
        args.append("--force-data-refresh")

    # E2E 測試傳入的資料庫路徑參數
    if validated_params.get("DB_PATH_LOGS"):
        args.extend(["--db-path-logs", validated_params["DB_PATH_LOGS"]])
    if validated_params.get("DB_PATH_YFINANCE_CACHE"):
        args.extend(["--db-path-yfinance-cache", validated_params["DB_PATH_YFINANCE_CACHE"]])

    # 假設 REPOSITORY_URL, TARGET_BRANCH, FORCE_REPO_REFRESH 等參數
    # 主要由 mission_runner 本身使用 (例如，用於前置的 git 操作)，
    # 或者需要決定是否以及如何傳遞給下游。
    # 此處暫未將所有 validated_params 都傳遞給 build_analyzer_args 的下游。
    # 如果 daily_market_analyzer 也需要這些，則應在此處加入。
    # 例如：
    # if params.get("LOGGING_MODE") == "除錯模式":
    # args.append("--verbose")

    return args

def build_taifex_load_args(validated_params): # 改用 validated_params
    # 待辦事項：根據實際下游腳本的需要，精確調整參數名稱和格式
    args = []

    # 處理布林型旗標參數
    if validated_params.get("FORCE_DATA_REFRESH", False):
        args.append("--force-data-refresh")

    # 同上，根據 taifex_data_pipeline/run.py 的實際需要傳遞參數
    # if params.get("LOGGING_MODE") == "除錯模式":
    # args.append("--debug-level", "high")

    # E2E 測試傳入的資料庫路徑參數
    if validated_params.get("DB_PATH_LOGS"): # ELT 過程可能也需要記錄日誌
        args.extend(["--db-path-logs", validated_params["DB_PATH_LOGS"]])
    if validated_params.get("DB_PATH_RAW_TAIFEX"):
        args.extend(["--db-path-raw-taifex", validated_params["DB_PATH_RAW_TAIFEX"]])
    if validated_params.get("DB_PATH_TAIFEX_HISTORICAL"):
        args.extend(["--db-path-taifex-historical", validated_params["DB_PATH_TAIFEX_HISTORICAL"]])

    # ELT 流程可能還需要輸入檔案路徑等
    if validated_params.get("ELT_INPUT_FILE_PATH"):
        args.extend(["--input-file-path", validated_params["ELT_INPUT_FILE_PATH"]])
    if validated_params.get("ELT_PIPELINE_STEP"): # 用於區分 ELT 的不同階段
        args.extend(["--pipeline-step", validated_params["ELT_PIPELINE_STEP"]])

    return args

def build_streaming_test_args(validated_params):
    """為串流測試腳本建構參數。"""
    args = []
    # 從 validated_params 中獲取 STREAMING_TEST_EXIT_CODE 和 STREAMING_TEST_MESSAGE_COUNT
    # 並將它們轉換為 --exit-code 和 --message-count
    # 這些參數名是 mission_params JSON 中的鍵名
    if validated_params.get("STREAMING_TEST_EXIT_CODE") is not None: # 檢查 None 以允許 0
        args.extend(["--exit-code", str(validated_params["STREAMING_TEST_EXIT_CODE"])])
    if validated_params.get("STREAMING_TEST_MESSAGE_COUNT") is not None: # 檢查 None
        args.extend(["--message-count", str(validated_params["STREAMING_TEST_MESSAGE_COUNT"])])
    if validated_params.get("DB_PATH_LOGS"): # 新增
        args.extend(["--db-path-logs", validated_params["DB_PATH_LOGS"]])
    if validated_params.get("STREAMING_TEST_NUM_HW_LOGS") is not None: # 新增
        args.extend(["--num-hw-logs", str(validated_params["STREAMING_TEST_NUM_HW_LOGS"])])
    if validated_params.get("STREAMING_TEST_HW_LOG_INTERVAL") is not None: # 新增
        args.extend(["--hw-log-interval", str(validated_params["STREAMING_TEST_HW_LOG_INTERVAL"])])
    return args

def main():
    parser = argparse.ArgumentParser(description="任務路由器，接收並分派後端任務。")
    parser.add_argument("--mission-params", required=True, help="Base64 編碼的 JSON 字串，包含所有任務參數。")
    args = parser.parse_args()

    try:
        params_json = base64.b64decode(args.mission_params).decode('utf-8')
        mission_params = json.loads(params_json)
    except (json.JSONDecodeError, UnicodeDecodeError, base64.binascii.Error) as e:
        print(f"[MISSION_RUNNER_ERROR] 指令包解析失敗: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 參數驗證與預設值設定 ---
    validated_params = {}

    # EXECUTION_MODE
    validated_params["EXECUTION_MODE"] = mission_params.get("EXECUTION_MODE", "標準分析流程")

    # REPOSITORY_URL (必要參數，若無則報錯)
    if "REPOSITORY_URL" not in mission_params or not mission_params["REPOSITORY_URL"]:
        print(f"[MISSION_RUNNER_ERROR] 必要參數 'REPOSITORY_URL' 未提供或為空。", file=sys.stderr)
        sys.exit(1)
    validated_params["REPOSITORY_URL"] = mission_params["REPOSITORY_URL"]

    # TARGET_BRANCH (必要參數，若無則報錯)
    if "TARGET_BRANCH" not in mission_params or not mission_params["TARGET_BRANCH"]:
        print(f"[MISSION_RUNNER_ERROR] 必要參數 'TARGET_BRANCH' 未提供或為空。", file=sys.stderr)
        sys.exit(1)
    validated_params["TARGET_BRANCH"] = mission_params["TARGET_BRANCH"]

    # ANALYSIS_TICKERS
    analysis_tickers_input = mission_params.get("ANALYSIS_TICKERS")
    if analysis_tickers_input is None or analysis_tickers_input == "": # 明確檢查 None 或空字串
        validated_params["ANALYSIS_TICKERS"] = "NQ=F,ES=F,^VIX" # 後端套用預設值
        print(f"[MISSION_RUNNER_INFO] 'ANALYSIS_TICKERS' 未提供或為空，使用預設值: {validated_params['ANALYSIS_TICKERS']}", file=sys.stderr)
    else:
        validated_params["ANALYSIS_TICKERS"] = analysis_tickers_input

    # ANALYSIS_START_DATE (暫不設預設值，格式由下游服務處理或後續增強)
    validated_params["ANALYSIS_START_DATE"] = mission_params.get("ANALYSIS_START_DATE")

    # ANALYSIS_END_DATE (暫不設預設值)
    validated_params["ANALYSIS_END_DATE"] = mission_params.get("ANALYSIS_END_DATE")

    # FORCE_REPO_REFRESH (bool, 預設 False)
    force_repo_refresh = mission_params.get("FORCE_REPO_REFRESH", False)
    if not isinstance(force_repo_refresh, bool):
        print(f"[MISSION_RUNNER_WARNING] 'FORCE_REPO_REFRESH' 應為布林值，收到 {type(force_repo_refresh)}:'{force_repo_refresh}'。將使用預設值 False。", file=sys.stderr)
        validated_params["FORCE_REPO_REFRESH"] = False
    else:
        validated_params["FORCE_REPO_REFRESH"] = force_repo_refresh

    # FORCE_DATA_REFRESH (bool, 預設 False)
    force_data_refresh = mission_params.get("FORCE_DATA_REFRESH", False)
    if not isinstance(force_data_refresh, bool):
        print(f"[MISSION_RUNNER_WARNING] 'FORCE_DATA_REFRESH' 應為布林值，收到 {type(force_data_refresh)}:'{force_data_refresh}'。將使用預設值 False。", file=sys.stderr)
        validated_params["FORCE_DATA_REFRESH"] = False
    else:
        validated_params["FORCE_DATA_REFRESH"] = force_data_refresh

    # LOGGING_MODE
    logging_mode_input = mission_params.get("LOGGING_MODE", "標準模式")
    # 允許的模式列表，可以根據實際需求擴展
    allowed_logging_modes = ["標準模式", "除錯模式", "詳細模式"]
    if logging_mode_input not in allowed_logging_modes:
        print(f"[MISSION_RUNNER_WARNING] 'LOGGING_MODE' 值 '{logging_mode_input}' 未知或不被支援。將使用預設值 '標準模式'。", file=sys.stderr)
        validated_params["LOGGING_MODE"] = "標準模式"
    else:
        validated_params["LOGGING_MODE"] = logging_mode_input

    # 串流測試模式特定參數 (如果 EXECUTION_MODE 是 "串流測試模式")
    if validated_params["EXECUTION_MODE"] == "串流測試模式":
        validated_params["STREAMING_TEST_EXIT_CODE"] = mission_params.get("STREAMING_TEST_EXIT_CODE", 0)
        validated_params["STREAMING_TEST_MESSAGE_COUNT"] = mission_params.get("STREAMING_TEST_MESSAGE_COUNT", 3)
        validated_params["DB_PATH_LOGS"] = mission_params.get("DB_PATH_LOGS") # 也給串流測試用
        validated_params["STREAMING_TEST_NUM_HW_LOGS"] = mission_params.get("STREAMING_TEST_NUM_HW_LOGS", 0)
        validated_params["STREAMING_TEST_HW_LOG_INTERVAL"] = mission_params.get("STREAMING_TEST_HW_LOG_INTERVAL", 0.05)
    elif validated_params["EXECUTION_MODE"] == "ELT第一階段：載入":
        # 這些參數是 E2E 測試傳入的，用於告知虛假腳本如何操作
        validated_params["DB_PATH_LOGS"] = mission_params.get("DB_PATH_LOGS")
        validated_params["DB_PATH_RAW_TAIFEX"] = mission_params.get("DB_PATH_RAW_TAIFEX")
        validated_params["DB_PATH_TAIFEX_HISTORICAL"] = mission_params.get("DB_PATH_TAIFEX_HISTORICAL")
        validated_params["ELT_INPUT_FILE_PATH"] = mission_params.get("ELT_INPUT_FILE_PATH")
        validated_params["ELT_PIPELINE_STEP"] = mission_params.get("ELT_PIPELINE_STEP") # 例如 "load" 或 "transform"
    elif validated_params["EXECUTION_MODE"] == "標準分析流程":
        # 這些參數是 E2E 測試傳入的
        validated_params["DB_PATH_LOGS"] = mission_params.get("DB_PATH_LOGS")
        validated_params["DB_PATH_YFINANCE_CACHE"] = mission_params.get("DB_PATH_YFINANCE_CACHE")

    # 處理 FORCE_REPO_REFRESH 的日誌 (如果為 True)
    if validated_params.get("FORCE_REPO_REFRESH", False):
        # 實際的 git 操作應該在這裡或專用函數中執行，這裡僅為 E2E 測試添加日誌點
        print("[MISSION_RUNNER_INFO] FORCE_REPO_REFRESH=True，模擬執行倉庫刷新操作（例如移除舊目錄）。", file=sys.stderr)

    # --- 路由邏輯，使用 validated_params ---
    execution_mode = validated_params["EXECUTION_MODE"]

    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    base_apps_dir = os.path.dirname(current_script_dir)

    if execution_mode == "標準分析流程":
        target_script_name = "daily_market_analyzer/run.py"
        cmd_args = build_analyzer_args(validated_params)
    elif execution_mode == "ELT第一階段：載入":
        target_script_name = "taifex_data_pipeline/run.py"
        cmd_args = build_taifex_load_args(validated_params)
    elif execution_mode == "串流測試模式": # 新增的 EXECUTION_MODE
        target_script_name = "fake_streaming_script/run.py"
        # validated_params 應包含 STREAMING_TEST_EXIT_CODE, STREAMING_TEST_MESSAGE_COUNT
        # 這些參數需要在 validated_params 賦值區塊中從 mission_params 讀取並放入 validated_params
        cmd_args = build_streaming_test_args(validated_params)
    else:
        print(f"[MISSION_RUNNER_ERROR] 未知的執行模式: {execution_mode}", file=sys.stderr)
        sys.exit(1)

    target_script_path = os.path.join(base_apps_dir, target_script_name)

    # 這裡僅檢查檔案是否存在，實際下游腳本是否可執行或有內部錯誤由 subprocess 處理
    if not os.path.isfile(target_script_path): # 更精確地檢查是否為檔案
        print(f"[MISSION_RUNNER_ERROR] 找不到目標腳本檔案或非檔案: {target_script_path}", file=sys.stderr)
        sys.exit(1)

    # =================================================================
    #               「戰神之究極」標準執行核心 v1.0
    # =================================================================
    try:
        # 構建最終指令
        final_cmd = [sys.executable, target_script_path] + cmd_args

        # 在除錯模式下，可以打印更多關於 validated_params 的信息 (移到指令打印前)
        if validated_params.get("LOGGING_MODE") in ["除錯模式", "詳細模式"]: # 使用 .get 以防 LOGGING_MODE 意外缺失
            print(f"[MISSION_RUNNER_DEBUG] 最終驗證參數: {validated_params}", file=sys.stderr, flush=True)

        print(f"\n[MISSION_RUNNER_EXECUTE] 正在向最終作戰單位下達指令...", flush=True)
        print(f"[CMD] {' '.join(final_cmd)}", flush=True)
        print("-" * 25, "下游服務即時戰報開始", "-" * 25, flush=True)

        # **核心：使用 Popen 創建子進程，這是確保流式輸出的唯一標準模式**
        process = subprocess.Popen(
            final_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # 合併 stdout 和 stderr
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        # **核心：實時監聽並打印子進程的每一行輸出**
        if process.stdout: # 檢查 stdout 是否有效
            for line in iter(process.stdout.readline, ''):
                sys.stdout.write(line) # 直接寫入，避免 print 額外的換行 (除非 line 本身有)
                sys.stdout.flush()
            process.stdout.close() # 完成讀取後關閉

        # **核心：阻塞式等待子進程完全結束，並獲取最終的返回碼**
        process.wait()
        final_return_code = process.returncode

        print("-" * 25, "下游服務即時戰報結束", "-" * 25, flush=True)

        # **核心：根據下游服務的真實返回碼，決定自身的成敗**
        if final_return_code != 0:
            print(f"\n[MISSION_RUNNER_FAILURE] 下游任務 '{target_script_path}' 執行失敗，返回碼: {final_return_code}", file=sys.stderr, flush=True)
            sys.exit(final_return_code)
        else:
            # 只有當 final_return_code 為 0 時才打印成功訊息
            print(f"\n[MISSION_RUNNER_SUCCESS] 下游任務成功完成。", flush=True)

    except FileNotFoundError: # 特指 final_cmd 中的可執行文件找不到
        print(f"\n[MISSION_RUNNER_CRITICAL] 指令執行失敗：找不到目標腳本 '{target_script_path}' 或 Python 解釋器 '{sys.executable}'。", file=sys.stderr, flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"\n[MISSION_RUNNER_CRITICAL] 執行子進程 '{target_script_path}' 時發生未知致命錯誤: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
