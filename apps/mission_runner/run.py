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
    else:
        print(f"[MISSION_RUNNER_ERROR] 未知的執行模式: {execution_mode}", file=sys.stderr)
        sys.exit(1)

    target_script_path = os.path.join(base_apps_dir, target_script_name)

    # 這裡僅檢查檔案是否存在，實際下游腳本是否可執行或有內部錯誤由 subprocess 處理
    if not os.path.isfile(target_script_path): # 更精確地檢查是否為檔案
        print(f"[MISSION_RUNNER_ERROR] 找不到目標腳本檔案或非檔案: {target_script_path}", file=sys.stderr)
        sys.exit(1)

    try:
        final_cmd = [sys.executable, target_script_path] + cmd_args
        # 在除錯模式下，可以打印更多關於 validated_params 的信息
        if validated_params["LOGGING_MODE"] in ["除錯模式", "詳細模式"]:
            print(f"[MISSION_RUNNER_DEBUG] 最終驗證參數: {validated_params}", file=sys.stderr)
        print(f"[MISSION_RUNNER_INFO] 準備執行指令: {' '.join(final_cmd)}", file=sys.stderr)

        process = subprocess.Popen(final_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')

        # 即時讀取 stdout
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                sys.stdout.write(line)
                sys.stdout.flush()
            process.stdout.close()

        # 即時讀取 stderr
        if process.stderr:
            for line in iter(process.stderr.readline, ''): # 修正此處，之前誤寫為 process.stdout.readline
                sys.stderr.write(line)
                sys.stderr.flush()
            process.stderr.close()

        return_code = process.wait()

        if return_code != 0:
            print(f"[MISSION_RUNNER_ERROR] 子腳本 {target_script_path} 執行失敗，返回碼: {return_code}", file=sys.stderr)
            sys.exit(return_code)

    except Exception as e:
        # 捕獲更廣泛的異常，例如 Popen 構造時的錯誤
        print(f"[MISSION_RUNNER_ERROR] 執行子腳本 {target_script_path} 時發生未預期錯誤: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
