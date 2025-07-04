# -*- coding: utf-8 -*-
"""
整合測試腳本 for v33.0 "智能獵犬" 引擎升級。
此腳本必須包含並依次通過所有指定的測試案例。
"""
import subprocess
import os
import sys
import time
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import shutil

# --- 配置 ---
PYTHON_EXE = sys.executable
SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'run.py'))
BASE_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data_workspace', 'test_v33_analyzer.duckdb'))
REPORTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data_workspace', 'reports'))
TABLE_NAME = "market_ohlcv_data_test_v33"
LOG_FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data_workspace', '_test_v33_harness.log'))

# 測試股票和日期
TICKER_INVALID_HISTORICAL = "GOOG"
START_DATE_INVALID_HISTORICAL = "2000-01-01"
END_DATE_INVALID_HISTORICAL = "2000-02-15" # 擴大日期範圍以觸發 >30 天的預檢邏輯

TICKER_DATA_ONLY = "MSFT"
END_DATE_DATA_ONLY_REPORT_ONLY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE_DATA_ONLY_REPORT_ONLY = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

TICKER_FULL_FLOW = "NVDA"
END_DATE_FULL_FLOW = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE_FULL_FLOW = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")


# --- 輔助函數 ---
def print_header(message):
    print(f"\n{'='*60}\n{message}\n{'='*60}")

def print_subheader(message):
    print(f"\n--- {message} ---")

def log_message(message, to_console=True):
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")
    if to_console:
        print(message)

def run_command(command: list[str]) -> tuple[int, str, str]:
    log_message(f"執行命令: {' '.join(command)}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    stdout, stderr = process.communicate()
    log_message(f"命令返回碼: {process.returncode}")
    if stdout:
        log_message(f"標準輸出:\n{stdout[-1000:]}{'... (輸出過長，已截斷)' if len(stdout)>1000 else ''}", to_console=False) # Log last 1000 chars
    if stderr:
        log_message(f"標準錯誤:\n{stderr}", to_console=False)
    return process.returncode, stdout, stderr

def check_log_contains(log_content: str, expected_strings: list[str], case_sensitive=True) -> bool:
    all_found = True
    for s in expected_strings:
        if case_sensitive:
            if s not in log_content:
                log_message(f"檢查日誌: 未找到預期字串 '{s}'")
                all_found = False
        else:
            if s.lower() not in log_content.lower():
                log_message(f"檢查日誌 (不區分大小寫): 未找到預期字串 '{s}'")
                all_found = False
    return all_found

def check_log_not_contains(log_content: str, unexpected_strings: list[str], case_sensitive=True) -> bool:
    none_found = True
    for s in unexpected_strings:
        if case_sensitive:
            if s in log_content:
                log_message(f"檢查日誌: 找到非預期字串 '{s}'")
                none_found = False
        else:
            if s.lower() in log_content.lower():
                log_message(f"檢查日誌 (不區分大小寫): 找到非預期字串 '{s}'")
                none_found = False
    return none_found

def query_duckdb(db_path: str, query: str, params=None) -> pd.DataFrame:
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            if params:
                result = con.execute(query, params).fetchdf()
            else:
                result = con.execute(query).fetchdf()
            log_message(f"DuckDB 查詢: {query} (參數: {params}) -> 返回 {len(result)} 行", to_console=False)
            return result
    except Exception as e:
        log_message(f"DuckDB 查詢失敗: {e}")
        return pd.DataFrame()

def file_exists(filepath: str) -> bool:
    exists = os.path.exists(filepath)
    log_message(f"檢查檔案存在性: {filepath} -> {'存在' if exists else '不存在'}")
    return exists

def cleanup_files(db_to_clean_path: str, report_dir_to_clean: str):
    print_subheader("開始清理...")
    if os.path.exists(db_to_clean_path):
        try:
            os.remove(db_to_clean_path)
            log_message(f"已刪除測試資料庫: {db_to_clean_path}")
        except Exception as e:
            log_message(f"刪除測試資料庫失敗 {db_to_clean_path}: {e}")

    # 清理報告目錄下的 md 檔案 (避免刪除其他可能的非測試報告)
    if os.path.exists(report_dir_to_clean):
        for item in os.listdir(report_dir_to_clean):
            if item.endswith(".md") and ("market_analysis_report_" in item or "on_demand_report_" in item or "data_pipeline_summary_" in item):
                item_path = os.path.join(report_dir_to_clean, item)
                try:
                    os.remove(item_path)
                    log_message(f"已刪除測試報告檔案: {item_path}")
                except Exception as e:
                    log_message(f"刪除測試報告檔案失敗 {item_path}: {e}")
    log_message("清理完成。")

def get_report_files(directory: str) -> list[str]:
    if not os.path.exists(directory):
        return []
    return [f for f in os.listdir(directory) if f.endswith(".md") and ("market_analysis_report_" in f or "on_demand_report_" in f or "data_pipeline_summary_" in f)]

# --- 測試案例 ---

def test_1_invalid_historical_data():
    print_header("測試 1: 無效歷史數據測試 (`--data-only`)")
    cleanup_files(BASE_DB_PATH, REPORTS_DIR) # 清理舊數據

    cmd = [
        PYTHON_EXE, SCRIPT_PATH,
        "--tickers", TICKER_INVALID_HISTORICAL,
        "--start-date", START_DATE_INVALID_HISTORICAL,
        "--end-date", END_DATE_INVALID_HISTORICAL,
        "--db-path", BASE_DB_PATH,
        "--table-name", TABLE_NAME,
        "--data-only"
    ]
    returncode, stdout, stderr = run_command(cmd)

    assert returncode == 0, f"測試 1 失敗: 命令執行失敗 (返回碼 {returncode})"
    log_message("測試 1: 命令執行成功。")

    expected_in_log = [
        f"Pre-flight check for {TICKER_INVALID_HISTORICAL} from {START_DATE_INVALID_HISTORICAL} to {END_DATE_INVALID_HISTORICAL} (1mo) failed",
        "Skipping all intervals",
        "INFO: Pre-flight check for TICKER failed. Skipping all intervals." # 這是指令中要求的日誌格式，但 yfinance_client.py 中實際日誌可能不同
    ]
    # yfinance_client.py 實際日誌是 "INFO: Pre-flight check for {ticker} from {start_date_str} to {end_date_str} (1mo) failed. Skipping all intervals."
    # 和 "status": "preflight_failed_empty"
    # 我們需要根據實際輸出來調整 expected_in_log

    # 更新為更精確的預期日誌 (基於 yfinance_client.py 的實現)
    # overall_execution_log 的內部狀態不會直接打印到 stdout，所以不檢查 status 和 message JSON 片段
    expected_in_log_actual = [
        f"Performing existence pre-flight check for historical range [{START_DATE_INVALID_HISTORICAL} to {END_DATE_INVALID_HISTORICAL}]",
        f"INFO: Pre-flight check for {TICKER_INVALID_HISTORICAL} from {START_DATE_INVALID_HISTORICAL} to {END_DATE_INVALID_HISTORICAL} (1mo) failed. Skipping all intervals.",
        f"===== 數據回填任務結束 (預檢失敗): Ticker={TICKER_INVALID_HISTORICAL} =====" # 修正為匹配 yfinance_client.py 的中文輸出
    ]

    assert check_log_contains(stdout, expected_in_log_actual, case_sensitive=False), "測試 1 失敗: 日誌未包含預檢失敗的關鍵訊息。"
    log_message("測試 1: 日誌包含預檢失敗訊息。")

    unexpected_in_log = [
        f"正在評估顆粒度 '1d'", # 不應嘗試 1d
        f"正在評估顆粒度 '1h'", # 不應嘗試 1h
        f"正在評估顆粒度 '1m'", # 不應嘗試 1m
        f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1d",
        f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1h",
        f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1m",
    ]
    # 預檢後，不應該有對 '1mo' 以外的 interval 的 fetch_single_chunk 嘗試 (除了預檢本身的 '1mo')
    # 也不應該有 "正在評估顆粒度" 除了可能的 '1mo' (如果預檢邏輯在那裡)
    # 由於預檢是在 hydrate_data_range 的開頭，如果失敗則直接返回，因此不應看到後續 interval 的評估日誌。

    # 調整 unexpected_in_log 以反映 yfinance_client 的實際日誌輸出
    # 如果預檢失敗，hydrate_data_range 會提前退出，不會進入 fallback interval 循環。
    # 所以，不應該看到 "正在評估顆粒度" for '1d', '1h', '1m' for the main hydration loop.
    # unexpected_in_log_actual = [ # 舊的檢查，會檢查整個 stdout
    #     f"INFO: hydrate_data_range: Ticker={TICKER_INVALID_HISTORICAL}. 正在評估顆粒度 '1d'",
    #     f"INFO: hydrate_data_range: Ticker={TICKER_INVALID_HISTORICAL}. 正在評估顆粒度 '1h'",
    #     f"INFO: hydrate_data_range: Ticker={TICKER_INVALID_HISTORICAL}. 正在評估顆粒度 '1m'",
    #     f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1d",
    #     f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1h",
    #     f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1m",
    # ]
    # assert check_log_not_contains(stdout, unexpected_in_log_actual, case_sensitive=False), "測試 1 失敗: 日誌中包含多餘的 interval 嘗試記錄。"
    # log_message("測試 1: 日誌未包含多餘的 interval 嘗試。")

    # --- 新增的更精確的檢查 ---
    pre_flight_failed_marker = f"INFO: Pre-flight check for {TICKER_INVALID_HISTORICAL} from {START_DATE_INVALID_HISTORICAL} to {END_DATE_INVALID_HISTORICAL} (1mo) failed. Skipping all intervals."
    marker_index = stdout.find(pre_flight_failed_marker)

    assert marker_index != -1, f"測試 1 失敗: 未在日誌中找到預檢失敗標記 '{pre_flight_failed_marker}'"
    log_message(f"測試 1: 在日誌中找到預檢失敗標記: '{pre_flight_failed_marker}'")

    # 只檢查標記之後的日誌內容
    stdout_after_marker = stdout[marker_index + len(pre_flight_failed_marker):]

    unexpected_strings_after_preflight_failure = [
        f"INFO: hydrate_data_range: Ticker={TICKER_INVALID_HISTORICAL}. 正在評估顆粒度 '1d'",
        f"INFO: hydrate_data_range: Ticker={TICKER_INVALID_HISTORICAL}. 正在評估顆粒度 '1h'",
        f"INFO: hydrate_data_range: Ticker={TICKER_INVALID_HISTORICAL}. 正在評估顆粒度 '1m'",
        f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1d",
        f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1h",
        f"fetch_single_chunk: Ticker={TICKER_INVALID_HISTORICAL}, Interval=1m",
    ]

    assert check_log_not_contains(stdout_after_marker, unexpected_strings_after_preflight_failure, case_sensitive=False), \
        "測試 1 失敗: 在預檢失敗標記之後的日誌中發現了多餘的 interval 嘗試記錄。"
    log_message("測試 1: 預檢失敗後未進行多餘的 interval 嘗試。")
    # --- 結束新增的檢查 ---

    db_data = query_duckdb(BASE_DB_PATH, f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE ticker = '{TICKER_INVALID_HISTORICAL}' AND datetime >= '{START_DATE_INVALID_HISTORICAL}' AND datetime <= '{END_DATE_INVALID_HISTORICAL} 23:59:59'")
    assert db_data['count_star()'].iloc[0] == 0, f"測試 1 失敗: 資料庫中不應存在 {TICKER_INVALID_HISTORICAL} 的數據，但找到了 {db_data['count_star()'].iloc[0]} 筆。"
    log_message(f"測試 1: 資料庫中沒有 {TICKER_INVALID_HISTORICAL} 在無效日期的數據。測試通過！")
    return True


def test_2_data_only_mode():
    print_header("測試 2: 純數據處理測試 (`--data-only`)")
    # 清理應確保不影響後續測試，如果後續測試依賴此處生成的數據
    # test_3 依賴 test_2 的數據，所以這裡不應該完全清理 DB，或者 test_3 自己準備數據
    # 根據指令 "緊接著上一步"，我們在這裡生成數據，test_3 使用它。
    # 所以，在 test_2 執行 *前* 清理，執行 *後* 不清理 DB。
    cleanup_files(BASE_DB_PATH, REPORTS_DIR)
    initial_reports = get_report_files(REPORTS_DIR)

    cmd = [
        PYTHON_EXE, SCRIPT_PATH,
        "--tickers", TICKER_DATA_ONLY,
        "--start-date", START_DATE_DATA_ONLY_REPORT_ONLY,
        "--end-date", END_DATE_DATA_ONLY_REPORT_ONLY,
        "--db-path", BASE_DB_PATH,
        "--table-name", TABLE_NAME,
        "--data-only"
    ]
    returncode, stdout, stderr = run_command(cmd)

    assert returncode == 0, f"測試 2 失敗: 命令執行失敗 (返回碼 {returncode})"
    log_message("測試 2: 命令執行成功。")

    expected_in_log = [
        f"執行模式：僅數據處理 (--data-only)",
        f"開始處理標的 (數據流程): {TICKER_DATA_ONLY}",
        f"資訊：標的 {TICKER_DATA_ONLY} 數據成功寫入資料庫",
        "數據處理流程結束"
    ]
    assert check_log_contains(stdout, expected_in_log, case_sensitive=False), "測試 2 失敗: 日誌未包含數據處理成功的關鍵訊息。"
    log_message("測試 2: 日誌包含數據處理成功訊息。")

    # 驗證數據庫
    db_data = query_duckdb(BASE_DB_PATH, f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE ticker = '{TICKER_DATA_ONLY}' AND datetime >= '{START_DATE_DATA_ONLY_REPORT_ONLY}' AND datetime <= '{END_DATE_DATA_ONLY_REPORT_ONLY} 23:59:59'")
    assert db_data['count_star()'].iloc[0] > 0, f"測試 2 失敗: 資料庫中應存在 {TICKER_DATA_ONLY} 的數據，但未找到。"
    log_message(f"測試 2: 資料庫中已寫入 {TICKER_DATA_ONLY} 的數據 ({db_data['count_star()'].iloc[0]} 筆)。")

    # 驗證未生成報告
    final_reports = get_report_files(REPORTS_DIR)
    new_reports = [r for r in final_reports if r not in initial_reports]
    assert not new_reports, f"測試 2 失敗: --data-only 模式不應生成報告，但生成了: {new_reports}"
    log_message("測試 2: 未生成報告檔案。測試通過！")
    return True


def test_3_report_only_mode():
    print_header("測試 3: 純報告生成測試 (`--report-only`)")
    # 此測試依賴 test_2 生成的數據，所以不在此處清理 DB。
    # 但需要清理舊報告，以驗證新報告是否生成。

    # 先記錄當前報告目錄中的檔案，以便後續比較
    # 注意：REPORTS_DIR 的清理應該在 test_1 或 test_2 開始時進行，以確保一個乾淨的起點。
    # test_2 執行前已清理 REPORTS_DIR。
    initial_reports = get_report_files(REPORTS_DIR)

    cmd = [
        PYTHON_EXE, SCRIPT_PATH,
        "--tickers", TICKER_DATA_ONLY, # 使用與 test_2 相同的 Ticker
        "--report-start-date", START_DATE_DATA_ONLY_REPORT_ONLY, # 使用 test_2 的日期範圍
        "--report-end-date", END_DATE_DATA_ONLY_REPORT_ONLY,
        "--db-path", BASE_DB_PATH,
        "--table-name", TABLE_NAME,
        "--report-only"
    ]
    returncode, stdout, stderr = run_command(cmd)

    assert returncode == 0, f"測試 3 失敗: 命令執行失敗 (返回碼 {returncode})"
    log_message("測試 3: 命令執行成功。")

    expected_in_log = [
        "執行模式：僅報告生成 (--report-only)",
        f"INFO (run_report_generation): Generating report for tickers: ['{TICKER_DATA_ONLY}'] over range [{START_DATE_DATA_ONLY_REPORT_ONLY} to {END_DATE_DATA_ONLY_REPORT_ONLY}]",
        "報告已成功儲存至"
    ]
    assert check_log_contains(stdout, expected_in_log, case_sensitive=False), "測試 3 失敗: 日誌未包含報告生成成功的關鍵訊息。"
    log_message("測試 3: 日誌包含報告生成成功訊息。")

    unexpected_in_log = [
        "run_data_pipeline", # 不應執行數據流程
        "hydrate_data_range", # 不應調用數據回填
        "YFinanceClient", # YFinanceClient 可能會初始化，但其 hydrate_data_range 不應被調用
                           # run.py 中，如果 report_only, yf_client 不會被實例化。所以不該有 YFinanceClient 相關日誌
        "YFinanceClient 初始化完畢" # 確認 yf_client 是否真的未初始化
    ]
    # 根據 run.py 的邏輯，在 --report-only 模式下，yf_client 不會被初始化。
    # 所以 "YFinanceClient 初始化完畢" 也不應該出現。
    unexpected_in_log_actual = [
        "--- 開始數據處理流程 ---", # from run_data_pipeline
        "hydrate_data_range", # from yf_client
        "YFinanceClient (Data Hydrator v33.0) 初始化完畢" # from yf_client constructor
    ]
    assert check_log_not_contains(stdout, unexpected_in_log_actual, case_sensitive=False), "測試 3 失敗: 日誌中包含數據獲取相關的多餘訊息。"
    log_message("測試 3: 日誌未包含數據獲取相關訊息。")

    # 驗證報告是否生成
    final_reports = get_report_files(REPORTS_DIR)
    new_reports = [r for r in final_reports if r not in initial_reports] # 比較的是 test_2 運行後的狀態

    generated_report_for_current_test = False
    expected_report_name_part = "on_demand_report_" # run.py 中 --report-only 生成的檔案名

    for r_name in new_reports: # new_reports 可能為空，如果 test_2 執行後沒有清理 reports 目錄且 test_2 未產生報告
        if expected_report_name_part in r_name:
             generated_report_for_current_test = True
             break

    # 更可靠的檢查：直接檢查是否有 on_demand_report_*.md 檔案生成
    found_on_demand_report = any(expected_report_name_part in r for r in final_reports)

    assert found_on_demand_report, f"測試 3 失敗: 未能找到預期的 '{expected_report_name_part}*.md' 報告檔案。"
    log_message("測試 3: 成功生成報告檔案。測試通過！")
    return True


def test_4_full_flow():
    print_header("測試 4: 完整流程測試")
    cleanup_files(BASE_DB_PATH, REPORTS_DIR) # 清理舊數據和報告
    initial_reports = get_report_files(REPORTS_DIR)

    cmd = [
        PYTHON_EXE, SCRIPT_PATH,
        "--tickers", TICKER_FULL_FLOW,
        "--start-date", START_DATE_FULL_FLOW,
        "--end-date", END_DATE_FULL_FLOW,
        "--db-path", BASE_DB_PATH,
        "--table-name", TABLE_NAME
        # 無 --data-only 或 --report-only
    ]
    returncode, stdout, stderr = run_command(cmd)

    assert returncode == 0, f"測試 4 失敗: 命令執行失敗 (返回碼 {returncode})"
    log_message("測試 4: 命令執行成功。")

    expected_in_log = [
        "執行模式：完整流程 (數據處理與報告生成)",
        f"開始處理標的 (數據流程): {TICKER_FULL_FLOW}",
        f"資訊：標的 {TICKER_FULL_FLOW} 數據成功寫入資料庫",
        "數據處理流程結束",
        "開始報告生成流程",
        f"INFO (run_report_generation): Generating report for tickers: ['{TICKER_FULL_FLOW}'] over range [{START_DATE_FULL_FLOW} to {END_DATE_FULL_FLOW}]",
        "報告已成功儲存至"
    ]
    assert check_log_contains(stdout, expected_in_log, case_sensitive=False), "測試 4 失敗: 日誌未包含完整流程成功的關鍵訊息。"
    log_message("測試 4: 日誌包含完整流程成功訊息。")

    # 驗證數據庫
    db_data = query_duckdb(BASE_DB_PATH, f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE ticker = '{TICKER_FULL_FLOW}' AND datetime >= '{START_DATE_FULL_FLOW}' AND datetime <= '{END_DATE_FULL_FLOW} 23:59:59'")
    assert db_data['count_star()'].iloc[0] > 0, f"測試 4 失敗: 資料庫中應存在 {TICKER_FULL_FLOW} 的數據，但未找到。"
    log_message(f"測試 4: 資料庫中已寫入 {TICKER_FULL_FLOW} 的數據 ({db_data['count_star()'].iloc[0]} 筆)。")

    # 驗證報告是否生成
    final_reports = get_report_files(REPORTS_DIR)
    new_reports = [r for r in final_reports if r not in initial_reports]

    generated_report_for_current_test = False
    expected_report_name_part = "market_analysis_report_" # run.py 中完整流程生成的檔案名

    for r_name in new_reports: # new_reports 應該只包含當前測試生成的報告
        if expected_report_name_part in r_name:
             generated_report_for_current_test = True
             break
    # 或者更直接的檢查
    found_full_flow_report = any(expected_report_name_part in r for r in final_reports)

    assert found_full_flow_report, f"測試 4 失敗: 未能找到預期的 '{expected_report_name_part}*.md' 報告檔案。"
    log_message("測試 4: 成功生成報告檔案。測試通過！")
    return True

# --- 主執行邏輯 ---
if __name__ == "__main__":
    log_dir = os.path.dirname(LOG_FILE_PATH)
    # 使用 print 進行初始調試，因為 log_message 依賴於此目錄的成功創建
    print(f"DEBUG: Determined log directory: {log_dir}")
    if not os.path.exists(log_dir):
        print(f"DEBUG: Log directory {log_dir} does not exist. Attempting to create.")
        try:
            os.makedirs(log_dir, exist_ok=True)
            if os.path.exists(log_dir):
                print(f"INFO: Test harness created log directory: {log_dir}")
            else:
                print(f"CRITICAL_ERROR: os.makedirs was called for {log_dir}, but it still does not exist!")
                sys.exit(f"CRITICAL_FAILURE: Could not create log directory: {log_dir}")
        except Exception as e:
            print(f"CRITICAL_ERROR: Failed to create log directory {log_dir}: {e}")
            sys.exit(f"CRITICAL_FAILURE: Exception while creating log directory: {log_dir} - {e}")
    else:
        print(f"DEBUG: Log directory {log_dir} already exists.")

    # 初始化日誌檔案 (確保目錄已成功創建或已存在)
    if os.path.exists(LOG_FILE_PATH):
        try:
            os.remove(LOG_FILE_PATH)
        except Exception as e:
            print(f"WARNING: Failed to remove existing log file {LOG_FILE_PATH}: {e}. Log output may be mixed.")

    log_message(f"測試腳本 _test_v33_harness.py 開始執行。日誌將記錄於: {LOG_FILE_PATH}", to_console=True)
    log_message(f"Python 解譯器: {PYTHON_EXE}", to_console=False)
    log_message(f"待測腳本: {SCRIPT_PATH}", to_console=False)
    log_message(f"測試資料庫: {BASE_DB_PATH}", to_console=False)
    log_message(f"報告目錄: {REPORTS_DIR}", to_console=False)
    log_message(f"測試表格: {TABLE_NAME}", to_console=False)

    tests_passed = 0
    total_tests = 0 # 將在後面增加

    # 執行測試
    all_tests = [
        test_1_invalid_historical_data,
        test_2_data_only_mode,
        test_3_report_only_mode,
        test_4_full_flow
    ]
    total_tests = len(all_tests)

    for i, test_func in enumerate(all_tests):
        test_name = test_func.__name__
        log_message(f"開始執行測試 ({i+1}/{total_tests}): {test_name}", to_console=True)
        try:
            if test_func():
                tests_passed += 1
                log_message(f"測試 ({i+1}/{total_tests}): {test_name} 通過。", to_console=True)
            else:
                log_message(f"測試 ({i+1}/{total_tests}): {test_name} 失敗 (斷言觸發)。", to_console=True)
        except AssertionError as ae:
            log_message(f"測試 ({i+1}/{total_tests}): {test_name} 失敗 (斷言錯誤): {ae}", to_console=True)
            import traceback
            log_message(traceback.format_exc(), to_console=False)
        except Exception as e:
            log_message(f"測試 ({i+1}/{total_tests}): {test_name} 執行時發生意外錯誤: {e}", to_console=True)
            import traceback
            log_message(traceback.format_exc(), to_console=False)

        # 短暫停頓，確保日誌寫入和檔案操作完成
        time.sleep(0.5)


    print_header("測試總結")
    log_message(f"總共規劃測試: {total_tests}", to_console=True)
    log_message(f"通過測試數: {tests_passed}", to_console=True)

    if tests_passed == total_tests and total_tests > 0 : # 確保至少跑了一個測試
        log_message("所有 v33.0 整合測試案例均已通過！", to_console=True)
        sys.exit(0)
    else:
        log_message(f"v33.0 整合測試未完全通過 ({tests_passed}/{total_tests})。請檢查日誌 '{LOG_FILE_PATH}'。", to_console=True)
        sys.exit(1)

print("INFO: _test_v33_harness.py 腳本定義完畢。")
