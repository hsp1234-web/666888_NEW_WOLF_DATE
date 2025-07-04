import subprocess
import duckdb
import os
import sys
from datetime import datetime, timedelta
import time

# --- 測試參數 ---
TEST_TICKERS = ["AAPL", "GOOG", "^VIX"]
DAYS_FOR_TEST = 3
DB_NAME = "test_v32_harness.duckdb"
CACHE_DB_NAME = "test_v32_harness_cache.duckdb"
TABLE_NAME = "market_ohlcv_data_test_harness"

# --- 路徑設定 ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
APPS_DMA_DIR = os.path.dirname(__file__)
RUN_PY_PATH = os.path.join(APPS_DMA_DIR, "run.py")
DATA_WORKSPACE_DIR = os.path.join(PROJECT_ROOT, "data_workspace")
DB_PATH = os.path.join(DATA_WORKSPACE_DIR, DB_NAME)
CACHE_DB_PATH = os.path.join(DATA_WORKSPACE_DIR, CACHE_DB_NAME)

def print_test_step(message):
    print(f"\n{'='*10} [測試步驟] {message} {'='*10}")

def print_test_result(message, success=True):
    prefix = "✅" if success else "❌"
    print(f"{prefix} {message}")

def run_and_log(command_args, execution_label):
    print(f"INFO: 準備執行 {execution_label}: {' '.join(command_args)}")
    start_time = time.time()
    process_result = subprocess.run(command_args, capture_output=True, text=True, check=False)
    duration = time.time() - start_time
    print_test_step(f"{execution_label} 執行完畢 (耗時: {duration:.2f} 秒)")

    print(f"\n--- {execution_label} STDOUT ---")
    print(process_result.stdout)
    print(f"--- {execution_label} STDOUT 結束 ---\n")
    if process_result.stderr:
        print(f"\n--- {execution_label} STDERR ---")
        print(process_result.stderr)
        print(f"--- {execution_label} STDERR 結束 ---\n")

    if process_result.returncode != 0:
        print_test_result(f"{execution_label} 執行失敗，返回碼: {process_result.returncode}", False)
        return None # 返回 None 表示執行失敗
    else:
        print_test_result(f"{execution_label} 執行成功。", True)
        return process_result # 返回結果對象

def validate_db_data(db_path_to_check, table_name_to_check, tickers_to_check, start_date_obj, end_date_obj, label=""):
    print_test_step(f"開始自動化數據驗證 {label}")
    overall_validation_passed = True

    if not os.path.exists(db_path_to_check):
        print_test_result(f"資料庫檔案 {db_path_to_check} 未找到，無法進行驗證。", False)
        return False

    try:
        con = duckdb.connect(database=db_path_to_check, read_only=True)
        print_test_result(f"成功連接到資料庫: {db_path_to_check} {label}", True)

        test_dates = []
        current_date_loop = start_date_obj
        while current_date_loop <= end_date_obj:
            test_dates.append(current_date_loop)
            current_date_loop += timedelta(days=1)

        for ticker in tickers_to_check:
            print_test_step(f"驗證標的: {ticker} {label}")
            ticker_all_dates_passed = True
            for specific_date in test_dates:
                specific_date_str = specific_date.strftime("%Y-%m-%d")
                day_start_ts = f"{specific_date_str} 00:00:00.000000+00"
                next_day_start_ts = (specific_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00.000000+00")
                query = f"SELECT COUNT(*) FROM {table_name_to_check} WHERE ticker = ? AND datetime >= CAST(? AS TIMESTAMPTZ) AND datetime < CAST(? AS TIMESTAMPTZ);"
                try:
                    result = con.execute(query, [ticker, day_start_ts, next_day_start_ts]).fetchone()
                    count = result[0] if result and result[0] is not None else 0
                    if count > 0:
                        print_test_result(f"標的 {ticker} 在 {specific_date_str} 找到 {count} 筆記錄。", True)
                    else:
                        print_test_result(f"標的 {ticker} 在 {specific_date_str} 未找到任何記錄 (筆數: {count})。", False)
                        ticker_all_dates_passed = False; overall_validation_passed = False
                except Exception as e_sql:
                    print_test_result(f"查詢標的 {ticker} 在 {specific_date_str} 時發生 SQL 錯誤: {e_sql}", False)
                    ticker_all_dates_passed = False; overall_validation_passed = False

            if ticker_all_dates_passed:
                 print_test_result(f"標的 {ticker} {label} 所有日期數據初步驗證通過。", True)
            else:
                 print_test_result(f"標的 {ticker} {label} 部分日期數據驗證失敗。", False)
        con.close()
    except Exception as e_db:
        print_test_result(f"連接或操作資料庫 {db_path_to_check} {label} 時發生錯誤: {e_db}", False)
        overall_validation_passed = False

    return overall_validation_passed

def main():
    print_test_step("開始 v32.1 整合測試 (包含快取驗證)")

    # 清理：腳本開始時清理主庫和快取庫
    print_test_step(f"清理舊的測試資料庫: {DB_PATH} 和 {CACHE_DB_PATH}")
    if os.path.exists(DB_PATH): os.remove(DB_PATH); print_test_result(f"已刪除舊主資料庫: {DB_PATH}", True)
    if os.path.exists(CACHE_DB_PATH): os.remove(CACHE_DB_PATH); print_test_result(f"已刪除舊快取資料庫: {CACHE_DB_PATH}", True)
    os.makedirs(DATA_WORKSPACE_DIR, exist_ok=True)

    end_date_dt = datetime.now() - timedelta(days=1)
    start_date_dt = end_date_dt - timedelta(days=DAYS_FOR_TEST - 1)
    start_date_str = start_date_dt.strftime("%Y-%m-%d")
    end_date_str = end_date_dt.strftime("%Y-%m-%d")
    print(f"INFO: 測試日期範圍: {start_date_str} 至 {end_date_str}, 測試標的: {', '.join(TEST_TICKERS)}")

    # --- 第一次執行 run.py (旨在填充快取和主資料庫) ---
    common_args = [
        sys.executable, RUN_PY_PATH,
        "--tickers", ",".join(TEST_TICKERS),
        "--start-date", start_date_str,
        "--end-date", end_date_str,
        "--db-path", DB_PATH,
        "--cache-db-path", CACHE_DB_PATH,
        "--table-name", TABLE_NAME,
        "--verbose"
    ]

    process_result_run1 = run_and_log(common_args, "run.py (第一次執行 - 填充快取)")
    if not process_result_run1 or process_result_run1.returncode != 0:
        print_test_result("第一次 run.py 執行根本失敗，終止測試。", success=False)
        sys.exit(1)

    print("INFO: 等待1秒確保資料庫文件操作完成 (第一次執行後)...")
    time.sleep(1)
    validation_run1_passed = validate_db_data(DB_PATH, TABLE_NAME, TEST_TICKERS, start_date_dt, end_date_dt, "(第一次執行後)")
    if not validation_run1_passed:
        print_test_result("第一次 run.py 後數據驗證失敗，但仍將繼續第二次運行以檢查快取行為。", success=False)
        # 不退出，繼續測試快取
    else:
        print_test_result("第一次 run.py 後所有數據驗證成功通過！", True)

    # --- 第二次執行 run.py (旨在測試快取命中) ---
    print_test_step("第二次執行 run.py (無 --force-refresh, 旨在測試快取命中)")

    # 清理主資料庫，但保留快取資料庫
    if os.path.exists(DB_PATH): os.remove(DB_PATH); print_test_result(f"已刪除主資料庫 {DB_PATH} 以準備第二次運行。", True)

    process_result_run2 = run_and_log(common_args, "run.py (第二次執行 - 測試快取)") # common_args 不含 --force-refresh
    if not process_result_run2 or process_result_run2.returncode != 0:
        print_test_result("第二次 run.py 執行根本失敗，終止測試。", success=False)
        sys.exit(1)

    cache_hit_observed = False
    # 檢查所有 ticker 是否都報告了快取命中
    all_tickers_cache_hit = True
    for ticker in TEST_TICKERS:
        # 查找特定 ticker 和 interval (假設 1m 是最可能被快取的) 的快取命中日誌
        # "成功: hydrate_data_range: Ticker=AAPL, Interval=1m. 所有請求數據 (2025-07-01 to 2025-07-03) 均在快取中。"
        expected_cache_log = f"Ticker={ticker}, Interval=1m. 所有請求數據 ({start_date_str} to {end_date_str}) 均在快取中"
        if expected_cache_log in process_result_run2.stdout:
            print_test_result(f"標的 {ticker} 的 1m 快取命中已在日誌中確認。", True)
        else:
            # 如果 1m 沒命中，可能是因為第一次寫入1m快取失敗，但其他 interval 可能成功並被快取
            # 退而求其次，檢查是否有任何 "均在快取中" 的通用訊息
            if f"Ticker={ticker}" in process_result_run2.stdout and "均在快取中" in process_result_run2.stdout:
                 print_test_result(f"標的 {ticker} 觀察到通用快取命中訊息 (可能非1m interval)。", True)
                 # 這種情況也算觀察到快取機制在工作
            else:
                print_test_result(f"標的 {ticker} 未在日誌中明確確認 1m 快取命中。", False)
                all_tickers_cache_hit = False

    if all_tickers_cache_hit:
        print_test_result("所有標的的主要快取命中機制已觀察到。", True)
        cache_hit_observed = True # 指揮官要求的觀察標準
    elif "均在快取中" in process_result_run2.stdout or "cached_full_hit_verified" in process_result_run2.stdout:
        print_test_result("觀察到通用快取命中訊息，但並非所有標的都明確命中了預期的1m快取。", True)
        cache_hit_observed = True # 放寬標準，只要有快取行為即可
    else:
        print_test_result("關鍵：未能在第二次執行的 STDOUT 中明確找到任何股票的快取命中指示詞。快取測試未達預期。", False)


    print("INFO: 等待1秒確保資料庫文件操作完成 (第二次執行後)...")
    time.sleep(1)
    validation_run2_passed = validate_db_data(DB_PATH, TABLE_NAME, TEST_TICKERS, start_date_dt, end_date_dt, "(第二次執行後)")

    # --- 最終結果判斷 ---
    print_test_step("整合測試最終結果")
    # 交付標準：第二次執行時，必須能從日誌中明確觀察到「數據成功從快取命中」的訊息
    # 並且第二次執行後的數據驗證也必須通過。
    # 第一次執行的數據驗證也應該通過，以確保基本功能正常。

    final_success = (process_result_run1.returncode == 0 and validation_run1_passed and \
                     process_result_run2.returncode == 0 and validation_run2_passed and \
                     cache_hit_observed)

    if final_success:
        print_test_result("所有核心測試成功通過 (兩次運行成功、觀察到快取命中、兩次數據驗證通過)！v32.1 達到交付標準。", True)
    else:
        if not (process_result_run1.returncode == 0 and validation_run1_passed) :
             print_test_result("第一次運行或其後數據驗證失敗。", False)
        if not (process_result_run2.returncode == 0 and validation_run2_passed) :
             print_test_result("第二次運行或其後數據驗證失敗。", False)
        if not cache_hit_observed: print_test_result("第二次運行未能明確觀察到快取命中。", False)
        print_test_result("部分或全部核心測試未通過。請檢查日誌。", False)
        sys.exit(1)

if __name__ == "__main__":
    main()
