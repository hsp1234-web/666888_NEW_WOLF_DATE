import subprocess
import duckdb
import os
import sys
from datetime import datetime, timedelta
import time

# --- 測試參數 ---
TEST_TICKERS = ["AAPL", "GOOG", "^VIX"] # 加入波動率指數作為特性標的
DAYS_FOR_TEST = 3 # 測試數據的天數
DB_NAME = "test_v32_harness.duckdb"
CACHE_DB_NAME = "test_v32_harness_cache.duckdb"
TABLE_NAME = "market_ohlcv_data_test_harness"

# --- 路徑設定 ---
# 假設此腳本位於 apps/daily_market_analyzer/
# run.py 也位於此目錄
# 資料庫將創建在 data_workspace/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
APPS_DMA_DIR = os.path.dirname(__file__)
RUN_PY_PATH = os.path.join(APPS_DMA_DIR, "run.py")
DATA_WORKSPACE_DIR = os.path.join(PROJECT_ROOT, "data_workspace") # 修正路徑
DB_PATH = os.path.join(DATA_WORKSPACE_DIR, DB_NAME)
CACHE_DB_PATH = os.path.join(DATA_WORKSPACE_DIR, CACHE_DB_NAME)

def print_test_step(message):
    print(f"\n{'='*10} [測試步驟] {message} {'='*10}")

def print_test_result(message, success=True):
    prefix = "✅" if success else "❌"
    print(f"{prefix} {message}")

def main():
    print_test_step("開始 v32.0 整合測試")

    # 1. 清理舊的測試資料庫 (如果存在)
    print_test_step(f"清理舊的測試資料庫: {DB_PATH} 和 {CACHE_DB_PATH}")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print_test_result(f"已刪除舊資料庫: {DB_PATH}", True)
    if os.path.exists(CACHE_DB_PATH):
        os.remove(CACHE_DB_PATH)
        print_test_result(f"已刪除舊快取資料庫: {CACHE_DB_PATH}", True)

    # 確保 data_workspace 目錄存在
    os.makedirs(DATA_WORKSPACE_DIR, exist_ok=True)

    # 2. 設定日期範圍 (最近的N天)
    end_date = datetime.now() - timedelta(days=1) # 結束日期為昨天，確保數據已穩定
    start_date = end_date - timedelta(days=DAYS_FOR_TEST - 1)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    print_test_step(f"測試日期範圍: {start_date_str} 至 {end_date_str}")
    print_test_step(f"測試標的: {', '.join(TEST_TICKERS)}")

    # 3. 構造 run.py 命令
    # 使用 sys.executable 確保用同一個 Python 解釋器執行
    command = [
        sys.executable, RUN_PY_PATH,
        "--tickers", ",".join(TEST_TICKERS),
        "--start-date", start_date_str,
        "--end-date", end_date_str,
        "--db-path", DB_PATH,
        "--cache-db-path", CACHE_DB_PATH,
        "--table-name", TABLE_NAME,
        "--force-refresh", # 強制刷新以測試完整數據獲取路徑
        "--verbose"      # 啟用詳細日誌
    ]
    print_test_step(f"準備執行 run.py: {' '.join(command)}")

    # 4. 執行 run.py
    start_time_run_py = time.time()
    # try:
    process_result = subprocess.run(command, capture_output=True, text=True, check=False)
    # except FileNotFoundError:
    #     print_test_result(f"錯誤: 無法找到 run.py 或 Python 解釋器。請檢查路徑: {RUN_PY_PATH}, {sys.executable}", False)
    #     sys.exit(1)

    end_time_run_py = time.time()
    run_py_duration = end_time_run_py - start_time_run_py
    print_test_step(f"run.py 執行完畢 (耗時: {run_py_duration:.2f} 秒)")

    # 打印 run.py 的 stdout 和 stderr
    print("\n--- run.py STDOUT ---")
    print(process_result.stdout)
    print("--- run.py STDOUT 結束 ---\n")
    if process_result.stderr:
        print("\n--- run.py STDERR ---")
        print(process_result.stderr)
        print("--- run.py STDERR 結束 ---\n")

    if process_result.returncode != 0:
        print_test_result(f"run.py 執行失敗，返回碼: {process_result.returncode}", False)
        sys.exit(1)
    else:
        print_test_result("run.py 執行成功。", True)

    # 後續的數據庫驗證將在下一個計劃步驟中實現
    # print_test_step("整合測試腳本初步執行完成 (數據驗證待實現)")
    # print("\n請注意：此階段僅執行 run.py。數據庫內容的自動化驗證將在下一步驟中添加。")

    # 5. 自動化數據驗證
    print_test_step("開始自動化數據驗證")
    all_validations_passed = True

    if not os.path.exists(DB_PATH):
        print_test_result(f"資料庫檔案 {DB_PATH} 未找到，無法進行驗證。", False)
        sys.exit(1)

    try:
        con = duckdb.connect(database=DB_PATH, read_only=True) # 以唯讀模式連接
        print_test_result(f"成功連接到資料庫: {DB_PATH}", True)

        # 生成測試日期範圍內的每一天
        test_dates = []
        current_date_loop = start_date
        while current_date_loop <= end_date:
            test_dates.append(current_date_loop)
            current_date_loop += timedelta(days=1)

        for ticker_to_check in TEST_TICKERS:
            print_test_step(f"驗證標的: {ticker_to_check}")
            ticker_all_dates_passed = True
            for specific_date in test_dates:
                specific_date_str = specific_date.strftime("%Y-%m-%d")

                # 構造 SQL 查詢以計算特定股票在特定日期的記錄數
                # 由於 datetime 是 TIMESTAMPTZ，我們需要查詢一整天的範圍
                day_start_ts = f"{specific_date_str} 00:00:00.000000+00" # 假設 UTC
                next_day_start_ts = (specific_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00.000000+00")

                query = f"""
                SELECT COUNT(*)
                FROM {TABLE_NAME}
                WHERE ticker = ?
                  AND datetime >= CAST(? AS TIMESTAMPTZ)
                  AND datetime < CAST(? AS TIMESTAMPTZ);
                """

                try:
                    result = con.execute(query, [ticker_to_check, day_start_ts, next_day_start_ts]).fetchone()
                    if result and result[0] is not None:
                        count = result[0]
                        # 對於分鐘線數據，一天可能有很多筆；對於日線，一天一筆。
                        # ^VIX 通常是日線。 AAPL, GOOG 可能混合。
                        # 基本驗證：只要有數據 (count > 0) 就認為初步通過。
                        # 更精確的筆數驗證需要了解 yfinance 對這些標的在這些日期的具體數據提供情況。
                        if count > 0:
                            print_test_result(f"標的 {ticker_to_check} 在 {specific_date_str} 找到 {count} 筆記錄。", True)
                        else:
                            print_test_result(f"標的 {ticker_to_check} 在 {specific_date_str} 未找到任何記錄 (筆數: {count})。", False)
                            ticker_all_dates_passed = False
                            all_validations_passed = False
                    else:
                        print_test_result(f"查詢標的 {ticker_to_check} 在 {specific_date_str} 的數據時返回空結果。", False)
                        ticker_all_dates_passed = False
                        all_validations_passed = False
                except Exception as e_sql:
                    print_test_result(f"查詢標的 {ticker_to_check} 在 {specific_date_str} 時發生 SQL 錯誤: {e_sql}", False)
                    ticker_all_dates_passed = False
                    all_validations_passed = False

            if ticker_all_dates_passed:
                 print_test_result(f"標的 {ticker_to_check} 所有日期數據初步驗證通過。", True)
            else:
                 print_test_result(f"標的 {ticker_to_check} 部分日期數據驗證失敗。", False)

        con.close()

    except Exception as e_db:
        print_test_result(f"連接或操作資料庫 {DB_PATH} 時發生錯誤: {e_db}", False)
        all_validations_passed = False

    print_test_step("自動化數據驗證結束")
    if all_validations_passed:
        print_test_result("所有數據驗證成功通過！v32.0 達到交付標準。", True)
    else:
        print_test_result("部分或全部數據驗證失敗。請檢查日誌。", False)
        sys.exit(1) # 如果驗證失敗，以非0狀態碼退出

if __name__ == "__main__":
    main()
