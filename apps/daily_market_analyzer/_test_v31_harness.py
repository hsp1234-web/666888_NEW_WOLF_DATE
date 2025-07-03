# -*- coding: utf-8 -*-
"""
v31.0 「智能獵犬」引擎 - 強制性交付前整合驗收測試腳本

本腳本旨在驗證 v31.0 升級中引入的核心功能：
1.  數據獲取策略優化 - 「智能考古」
    - 存在性預檢：對遙遠歷史數據，應能快速判定不存在並跳過。
    - 時間感知回溯：歷史數據應使用逆轉降級策略。
2.  執行流程解耦 - 「模組化執行」
    - --data-only：僅處理數據，不生成報告。
    - --report-only：僅生成報告，不處理數據。
3.  系統效能與資源利用 - 「動態資源壓榨」
    - DuckDB 記憶體限制應能正確設定 (間接驗證，主要看日誌和是否成功運行)。

驗收標準：此腳本必須完整、無錯誤地跑完所有測試案例。
"""
import subprocess
import os
import sys
import shutil
from datetime import datetime, timedelta
import time
import duckdb # 用於直接檢查資料庫內容

# --- 配置區 ---
PYTHON_EXECUTABLE = sys.executable # 使用當前 Python 解釋器
RUN_PY_SCRIPT = os.path.join(os.path.dirname(__file__), "run.py") # apps/daily_market_analyzer/run.py 的路徑

# 測試用的股票代碼和日期
TICKER_INVALID_HISTORICAL = "GOOG" # Google
DATE_INVALID_HISTORICAL_START = "2000-01-01"
DATE_INVALID_HISTORICAL_END = "2000-01-03" # 短範圍以快速測試預檢

TICKER_VALID_RECENT_1 = "AAPL" # Apple Inc.
# 近期的小範圍日期，確保 yfinance 有數據
# 執行測試前，請確保以下日期範圍對於 TICKER_VALID_RECENT_1 是有效的，並且有數據
# 避免選擇週末或市場休市日作為唯一日期
TODAY = datetime.now()
VALID_RECENT_END_DT = TODAY - timedelta(days=2) # 確保數據已落定，避開當天或前一天
VALID_RECENT_START_DT = VALID_RECENT_END_DT - timedelta(days=2) # 3天的小範圍
DATE_VALID_RECENT_1_START = VALID_RECENT_START_DT.strftime("%Y-%m-%d")
DATE_VALID_RECENT_1_END = VALID_RECENT_END_DT.strftime("%Y-%m-%d")

TICKER_VALID_RECENT_2 = "MSFT" # Microsoft Corp.
VALID_RECENT_2_END_DT = TODAY - timedelta(days=3)
VALID_RECENT_2_START_DT = VALID_RECENT_2_END_DT - timedelta(days=1) # 2天的小範圍
DATE_VALID_RECENT_2_START = VALID_RECENT_2_START_DT.strftime("%Y-%m-%d")
DATE_VALID_RECENT_2_END = VALID_RECENT_2_END_DT.strftime("%Y-%m-%d")


# 測試資料庫和報告路徑
BASE_TEST_WORKSPACE = os.path.join(os.path.dirname(__file__), "test_workspace_v31")
DB_PATH_CASE_1 = os.path.join(BASE_TEST_WORKSPACE, "db", "test_case1_invalid_hist.duckdb")
DB_PATH_CASE_2 = os.path.join(BASE_TEST_WORKSPACE, "db", "test_case2_data_only.duckdb")
# Case 3 使用與 Case 2 相同的 DB
DB_PATH_CASE_4 = os.path.join(BASE_TEST_WORKSPACE, "db", "test_case4_full_flow.duckdb")

CACHE_DB_PATH_GENERAL = os.path.join(BASE_TEST_WORKSPACE, "cache", "test_cache.duckdb")
REPORTS_DIR_GENERAL = os.path.join(BASE_TEST_WORKSPACE, "reports") # run.py 預設會在此基礎上再創建 reports 子目錄

TABLE_NAME = "market_ohlcv_data_test_v31"

# --- 輔助函數 ---
def print_test_header(case_name: str):
    print("\n" + "="*80)
    print(f"🧪 開始測試案例: {case_name}")
    print("="*80)

def print_test_footer(case_name: str, success: bool):
    status = "✅ 通過" if success else "❌ 失敗"
    print("\n" + "-"*80)
    print(f"{status} - 測試案例: {case_name}")
    print("-"*80 + "\n")
    if not success:
        print("🔥🔥🔥 驗收測試未通過，請檢查錯誤！ 🔥🔥🔥")
        sys.exit(1) # 如果任何測試失敗，則終止腳本

def run_command(command: list[str], timeout_seconds=300) -> tuple[bool, str, str]:
    """執行命令並返回成功狀態、標準輸出和標準錯誤。"""
    print(f"🚀 執行命令: {' '.join(command)}")
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        success = process.returncode == 0
        if not success:
            print(f"⚠️ 命令執行失敗 (返回碼: {process.returncode})")
            print(f"   STDOUT:\n{stdout}")
            print(f"   STDERR:\n{stderr}")
        return success, stdout, stderr
    except subprocess.TimeoutExpired:
        print(f"⏰ 命令執行超時 ({timeout_seconds} 秒)")
        return False, "", "TimeoutExpired"
    except Exception as e:
        print(f"💥 命令執行時發生例外: {e}")
        return False, "", str(e)

def cleanup_workspace():
    """清理測試工作區。"""
    if os.path.exists(BASE_TEST_WORKSPACE):
        print(f"🧹 清理測試工作區: {BASE_TEST_WORKSPACE}")
        shutil.rmtree(BASE_TEST_WORKSPACE)
    os.makedirs(os.path.join(BASE_TEST_WORKSPACE, "db"), exist_ok=True)
    os.makedirs(os.path.join(BASE_TEST_WORKSPACE, "cache"), exist_ok=True)
    # run.py 會自動創建 data_workspace/reports，但這裡我們用自己的 reports 目錄
    os.makedirs(REPORTS_DIR_GENERAL, exist_ok=True)


def count_files_in_dir(directory: str, extension: str = ".md") -> int:
    """計算目錄中指定副檔名的檔案數量。"""
    if not os.path.exists(directory):
        return 0
    return len([f for f in os.listdir(directory) if f.endswith(extension) and os.path.isfile(os.path.join(directory, f))])

def check_db_data(db_path: str, table_name: str, ticker: str, start_date: str, end_date: str) -> bool:
    """檢查資料庫中是否存在指定 ticker 和日期範圍的數據。"""
    if not os.path.exists(db_path):
        print(f"DB檢查: 資料庫檔案不存在 {db_path}")
        return False
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            query = f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE ticker = ? AND datetime >= CAST(? AS TIMESTAMPTZ) AND datetime < CAST(? AS TIMESTAMPTZ)
            """
            # 日期需要轉換為 datetime 物件以包含時間部分
            start_dt_query = datetime.strptime(start_date, "%Y-%m-%d")
            # DuckDB 的 < CAST(? AS TIMESTAMPTZ) 是不包含的，所以 end_date 要加一天
            end_dt_query = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

            count = con.execute(query, [ticker, start_dt_query, end_dt_query]).fetchone()[0]
            print(f"DB檢查: 在 {db_path} 的 {table_name} 中找到 {ticker} ({start_date} to {end_date}) 的數據 {count} 筆。")
            return count > 0
    except Exception as e:
        print(f"DB檢查: 查詢資料庫 {db_path} 時發生錯誤: {e}")
        return False

# --- 測試案例 ---

def test_case_1_invalid_historical_data():
    case_name = "無效歷史數據測試 (存在性預檢)"
    print_test_header(case_name)
    success = False

    cmd = [
        PYTHON_EXECUTABLE, RUN_PY_SCRIPT,
        "--data-only",
        "--tickers", TICKER_INVALID_HISTORICAL,
        "--start-date", DATE_INVALID_HISTORICAL_START,
        "--end-date", DATE_INVALID_HISTORICAL_END,
        "--db-path", DB_PATH_CASE_1,
        "--cache-db-path", CACHE_DB_PATH_GENERAL, # 提供快取路徑
        "--table-name", TABLE_NAME,
        "--verbose" # 開啟詳細日誌以檢查輸出
    ]

    run_ok, stdout, stderr = run_command(cmd, timeout_seconds=60) # 預期很快完成

    if run_ok:
        # 1. 驗證系統能利用「存在性預檢」快速完成 (通過日誌判斷)
        if "preflight_no_data" in stdout or "預檢無數據" in stdout :
            print("✅ 日誌中檢測到 'preflight_no_data' 或 '預檢無數據'，符合預期。")
            # 2. 不產生大量無效的 API 請求日誌
            #    (間接驗證，如果預檢生效，就不會有後續 interval 的嘗試)
            #    可以檢查是否包含 '評估顆粒度' (evaluating interval) '1d' for GOOG 2000 的日誌，不應該有
            if f"評估顆粒度 '1d' for range [{DATE_INVALID_HISTORICAL_START} to {DATE_INVALID_HISTORICAL_END}]" not in stdout and \
               f"evaluating interval '1d' for range [{DATE_INVALID_HISTORICAL_START} to {DATE_INVALID_HISTORICAL_END}]" not in stdout:
                print("✅ 日誌中未發現對 '1d' 等細顆粒度的嘗試，符合預檢邏輯。")
                success = True
            else:
                print("❌ 日誌中發現了對 '1d' 等細顆粒度的嘗試，預檢可能未生效。")
                print(f"STDOUT:\n{stdout}")
        else:
            print("❌ 日誌中未檢測到 'preflight_no_data' 或 '預檢無數據'。")
            print(f"STDOUT:\n{stdout}")
    else:
        print(f"❌ {case_name} 命令執行失敗。")

    print_test_footer(case_name, success)
    return success

def test_case_2_data_only_processing():
    case_name = "純數據處理測試 (--data-only)"
    print_test_header(case_name)
    success = False

    # 清理可能存在的舊報告
    # run.py 報告輸出到 data_workspace/reports/
    # 但我們在此測試腳本中檢查的是 REPORTS_DIR_GENERAL
    # 為了確保，我們可以手動指定 run.py 的報告輸出目錄到我們可控的地方，或者依賴其預設路徑並檢查
    # 這裡假設 run.py 的報告目錄是固定的，並且我們在每次測試前清理 BASE_TEST_WORKSPACE/reports
    # run.py 中的 ReportGenerator 寫入 "data_workspace/reports"
    # 為確保隔離，我們應該讓 run.py 的報告寫到測試工作區內
    # 目前 run.py 的報告路徑是硬編碼的，這在測試中不理想。
    # 暫時策略：檢查預設的 "data_workspace/reports" 是否有新文件，並在測試開始前記錄其狀態。
    # 更好的策略是讓 run.py 接受 --reports-output-dir 參數。由於目前沒有，我們採取權宜之計。

    # 記錄測試前報告目錄中的檔案數量
    # run.py 的報告輸出路徑是 'data_workspace/reports'，我們需要檢查這個路徑
    # 但為了測試腳本的獨立性，我們將檢查 REPORTS_DIR_GENERAL，並假設 run.py 的報告也被導向這裡
    # (這需要 run.py 支持 --report-output-dir，目前沒有，所以這個檢查可能不準確)
    # 暫時跳過報告目錄檢查的精確性，重點放在DB數據。
    # 理想情況下，應該修改 run.py 讓報告目錄可配置。

    # 為了測試，我們將 run.py 的報告目錄暫時指向測試工作區
    # 這通過修改 run.py 內部的 report_output_dir 變量實現（如果可以）
    # 或者，我們假設在測試環境中，data_workspace 就是 BASE_TEST_WORKSPACE
    # 這裡我們專注於DB的驗證

    reports_dir_to_check = os.path.join(os.path.dirname(__file__), "..", "..", "data_workspace", "reports") # run.py 的實際報告路徑
    os.makedirs(reports_dir_to_check, exist_ok=True) # 確保存在
    initial_report_count = count_files_in_dir(reports_dir_to_check)
    print(f"INFO: 測試前，'{reports_dir_to_check}' 中有 {initial_report_count} 份報告。")


    cmd = [
        PYTHON_EXECUTABLE, RUN_PY_SCRIPT,
        "--data-only",
        "--tickers", TICKER_VALID_RECENT_1,
        "--start-date", DATE_VALID_RECENT_1_START,
        "--end-date", DATE_VALID_RECENT_1_END,
        "--db-path", DB_PATH_CASE_2,
        "--cache-db-path", CACHE_DB_PATH_GENERAL,
        "--table-name", TABLE_NAME,
        "--verbose"
    ]

    run_ok, stdout, stderr = run_command(cmd)

    if run_ok:
        # 1. 驗證數據被成功寫入主資料庫
        db_has_data = check_db_data(DB_PATH_CASE_2, TABLE_NAME, TICKER_VALID_RECENT_1, DATE_VALID_RECENT_1_START, DATE_VALID_RECENT_1_END)
        if db_has_data:
            print(f"✅ 資料庫 {DB_PATH_CASE_2} 中成功寫入 {TICKER_VALID_RECENT_1} 的數據。")

            # 2. 驗證未生成任何報告檔案
            time.sleep(1) # 等待檔案系統操作完成
            final_report_count = count_files_in_dir(reports_dir_to_check)
            print(f"INFO: 測試後，'{reports_dir_to_check}' 中有 {final_report_count} 份報告。")
            if final_report_count == initial_report_count:
                print("✅ 未生成新的報告檔案，符合 --data-only 模式預期。")
                success = True
            else:
                print(f"❌ 生成了新的報告檔案 (之前 {initial_report_count}, 現在 {final_report_count})，與 --data-only 模式不符。")
                # 列出新增的檔案以供調試
                # (這部分邏輯較複雜，暫時省略)
        else:
            print(f"❌ 資料庫 {DB_PATH_CASE_2} 中未找到 {TICKER_VALID_RECENT_1} 的數據。")
            print(f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}")

    else:
        print(f"❌ {case_name} 命令執行失敗。")

    print_test_footer(case_name, success)
    return success

def test_case_3_report_only_generation():
    case_name = "純報告生成測試 (--report-only)"
    print_test_header(case_name)
    success = False

    reports_dir_to_check = os.path.join(os.path.dirname(__file__), "..", "..", "data_workspace", "reports")
    os.makedirs(reports_dir_to_check, exist_ok=True)
    initial_report_count = count_files_in_dir(reports_dir_to_check)
    print(f"INFO: 測試前，'{reports_dir_to_check}' 中有 {initial_report_count} 份報告。")

    cmd = [
        PYTHON_EXECUTABLE, RUN_PY_SCRIPT,
        "--report-only",
        "--tickers", TICKER_VALID_RECENT_1, # 使用與 case 2 相同的 ticker
        "--report-start-date", DATE_VALID_RECENT_1_START, # 使用與 case 2 相同的日期範圍
        "--report-end-date", DATE_VALID_RECENT_1_END,
        "--db-path", DB_PATH_CASE_2, # 使用 case 2 生成的資料庫
        "--table-name", TABLE_NAME,
        "--verbose"
    ]

    run_ok, stdout, stderr = run_command(cmd)

    if run_ok:
        # 1. 驗證報告能被成功生成
        time.sleep(1)
        final_report_count = count_files_in_dir(reports_dir_to_check)
        print(f"INFO: 測試後，'{reports_dir_to_check}' 中有 {final_report_count} 份報告。")
        if final_report_count > initial_report_count:
            print(f"✅ 成功生成了新的報告檔案 (之前 {initial_report_count}, 現在 {final_report_count})。")

            # 2. 驗證日誌中沒有任何數據獲取 (hydrate_data_range) 的相關訊息
            if "hydrate_data_range" not in stdout and "數據回填任務" not in stdout :
                print("✅ 日誌中未發現 'hydrate_data_range' 或 '數據回填任務' 相關訊息，符合 --report-only 模式預期。")
                success = True
            else:
                print("❌ 日誌中包含了 'hydrate_data_range' 或 '數據回填任務' 相關訊息，與 --report-only 模式不符。")
                print(f"STDOUT snippet containing 'hydrate':\n{stdout[stdout.find('hydrate_data_range')-50 : stdout.find('hydrate_data_range')+100]}")
        else:
            print("❌ 未能生成新的報告檔案。")
            print(f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
    else:
        print(f"❌ {case_name} 命令執行失敗。")

    print_test_footer(case_name, success)
    return success

def test_case_4_full_flow():
    case_name = "完整流程測試 (無開關)"
    print_test_header(case_name)
    success = False

    reports_dir_to_check = os.path.join(os.path.dirname(__file__), "..", "..", "data_workspace", "reports")
    os.makedirs(reports_dir_to_check, exist_ok=True)
    initial_report_count = count_files_in_dir(reports_dir_to_check)
    print(f"INFO: 測試前，'{reports_dir_to_check}' 中有 {initial_report_count} 份報告。")

    cmd = [
        PYTHON_EXECUTABLE, RUN_PY_SCRIPT,
        # 無 --data-only 或 --report-only
        "--tickers", TICKER_VALID_RECENT_2,
        "--start-date", DATE_VALID_RECENT_2_START,
        "--end-date", DATE_VALID_RECENT_2_END,
        "--db-path", DB_PATH_CASE_4,
        "--cache-db-path", CACHE_DB_PATH_GENERAL,
        "--table-name", TABLE_NAME,
        "--verbose"
    ]

    run_ok, stdout, stderr = run_command(cmd)

    if run_ok:
        # 1. 驗證數據被成功寫入主資料庫
        db_has_data = check_db_data(DB_PATH_CASE_4, TABLE_NAME, TICKER_VALID_RECENT_2, DATE_VALID_RECENT_2_START, DATE_VALID_RECENT_2_END)
        if db_has_data:
            print(f"✅ 資料庫 {DB_PATH_CASE_4} 中成功寫入 {TICKER_VALID_RECENT_2} 的數據。")

            # 2. 驗證報告也被成功生成
            time.sleep(1)
            final_report_count = count_files_in_dir(reports_dir_to_check)
            print(f"INFO: 測試後，'{reports_dir_to_check}' 中有 {final_report_count} 份報告。")
            if final_report_count > initial_report_count:
                print("✅ 成功生成了新的報告檔案。")
                success = True
            else:
                print("❌ 未能生成新的報告檔案。")
        else:
            print(f"❌ 資料庫 {DB_PATH_CASE_4} 中未找到 {TICKER_VALID_RECENT_2} 的數據。")
            print(f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
    else:
        print(f"❌ {case_name} 命令執行失敗。")

    print_test_footer(case_name, success)
    return success

# --- 主執行邏輯 ---
def run_all_tests():
    """按順序執行所有測試案例。"""
    print("🚀🚀🚀 開始 v31.0 「智能獵犬」引擎整合驗收測試 🚀🚀🚀")

    # 確保測試環境乾淨
    cleanup_workspace()

    # 依序執行測試
    results = []
    results.append(test_case_1_invalid_historical_data())
    results.append(test_case_2_data_only_processing())
    results.append(test_case_3_report_only_generation())
    results.append(test_case_4_full_flow())

    # 最終總結
    if all(results):
        print("\n🎉🎉🎉 所有 v31.0 整合驗收測試案例均已通過！引擎達到可交付狀態。 🎉🎉🎉")
    else:
        failed_count = len([r for r in results if not r])
        print(f"\n💔💔💔 {failed_count} 個 v31.0 整合驗收測試案例失敗。引擎未達到可交付狀態。 💔💔💔")
        sys.exit(1) # 強制退出，標記失敗

if __name__ == "__main__":
    # 確保 run.py 可執行
    if not os.path.isfile(RUN_PY_SCRIPT):
        print(f"錯誤: 找不到 run.py 腳本於 '{RUN_PY_SCRIPT}'。請確保路徑正確。")
        sys.exit(1)

    run_all_tests()
