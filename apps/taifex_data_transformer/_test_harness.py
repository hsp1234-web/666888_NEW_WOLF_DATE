# -*- coding: utf-8 -*-
# 整合測試腳本 v35.3 - 驗證數據轉換器及全鏈路流程 (含預翻譯)
import os
import sys
import subprocess
import shutil
import zipfile
import duckdb
import pandas as pd
from datetime import date

# --- 測試配置 ---
TEST_WORKSPACE = "/tmp/test_harness_v35_p3"
RAW_FILES_DIR = os.path.join(TEST_WORKSPACE, "raw_files_for_elt")
DB_DIR = os.path.join(TEST_WORKSPACE, "databases_elt")
METADATA_DB_PATH = os.path.join(DB_DIR, "pipeline_metadata.duckdb")
RAW_DB_PATH = os.path.join(DB_DIR, "raw_taifex.duckdb")
ANALYTICS_DB_PATH = os.path.join(DB_DIR, "taifex_historical.duckdb") # 修正為符合原始計畫書

# --- 自動尋找專案腳本路徑 ---
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    LOADER_SCRIPT = os.path.join(project_root, "apps", "taifex_data_pipeline", "run.py")
    TRANSFORMER_SCRIPT = os.path.join(project_root, "apps", "taifex_data_transformer", "run.py")
except Exception as e:
    print(f"路徑設定錯誤: {e}")
    sys.exit(1)


def print_header(title):
    print("\n" + "="*80)
    print(f"⚔️  {title}")
    print("="*80)

def setup_test_environment():
    """清理並建立一個包含虛假數據的乾淨測試環境"""
    print_header("1. 建立全鏈路測試環境")
    if os.path.exists(TEST_WORKSPACE):
        shutil.rmtree(TEST_WORKSPACE)
    os.makedirs(RAW_FILES_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)

    # 檔案一：ZIP 檔案，BIG5 編碼, 包含帶逗號的數字
    zip_path_A = os.path.join(RAW_FILES_DIR, "Options_20250102.zip")
    with zipfile.ZipFile(zip_path_A, 'w') as zf:
        csv_content = (
            "交易日期,契約代碼,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,,,,,,交易時段\n"
            "2025/01/02,TXO,202501W1,18000.0,Call,150.5,160,140,155,1025,155,500,,,,,,一般\n"
            "2025/01/02,TXO,202501W1,18100.0,Put,80,90.5,75,88,\"2,510\",88,\"800\",,,,,一般\n"
        ).encode('big5', errors='ignore')
        zf.writestr("Daily_2025_01_02.csv", csv_content)

    # 檔案二：單獨的 CSV 檔案，MS950 編碼 (與 BIG5 高度相容)
    csv_path_B = os.path.join(RAW_FILES_DIR, "Futures_20250103.csv")
    with open(csv_path_B, 'w', encoding='ms950') as f:
        # 統一表頭結構，包含選擇權和期貨都可能需要的欄位 (例如13個核心欄位，或按Options的18個補齊)
        # Options CSV 有18個欄位 (交易時段是第18個，中間有空的)
        # 我們讓期貨CSV也模擬這個結構，以便transformer的pandas.read_csv能一致處理
        f.write("交易日期,契約代碼,到期月份(週別),履約價,買賣權,開盤價,最高價,最低價,收盤價,成交量,結算價,未沖銷契約數,,,,,,交易時段\n") # 18個欄位名
        f.write("2025/01/03,MTX,202501,,期貨,17500,17510,17450,17480,\"3,000\",17480,\"1,500\",,,,,一般\n") # 數據行，履約價為空，買賣權為'期貨'

    print(f"✅ 測試環境 '{TEST_WORKSPACE}' 及虛假數據檔案建立完畢。")
    return [zip_path_A, csv_path_B]

def run_script(script_path, args, title):
    """執行指定的後端腳本"""
    print_header(title)
    cmd = [sys.executable, script_path] + args
    print(f"🚀 執行指令: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    print("--- STDOUT ---")
    print(result.stdout)
    if result.stderr:
        print("--- STDERR ---")
        print(result.stderr)
    assert result.returncode == 0, f"{title} 執行失敗！"
    print(f"✅ {title} 執行成功。")
    return result

def verify_final_data(analytics_db_path):
    """對最終的分析資料庫進行抽樣驗證"""
    print_header("4. 最終驗證分析數據庫")
    if not os.path.exists(analytics_db_path):
        print(f"❌ 驗證失敗: 分析資料庫檔案不存在 {analytics_db_path}")
        return False

    conn = duckdb.connect(analytics_db_path, read_only=True)
    try:
        tables = conn.execute("SHOW TABLES;").fetchdf()
        assert 'daily_ohlc' in tables['name'].values, "目標表格 'daily_ohlc' 未被創建"
        print("✅ 驗證點 1/4: 目標表格 'daily_ohlc' 已成功創建。")

        total_rows = conn.execute("SELECT COUNT(*) FROM daily_ohlc").fetchone()[0]
        assert total_rows == 3, f"數據總行數應為 3，實際為 {total_rows}"
        print(f"✅ 驗證點 2/4: 數據總行數為 {total_rows}，符合預期。")

        df = conn.execute("SELECT * FROM daily_ohlc ORDER BY trading_date, product_id, option_type NULLS LAST, strike_price NULLS LAST").fetchdf()

        # 檢查 trading_date 是否為 datetime.date 物件
        # 轉換 trading_date 列為 python date 物件，如果它是 datetime64 類型
        if pd.api.types.is_datetime64_any_dtype(df['trading_date'].dtype):
            df['trading_date'] = df['trading_date'].dt.date

        assert all(isinstance(d, date) for d in df['trading_date']), "trading_date 欄位類型不正確, 應為 date object"
        assert pd.api.types.is_numeric_dtype(df['strike_price']), "strike_price 欄位類型不正確"
        assert pd.api.types.is_integer_dtype(df['volume']), "volume 欄位類型不正確"
        print("✅ 驗證點 3/4: 關鍵欄位的資料類型已成功轉換。")

        # 數據抽樣檢查
        # Options_20250102.zip - Daily_2025_01_02.csv - Row 1
        option_row_1 = df[(df['product_id'] == 'TXO') & (df['option_type'] == 'Call') & (df['strike_price'] == 18000.0)].iloc[0]
        assert option_row_1['trading_date'] == date(2025, 1, 2), f"選擇權1日期: Expected 2025-01-02, Got {option_row_1['trading_date']}"
        assert option_row_1['volume'] == 1025, f"選擇權1成交量: Expected 1025, Got {option_row_1['volume']}"

        # Options_20250102.zip - Daily_2025_01_02.csv - Row 2
        option_row_2 = df[(df['product_id'] == 'TXO') & (df['option_type'] == 'Put') & (df['strike_price'] == 18100.0)].iloc[0]
        assert option_row_2['trading_date'] == date(2025, 1, 2), f"選擇權2日期: Expected 2025-01-02, Got {option_row_2['trading_date']}"
        assert option_row_2['volume'] == 2510, f"選擇權2成交量 (帶逗號): Expected 2510, Got {option_row_2['volume']}"

        # Futures_20250103.csv - Row 1
        future_row = df[df['product_id'] == 'MTX'].iloc[0]
        assert future_row['trading_date'] == date(2025, 1, 3), f"期貨日期: Expected 2025-01-03, Got {future_row['trading_date']}"
        assert future_row['option_type'] == '期貨', f"期貨買賣權: Expected '期貨', Got {future_row['option_type']}"
        assert pd.isna(future_row['strike_price']), f"期貨履約價應為 NULL, Got {future_row['strike_price']}"
        assert future_row['volume'] == 3000, f"期貨成交量 (帶逗號): Expected 3000, Got {future_row['volume']}"
        print("✅ 驗證點 4/4: 數據內容抽樣檢查通過。")

        return True
    except Exception as e:
        import traceback
        print(f"❌ 驗證失敗: 查詢分析資料庫時發生錯誤: {e}")
        traceback.print_exc()
        return False
    finally:
        conn.close()

def main():
    final_test_result = False # 初始化為 False
    try:
        all_files = setup_test_environment()

        loader_args = [
            "--input-files"] + all_files + [
            "--db-output-dir", DB_DIR,
            "--db-name", os.path.basename(RAW_DB_PATH),
            "--metadata-db-path", METADATA_DB_PATH,
            "--log-level", "INFO"
        ]
        run_script(LOADER_SCRIPT, loader_args, "2. 執行高速載入器 (含預翻譯)")

        transformer_args = [
            "--raw-db-path", RAW_DB_PATH,
            "--analytics-db-path", ANALYTICS_DB_PATH,
            "--log-level", "DEBUG" # 傳遞 DEBUG 日誌級別
        ]
        run_script(TRANSFORMER_SCRIPT, transformer_args, "3. 執行數據轉換器 (Python 迭代模式)")

        final_test_result = verify_final_data(ANALYTICS_DB_PATH)

    except Exception as e:
        import traceback
        print(f"測試腳本執行期間發生未預期的嚴重錯誤: {e}")
        traceback.print_exc()
        final_test_result = False # 確保出錯時為 False
    finally:
        print_header("5. 清理測試環境")
        if os.path.exists(TEST_WORKSPACE):
            shutil.rmtree(TEST_WORKSPACE)
            print(f"✅ 測試工作區 '{TEST_WORKSPACE}' 已成功刪除。")

        if final_test_result:
            print("\n" + "#"*40)
            print("✅  v35.0 (最終優化版) 全鏈路整合演習成功！")
            print("#"*40)
            sys.exit(0)
        else:
            print("\n" + "!"*40)
            print("❌  v35.0 (最終優化版) 全鏈路整合演習失敗！ 請檢查上方日誌。")
            print("!"*40)
            sys.exit(1)

if __name__ == "__main__":
    main()
