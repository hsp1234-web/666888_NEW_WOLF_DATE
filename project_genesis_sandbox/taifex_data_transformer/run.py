# 版本: v1.2 (Prometheus)
# 職責: 配方感知、動態歸檔、安全轉換 (在「創世紀閃擊戰」中不直接執行)
import sys
import json
import duckdb
import pandas as pd
from io import StringIO
from datetime import datetime

# --- 日誌記錄函數 ---
def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def log_info(message, component="TRANSFORMER"):
    print(f"[{get_timestamp()}] [{component}] [INFO] {message}")

def log_success(message, component="TRANSFORMER"):
    print(f"[{get_timestamp()}] [{component}] [SUCCESS] ✅ {message}")

def log_warning(message, component="TRANSFORMER"):
    print(f"[{get_timestamp()}] [{component}] [WARNING] ⚠️ {message}")

def log_error(message, component="TRANSFORMER"):
    print(f"[{get_timestamp()}] [{component}] [ERROR] ❌ {message}")
# --- 日誌記錄函數結束 ---

def get_target_table(filename):
    """根據檔名動態選擇目標表"""
    filename_lower = filename.lower()
    if 'daily' in filename_lower and ('futures_tick' in filename_lower or 'options_tick' in filename_lower or 'daily_' in filename_lower): # Daily_YYYY_MM_DD.zip, Daily_Futures_Tick_YYYY_MM_DD.zip, etc.
        return 'daily_market_summary' # 假設這是主要的日交易匯總表
    elif 'annual_futures' in filename_lower: # Annual_Futures_YYYY.zip
        return 'annual_futures_summary' # 年度期貨數據表
    elif 'annual_options' in filename_lower: # Annual_Options_YYYY.zip
        return 'annual_options_summary' # 年度選擇權數據表
    elif 'pc_ratio' in filename_lower: # 假設有專門的 P/C Ratio 檔案
        return 'pc_ratio_data'
    elif 'delta' in filename_lower: # 假設有專門的選擇權 Delta 檔案
        return 'options_delta_data'

    log_warning(f"無法根據檔名 '{filename}' 明確路由到目標表，將使用預設表 'other_data'。請檢查路由邏輯。")
    return 'other_data' # 一個預設的表，用於未明確路由的數據

def clean_column_names(df):
    """標準化欄位名稱：移除特殊字符、空格，轉為小寫，處理中文括號等。"""
    new_cols = []
    for col in df.columns:
        s = str(col).strip()
        s = s.replace(' ', '_').replace('(', '').replace(')', '')
        s = s.replace('（', '').replace('）', '') # 處理全形括號
        s = s.replace('：', '').replace(':', '') # 處理冒號
        s = s.lower() # 統一轉為小寫
        new_cols.append(s)
    df.columns = new_cols
    return df

def main(raw_db_path, target_db_path):
    log_info("--- 數據轉換器 (v1.2 - 普羅米修斯版) ---")
    log_info("在「創世紀閃擊戰」模式下，此模組不直接執行。")

    # 以下是原始的轉換邏輯，為保持模組完整性而保留，但在閃擊戰中不直接使用。
    # try:
    #     # 連接到原始數據艙 (只讀)
    #     raw_conn = duckdb.connect(database=raw_db_path, read_only=True)
    #     # 連接到目標分析數據庫 (讀寫)
    #     target_conn = duckdb.connect(database=target_db_path, read_only=False)
    #     log_info(f"成功連接「原始數據艙」({raw_db_path}) 與「分析數據庫」({target_db_path})。")
    # except Exception as e:
    #     log_error(f"連接資料庫失敗: {e}"); return

    # # --- 初始化目標表結構 (Schema) ---
    # # 這些 CREATE TABLE IF NOT EXISTS 語句定義了目標數據庫的結構。
    # # 欄位名稱應使用清理後的英文/拼音小寫。
    # target_conn.execute("""
    # CREATE TABLE IF NOT EXISTS daily_market_summary (
    #     file_hash VARCHAR PRIMARY KEY,
    #     source_file VARCHAR,
    #     trade_date DATE, -- 交易日期
    #     contract VARCHAR, -- 契約
    #     open_price DOUBLE, -- 開盤價
    #     high_price DOUBLE, -- 最高價
    #     low_price DOUBLE, -- 最低價
    #     close_price DOUBLE, -- 收盤價
    #     settlement_price DOUBLE, -- 結算價
    #     volume BIGINT, -- 成交量
    #     open_interest BIGINT, -- 未平倉量
    #     transformed_at TIMESTAMP DEFAULT current_timestamp
    # );""")
    # log_info("已確認/建立 'daily_market_summary' 表。")

    # target_conn.execute("""
    # CREATE TABLE IF NOT EXISTS annual_futures_summary (
    #     file_hash VARCHAR,
    #     source_file VARCHAR,
    #     trade_date DATE,
    #     contract_month VARCHAR, -- 契約月份
    #     product_name VARCHAR, -- 商品名稱
    #     open_price DOUBLE,
    #     high_price DOUBLE,
    #     low_price DOUBLE,
    #     close_price DOUBLE,
    #     volume BIGINT,
    #     transformed_at TIMESTAMP DEFAULT current_timestamp,
    #     PRIMARY KEY (file_hash, trade_date, contract_month, product_name) -- 複合主鍵
    # );""")
    # log_info("已確認/建立 'annual_futures_summary' 表。")

    # target_conn.execute("""
    # CREATE TABLE IF NOT EXISTS annual_options_summary (
    #     file_hash VARCHAR,
    #     source_file VARCHAR,
    #     trade_date DATE,
    #     contract_month VARCHAR,
    #     call_put VARCHAR, -- 買賣權 (C 或 P)
    #     strike_price DOUBLE, -- 履約價
    #     open_price DOUBLE,
    #     high_price DOUBLE,
    #     low_price DOUBLE,
    #     close_price DOUBLE,
    #     volume BIGINT,
    #     transformed_at TIMESTAMP DEFAULT current_timestamp,
    #     PRIMARY KEY (file_hash, trade_date, contract_month, call_put, strike_price) -- 複合主鍵
    # );""")
    # log_info("已確認/建立 'annual_options_summary' 表。")

    # target_conn.execute("""
    # CREATE TABLE IF NOT EXISTS pc_ratio_data (
    #     file_hash VARCHAR PRIMARY KEY,
    #     source_file VARCHAR,
    #     date DATE,
    #     pc_ratio DOUBLE, -- 買賣權成交量比率
    #     transformed_at TIMESTAMP DEFAULT current_timestamp
    # );""")
    # log_info("已確認/建立 'pc_ratio_data' 表。")

    # target_conn.execute("""
    # CREATE TABLE IF NOT EXISTS options_delta_data (
    #     file_hash VARCHAR,
    #     source_file VARCHAR,
    #     date DATE,
    #     strike_price DOUBLE,
    #     delta DOUBLE,
    #     transformed_at TIMESTAMP DEFAULT current_timestamp,
    #     PRIMARY KEY (file_hash, date, strike_price)
    # );""")
    # log_info("已確認/建立 'options_delta_data' 表。")

    # target_conn.execute("""
    # CREATE TABLE IF NOT EXISTS other_data (
    #     file_hash VARCHAR PRIMARY KEY,
    #     source_file VARCHAR,
    #     raw_content TEXT, -- 對於無法解析的數據，可以考慮儲存原始文本
    #     transformed_at TIMESTAMP DEFAULT current_timestamp
    # );""")
    # log_info("已確認/建立 'other_data' (預設) 表。")
    # # --- Schema 初始化完畢 ---

    # # --- 查詢所有尚未被處理的任務 ---
    # # 這裡的查詢邏輯需要根據你的目標表來動態構建，或者有一個單獨的已處理日誌
    # # 為簡化，我們假設 raw_import_log 中所有未在任何目標表中出現 file_hash 的都需要處理
    # # 實際應用中，可能需要一個更明確的 "transformed_log" 表。
    # # 此查詢僅為示例，實際應用需要更嚴謹的邏輯判斷哪些任務是「新」的。
    # # 一個簡單的方法是，檢查 file_hash 是否已存在於 *任何* 一個目標表中。
    # # DuckDB 不直接支持查詢所有表，所以我們需要分別檢查或維護一個中央處理日誌。
    # # 這裡使用一個簡化的邏輯：只從 raw_import_log 讀取，然後在轉換時用 INSERT OR IGNORE 或 REPLACE。

    # new_tasks_query = """
    # SELECT log.file_hash, log.source_file, log.file_content_as_text, log.parsing_recipe_json
    # FROM raw_import_log AS log
    # WHERE log.file_hash NOT IN (SELECT DISTINCT file_hash FROM daily_market_summary)
    #   AND log.file_hash NOT IN (SELECT DISTINCT file_hash FROM annual_futures_summary)
    #   AND log.file_hash NOT IN (SELECT DISTINCT file_hash FROM annual_options_summary)
    #   AND log.file_hash NOT IN (SELECT DISTINCT file_hash FROM pc_ratio_data)
    #   AND log.file_hash NOT IN (SELECT DISTINCT file_hash FROM options_delta_data)
    #   AND log.file_hash NOT IN (SELECT DISTINCT file_hash FROM other_data);
    # """
    # try:
    #     tasks = raw_conn.execute(new_tasks_query).fetchall()
    #     log_info(f"發現 {len(tasks)} 個新的數據轉換任務。")
    # except Exception as e_query:
    #     log_error(f"查詢新任務時發生錯誤: {e_query}. 可能原因：raw_import_log 表不存在或查詢語法問題。")
    #     tasks = []


    # for file_hash, source_file, text_content, recipe_json_str in tasks:
    #     try:
    #         log_info(f"--- 開始轉換檔案: {source_file} (Hash: {file_hash[:8]}) ---")
    #         if not recipe_json_str or recipe_json_str.lower() == 'null':
    #             log_warning(f"配方為空 (null string or empty)，無法轉換，將檔案標記至 'other_data'。 Hash: {file_hash[:8]}");
    #             target_conn.execute("INSERT OR REPLACE INTO other_data (file_hash, source_file, raw_content) VALUES (?, ?, ?)",
    #                                 [file_hash, source_file, text_content])
    #             continue

    #         try:
    #             recipe = json.loads(recipe_json_str)
    #         except json.JSONDecodeError:
    #             log_error(f"解析配方JSON失敗: '{recipe_json_str[:100]}...'，跳過。 Hash: {file_hash[:8]}");
    #             target_conn.execute("INSERT OR REPLACE INTO other_data (file_hash, source_file, raw_content) VALUES (?, ?, ?)",
    #                                 [file_hash, source_file, text_content])
    #             continue

    #         if not recipe or recipe.get('parser') != 'csv':
    #             log_warning(f"配方不適用或非CSV ({recipe.get('parser') if recipe else 'N/A'})，無法標準轉換，將檔案標記至 'other_data'。 Hash: {file_hash[:8]}");
    #             target_conn.execute("INSERT OR REPLACE INTO other_data (file_hash, source_file, raw_content) VALUES (?, ?, ?)",
    #                                 [file_hash, source_file, text_content])
    #             continue

    #         # 使用配方解析CSV
    #         # header 参数：如果是 0-based index，则用 recipe.get('header_row')；如果是 None，则 pandas 自动推断或无表头
    #         # skiprows 参数：跳过表头之前的行，以及表头本身，所以是 recipe.get('data_start_row')
    #         # 如果 data_start_row 就是 header_row 的下一行，并且 header_row 有效，那么 skiprows 应该是 header_row
    #         # pandas 的 header 参数可以直接指定哪一行是表头
    #         header_setting = recipe.get('header_row') # 可能为 None 或 int
    #         # 如果 recipe['data_start_row'] 是实际数据的第一行（0-indexed），且 header_setting 是表头行号
    #         # pandas 的 `header` 参数已经处理了表头行的读取，`skiprows` 通常用于跳过文件顶部的非数据行
    #         # 如果 header_setting is None, pandas 会尝试自动检测或认为没有表头。
    #         # 如果 header_setting is not None, data should start from header_setting + 1.
    #         # 我们用 data_start_row 来精确控制，但 pandas read_csv 的 skiprows 和 header 配合使用需要小心

    #         df = pd.read_csv(StringIO(text_content),
    #                          header=header_setting, # 可以是 None
    #                          skiprows=recipe.get('data_start_row') if header_setting is None and recipe.get('data_start_row', 0) > 0 else None, # 只有在無表頭推斷時，才用data_start_row跳過
    #                          thousands=',', # 處理千分位逗號
    #                          on_bad_lines='warn', # 對於壞行發出警告，而不是直接失敗
    #                          encoding=recipe.get('encoding', 'utf-8')) # 使用配方中的編碼

    #         df = clean_column_names(df)
    #         df['file_hash'] = file_hash # 加入檔案哈希作為溯源
    #         df['source_file'] = source_file # 加入源檔名

    #         table_name = get_target_table(source_file)
    #         log_info(f"檔案 '{source_file}' 被路由到目標表: {table_name}")

    #         # --- 安全欄位映射與類型轉換 ---
    #         db_cols_info = target_conn.execute(f"DESCRIBE {table_name};").fetchall()
    #         db_cols_type_map = {col[0]: col[1].upper() for col in db_cols_info} # {col_name: TYPE}

    #         df_to_insert = pd.DataFrame()
    #         for db_col_name, db_col_type in db_cols_type_map.items():
    #             if db_col_name in df.columns:
    #                 # 類型轉換 (非常重要)
    #                 try:
    #                     if df[db_col_name].isnull().all() and 'VARCHAR' not in db_col_type and 'TEXT' not in db_col_type: # 全空且非文本類型
    #                         df_to_insert[db_col_name] = None # 維持 None
    #                     elif 'DATE' in db_col_type:
    #                         df_to_insert[db_col_name] = pd.to_datetime(df[db_col_name], errors='coerce').dt.date
    #                     elif 'TIMESTAMP' in db_col_type:
    #                          df_to_insert[db_col_name] = pd.to_datetime(df[db_col_name], errors='coerce')
    #                     elif 'DOUBLE' in db_col_type or 'FLOAT' in db_col_type or 'DECIMAL' in db_col_type or 'NUMERIC' in db_col_type:
    #                         # 移除所有非數字、非點號、非負號的字符，然後轉換
    #                         df_to_insert[db_col_name] = pd.to_numeric(df[db_col_name].astype(str).str.replace(r'[^\d\.\-]', '', regex=True), errors='coerce')
    #                     elif 'BIGINT' in db_col_type or 'INTEGER' in db_col_type:
    #                         df_to_insert[db_col_name] = pd.to_numeric(df[db_col_name].astype(str).str.replace(r'[^\d\-]', '', regex=True), errors='coerce', downcast='integer')
    #                     elif 'BOOLEAN' in db_col_type:
    #                         # 簡單的布爾轉換，可能需要根據實際數據調整
    #                         df_to_insert[db_col_name] = df[db_col_name].astype(str).str.lower().isin(['true', '1', 'yes', 't'])
    #                     else: # VARCHAR, TEXT 等
    #                         df_to_insert[db_col_name] = df[db_col_name].astype(str)
    #                 except Exception as e_type_conv:
    #                     log_warning(f"欄位 '{db_col_name}' 類型轉換至 '{db_col_type}' 失敗: {e_type_conv}. 設為 NULL。")
    #                     df_to_insert[db_col_name] = None # 轉換失敗則設為 NULL
    #             elif db_col_name not in ['transformed_at']: # transformed_at 有預設值，不需源數據
    #                 log_warning(f"目標表 '{table_name}' 的欄位 '{db_col_name}' 在來源 DataFrame 中未找到，將被設為 NULL。")
    #                 df_to_insert[db_col_name] = None # 若目標欄位不存在於源DF，則設為 NULL

    #         # 確保所有目標表欄位都在 df_to_insert 中，即使是全NULL
    #         for db_col_name in db_cols_type_map.keys():
    #             if db_col_name not in df_to_insert.columns:
    #                 df_to_insert[db_col_name] = None

    #         # 只選擇目標表中存在的欄位進行插入
    #         df_final_for_insert = df_to_insert[list(db_cols_type_map.keys())]


    #         if df_final_for_insert.empty or df_final_for_insert.drop(columns=['file_hash', 'source_file'], errors='ignore').isnull().all().all():
    #             log_warning(f"轉換後 DataFrame 為空或僅含 NULL (除 file_hash/source_file 外)，不執行插入。 Hash: {file_hash[:8]}"); continue

    #         try:
    #             # 使用 INSERT OR REPLACE (如果主鍵衝突則替換) 或 INSERT OR IGNORE (衝突則忽略)
    #             # 這取決於業務邏輯，通常對於歷史數據，REPLACE 更合適以確保最新
    #             # 注意: DuckDB 的 INSERT OR REPLACE INTO table BY NAME SELECT * FROM df; 語法很方便
    #             target_conn.execute(f"INSERT OR REPLACE INTO {table_name} BY NAME SELECT * FROM df_final_for_insert;")
    #             log_success(f"成功轉換並載入 {len(df_final_for_insert)} 筆記錄至 '{table_name}'。 Hash: {file_hash[:8]}")
    #         except Exception as e_insert:
    #             log_error(f"插入數據到 '{table_name}' 失敗: {e_insert}. Hash: {file_hash[:8]}")
    #             # 可以考慮將失敗的數據存到一個錯誤表
    #             log_info(f"DataFrame 內容預覽 (前5行):\n{df_final_for_insert.head().to_string()}")


    #     except pd.errors.EmptyDataError:
    #         log_warning(f"檔案 {source_file} (或其解析配方) 導致了空數據，跳過。 Hash: {file_hash[:8]}")
    #     except Exception as e_outer:
    #         log_error(f"處理檔案 {source_file} 時發生未預期錯誤: {e_outer}. Hash: {file_hash[:8]}")

    # if raw_conn: raw_conn.close()
    # if target_conn: target_conn.close()
    log_info("--- 數據轉換器 ELT 邏輯 (未執行) ---")

if __name__ == '__main__':
    if len(sys.argv) == 3:
        # main(sys.argv[1], sys.argv[2])
        log_info("main() 函數未在「創世紀閃擊戰」模式下執行。若要執行，請取消註解對應行。")
        log_info(f"參數: raw_db_path={sys.argv[1]}, target_db_path={sys.argv[2]}")
    else:
        log_info("`taifex_data_transformer` 模組已載入。")
        # print("用法 (ELT): python run.py <raw_db_path> <target_db_path>")
