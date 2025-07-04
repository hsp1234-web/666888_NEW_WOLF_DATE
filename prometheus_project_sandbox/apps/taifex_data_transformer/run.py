import os
import sys
import argparse
import json
import duckdb
import pandas as pd
from io import StringIO
from datetime import datetime

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def log_info(message):
    print(f"[{get_timestamp()}] [INFO] {message}")

def log_success(message):
    print(f"[{get_timestamp()}] [SUCCESS] ✅ {message}")

def log_error(message):
    print(f"[{get_timestamp()}] [ERROR] ❌ {message}")

def clean_column_names(df):
    """標準化欄位名稱的清洗函數"""
    cols = df.columns
    new_cols = []
    for col in cols:
        new_col = str(col).strip().lower()
        new_col = new_col.replace(' ', '_').replace('(', '').replace(')', '').replace('%', '')
        # 如果最後是 _pct，則移除，因為 % 已經移除了
        if new_col.endswith('_pct'):
            new_col = new_col[:-4]
        new_cols.append(new_col)
    df.columns = new_cols
    return df

def main(raw_db_path, target_db_path):
    log_info("--- 數據轉換器 (v36.0 - 普羅米修斯版) 啟動 ---")

    # --- 連接數據庫 ---
    try:
        raw_conn = duckdb.connect(database=raw_db_path, read_only=True)
        target_conn = duckdb.connect(database=target_db_path, read_only=False)
        log_info("成功連接「原始數據艙」與「分析數據庫」。")
    except Exception as e:
        log_error(f"連接資料庫失敗: {e}")
        return

    # --- 準備目標表格 ---
    # 主要行情表
    SUMMARY_TABLE_NAME = "daily_market_summary"
    target_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE_NAME} (
            file_hash VARCHAR,
            交易日期 VARCHAR,
            契約 VARCHAR,
            開盤價 DOUBLE,
            最高價 DOUBLE,
            最低價 DOUBLE,
            收盤價 DOUBLE,
            成交量 BIGINT
        );
    """)

    # PC Ratio 表
    PCRATIO_TABLE_NAME = "pc_ratio_data"
    target_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {PCRATIO_TABLE_NAME} (
            file_hash VARCHAR,
            日期 VARCHAR,
            買賣權別 VARCHAR,
            買進成交量 BIGINT,
            賣出成交量 BIGINT,
            買賣權成交量比率 DOUBLE
        );
    """) # 欄位名 '買賣權成交量比率' 經 clean_column_names 會變 '買賣權成交量比率'

    # Delta 表
    DELTA_TABLE_NAME = "options_delta_data"
    target_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DELTA_TABLE_NAME} (
            file_hash VARCHAR,
            日期 VARCHAR,
            履約價 VARCHAR,
            買賣權別 VARCHAR,
            delta DOUBLE
        );
    """)

    # 定義所有目標表以供後續使用
    TARGET_TABLES_CONFIG = {
        SUMMARY_TABLE_NAME: {"columns": []},
        PCRATIO_TABLE_NAME: {"columns": []},
        DELTA_TABLE_NAME: {"columns": []}
    }

    for table_name in TARGET_TABLES_CONFIG.keys():
        try:
            table_info = target_conn.execute(f"PRAGMA table_info('{table_name}');").fetchall()
            TARGET_TABLES_CONFIG[table_name]["columns"] = [info[1] for info in table_info]
            log_info(f"目標表 '{table_name}' 的欄位: {TARGET_TABLES_CONFIG[table_name]['columns']}")
        except Exception as e:
            log_error(f"無法獲取目標表 '{table_name}' 的欄位資訊: {e}")
            raw_conn.close()
            target_conn.close()
            return

    # --- 讀取待處理數據 ---
    log_info("正在從「原始數據艙」讀取待處理的數據與配方...")
    try:
        # 僅查詢尚未轉換的數據 (從所有目標表中檢查)
        processed_hashes_set = set()
        for table_name in TARGET_TABLES_CONFIG.keys():
            try:
                hashes_in_table = target_conn.execute(f"SELECT DISTINCT file_hash FROM {table_name} WHERE file_hash IS NOT NULL").fetchall()
                for h_tuple in hashes_in_table:
                    if h_tuple and h_tuple[0]:
                        processed_hashes_set.add(h_tuple[0])
            except Exception as e:
                log_warning(f"查詢表 {table_name} 中的已處理 hashes 時出錯: {e}")

        processed_hashes_list = list(processed_hashes_set)
        log_info(f"從目標表中收集到的已處理 File Hashes 共 {len(processed_hashes_list)} 個。")

        query = "SELECT file_hash, source_file, file_content_as_text, parsing_recipe_json FROM raw_import_log"
        params = []
        if processed_hashes_list:
            placeholders = ','.join(['?'] * len(processed_hashes_list))
            query += f" WHERE file_hash NOT IN ({placeholders})"
            params.extend(processed_hashes_list)

        tasks = raw_conn.execute(query, params).fetchall()
        log_info(f"發現 {len(tasks)} 個新的數據任務需要轉換。")
    except Exception as e:
        log_error(f"查詢待處理任務時失敗: {e}")
        raw_conn.close()
        target_conn.close()
        return

    # --- 迭代執行轉換 (ELT之T) ---
    for file_hash, source_file, text_content, recipe_json in tasks:
        try:
            log_info(f"--- 開始轉換檔案: {source_file} (Hash: {file_hash[:8]}) ---")

            if not recipe_json: # Handles None or empty string from DB
                log_error(f"檔案 {source_file} 的解析配方為空，無法轉換，跳過。")
                continue

            try:
                recipe = json.loads(recipe_json) # Handles 'null' string from DB, converting to None
            except json.JSONDecodeError as e:
                log_error(f"解析檔案 {source_file} 的配方JSON時失敗: {e}，內容: {recipe_json}")
                continue

            header_to_use = None # Default to no header if recipe is None or header_row is missing
            if recipe is not None:
                header_to_use = recipe.get('header_row') # Returns None if key missing
            else:
                log_warning(f"檔案 {source_file} 的配方解析後為空 (可能原配方為 'null' 或無效)。嘗試按無表頭處理。")


            # 使用配方進行精準解析
            log_info(f"應用解析配方: header={header_to_use if header_to_use is not None else '自動(None)'}")
            df = pd.read_csv(StringIO(text_content), header=header_to_use, thousands=',')
            df = clean_column_names(df) # 標準化 df 的欄位名

            # 指令二：學會「對號入座」
            df_columns = df.columns.tolist()

            # 找出 DataFrame 和目標表之間的共同欄位
            # 確保 file_hash 也被考慮在內
            if 'file_hash' not in df.columns:
                df['file_hash'] = file_hash

            df_columns = df.columns.tolist()

            # --- 指令三：學會「分類歸檔」---
            current_target_table_name = None
            current_target_columns = None

            if "PC_Ratio" in source_file:
                current_target_table_name = PCRATIO_TABLE_NAME
            elif "Delta" in source_file: # 假設檔名中包含 "Delta" 指的是 Delta 值檔案
                current_target_table_name = DELTA_TABLE_NAME
            elif "OptionsDaily" in source_file or "Daily" in source_file or \
                 "Prometheus_Daily_Test" in source_file or "Prometheus_Options_Test" in source_file:
                current_target_table_name = SUMMARY_TABLE_NAME
            else:
                # 如果檔名不符合任何已知模式，可以選擇預設表或跳過
                log_warning(f"檔案 {source_file} 未匹配到特定目標表，將嘗試使用預設表 {SUMMARY_TABLE_NAME}。")
                current_target_table_name = SUMMARY_TABLE_NAME

            current_target_columns = TARGET_TABLES_CONFIG[current_target_table_name]["columns"]
            log_info(f"檔案 {source_file} 被路由到目標表: {current_target_table_name}")

            # 欄位交集 (同時保持目標表欄位順序)
            insert_cols = [col for col in current_target_columns if col in df_columns]

            if not insert_cols:
                log_warning(f"檔案 {source_file} 解析後的欄位與目標表 {current_target_table_name} 沒有共同欄位，跳過。 DataFrame欄位: {df_columns}")
                continue

            min_cols_required = 1 if 'file_hash' not in insert_cols else 2 # 若無file_hash則至少1欄, 有則至少2欄
            if len(insert_cols) < min_cols_required :
                 log_warning(f"檔案 {source_file} 與目標表 {current_target_table_name} 的共同欄位過少 (少於 {min_cols_required} 個非file_hash欄位)，跳過。交集欄位: {insert_cols}")
                 continue

            df_to_insert = df[insert_cols].copy() # 使用 .copy() 避免 SettingWithCopyWarning

            # 針對特定表的數據類型轉換 (範例)
            if current_target_table_name == PCRATIO_TABLE_NAME:
                if '買賣權成交量比率' in df_to_insert.columns: # 原欄位名是 '買賣權成交量比率%'
                    df_to_insert['買賣權成交量比率'] = pd.to_numeric(df_to_insert['買賣權成交量比率'], errors='coerce') / 100.0
            if current_target_table_name == DELTA_TABLE_NAME:
                if 'delta' in df_to_insert.columns:
                     df_to_insert['delta'] = pd.to_numeric(df_to_insert['delta'], errors='coerce')


            # 動態生成 INSERT 語句
            cols_str = ", ".join([f'"{c}"' for c in insert_cols])
            placeholders = ", ".join(["?"] * len(insert_cols))

            try:
                target_conn.executemany(f"INSERT INTO {current_target_table_name} ({cols_str}) VALUES ({placeholders})", df_to_insert.values.tolist())
                log_success(f"成功轉換並載入 {len(df_to_insert)} 筆記錄至 '{current_target_table_name}'。欄位: {insert_cols}")

            except Exception as insert_err:
                log_error(f"插入數據到 '{current_target_table_name}' 時失敗 for {source_file}: {insert_err}")
                log_error(f"    DataFrame欄位: {df_to_insert.columns.tolist()}")
                log_error(f"    DataFrame Dtypes: \n{df_to_insert.dtypes}")
                log_error(f"    目標表欄位: {current_target_columns}")
                log_error(f"    嘗試插入的欄位: {insert_cols}")

        except Exception as e:
            log_error(f"處理檔案 {source_file} 時發生嚴重錯誤: {e}")

    log_info("--- 數據轉換器所有任務執行完畢 ---")
    raw_conn.close()
    target_conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="數據轉換器 - 普羅米修斯版")
    parser.add_argument("--raw-db-path", required=True, help="原始數據艙 (raw_taifex.duckdb) 的路徑")
    parser.add_argument("--target-db-path", required=True, help="最終分析數據庫 (taifex_historical.duckdb) 的路徑")
    args = parser.parse_args()
    main(args.raw_db_path, args.target_db_path)
