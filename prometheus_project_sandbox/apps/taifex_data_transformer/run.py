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
        new_col = str(col).strip().lower().replace(' ', '_').replace('(', '').replace(')', '')
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

    # --- 準備目標表格 (簡化版，實際應更複雜) ---
    target_conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_market_summary (
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

    # --- 讀取待處理數據 ---
    log_info("正在從「原始數據艙」讀取待處理的數據與配方...")
    try:
        # 僅查詢尚未轉換的數據
        target_hashes = target_conn.execute("SELECT DISTINCT file_hash FROM daily_market_summary").fetchall()
        processed_hashes = [h[0] for h in target_hashes]

        query = "SELECT file_hash, source_file, file_content_as_text, parsing_recipe_json FROM raw_import_log"
        if processed_hashes:
            query += f" WHERE file_hash NOT IN ({','.join(['?']*len(processed_hashes))})"

        tasks = raw_conn.execute(query, processed_hashes if processed_hashes else []).fetchall()
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
            if not recipe_json or recipe_json == 'null':
                log_error("配方為空，無法轉換，跳過。")
                continue

            recipe = json.loads(recipe_json)
            header_row = recipe.get('header_row')

            # 使用配方進行精準解析
            log_info(f"應用解析配方: header={header_row}")
            df = pd.read_csv(StringIO(text_content), header=header_row, thousands=',')
            df = clean_column_names(df)

            # 簡單的數據清洗與篩選 (可擴充)
            required_cols = ['交易日期', '契約', '開盤價', '最高價', '最低價', '收盤價', '成交量']
            # 將 DataFrame 的欄位名稱也標準化
            df.columns = [str(c).strip() for c in df.columns]

            # 找到實際存在的欄位
            existing_cols = [col for col in required_cols if col in df.columns]

            if len(existing_cols) < 3: # 至少要有幾個關鍵欄位
                 log_error(f"缺少過多關鍵欄位，無法處理。")
                 continue

            df_final = df[existing_cols]
            df_final['file_hash'] = file_hash # 加入指紋，用於追蹤與去重

            # 載入至最終分析庫
            target_conn.execute(f"INSERT INTO daily_market_summary SELECT * FROM df_final;")
            log_success(f"成功轉換並載入 {len(df_final)} 筆記錄至分析數據庫。")

        except Exception as e:
            log_error(f"處理檔案 {source_file} 時發生錯誤: {e}")

    log_info("--- 數據轉換器所有任務執行完畢 ---")
    raw_conn.close()
    target_conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="數據轉換器 - 普羅米修斯版")
    parser.add_argument("--raw-db-path", required=True, help="原始數據艙 (raw_taifex.duckdb) 的路徑")
    parser.add_argument("--target-db-path", required=True, help="最終分析數據庫 (taifex_historical.duckdb) 的路徑")
    args = parser.parse_args()
    main(args.raw_db_path, args.target_db_path)
