import os
import sys
import argparse
import hashlib
import json
import duckdb
import chardet
import pandas as pd
from datetime import datetime
import zipfile
import io

# --- 核心作戰理念 ---
# 1. 速度 (Velocity): 快速完成檔案的初步處理與載入。
# 2. 預見 (Foresight): 植入智能探勘能力，為下游轉換器提供情報。
# 3. 原子化 (Atomicity): 作為一個獨立、可執行的作戰單元。

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def log_info(message):
    print(f"[{get_timestamp()}] [INFO] {message}")

def log_success(message):
    print(f"[{get_timestamp()}] [SUCCESS] ✅ {message}")

def log_warning(message):
    print(f"[{get_timestamp()}] [WARNING] ⚠️ {message}")

def log_error(message):
    print(f"[{get_timestamp()}] [ERROR] ❌ {message}")

def calculate_sha256(content_bytes):
    """計算檔案內容的 SHA-256 指紋"""
    return hashlib.sha256(content_bytes).hexdigest()

def determine_parsing_recipe(content_bytes):
    """
    智能格式探勘官 (移植自 v8.0 核心)
    對未知檔案進行審問，破解其編碼與結構，生成解析配方。
    """
    if not content_bytes or len(content_bytes) < 10: # 過濾空檔案或過小檔案
        return None

    recipe = {"parser": "unknown", "encoding": None, "header_row": None}

    # 1. 編碼破解
    try:
        detected = chardet.detect(content_bytes[:4096]) # 窺探樣本
        encoding = detected.get('encoding', 'utf-8').lower()
        if 'big5' in encoding or 'ms950' in encoding:
            recipe['encoding'] = 'ms950'
        else:
            recipe['encoding'] = 'utf-8-sig'
        sample_text = content_bytes.decode(recipe['encoding'])
    except (UnicodeDecodeError, TypeError):
        log_warning("編碼破解失敗，檔案可能為二進位或內容損毀。")
        return None # 無法解碼，直接判定為毒丸

    # 2. 結構識別 (簡化版，專注於CSV)
    try:
        sample_lines = sample_text.splitlines()[:20]
        if not any(',' in line for line in sample_lines):
             log_warning(f"結構識別失敗：檔案不包含CSV分隔符。")
             return None

        # 簡單的表頭探勘邏輯：尋找第一個包含較多非數字字元的行
        header_row_index = -1
        for i, line in enumerate(sample_lines):
            if line.strip() and len(line) > 5:
                 # 假設包含 '日期' 或 '契約' 的是表頭
                if any(kw in line for kw in ['日期', '契約', '商品', '成交']):
                    header_row_index = i
                    break

        if header_row_index != -1:
            recipe['parser'] = 'csv'
            recipe['header_row'] = header_row_index
            log_info(f"結構識別成功：判斷為CSV，表頭在第 {header_row_index} 行。")
            return recipe
        else:
            log_warning("結構識別失敗：未找到符合條件的表頭。")
            return None

    except Exception as e:
        log_error(f"結構識別時發生未知錯誤: {e}")
        return None


def main(source_dir, db_path, metadata_path):
    log_info("--- 高速載入器 (v36.0 - 普羅米修斯版) 啟動 ---")

    # --- 連接元數據資料庫 (作戰日誌系統) ---
    meta_conn = duckdb.connect(database=metadata_path, read_only=False)
    meta_conn.execute("""
        CREATE TABLE IF NOT EXISTS format_recipes (
            file_hash VARCHAR PRIMARY KEY,
            recipe_json VARCHAR,
            first_seen TIMESTAMP
        );
    """)
    meta_conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_log (
            file_hash VARCHAR PRIMARY KEY,
            file_name VARCHAR,
            processed_at TIMESTAMP
        );
    """)

    # --- 連接原始數據艙 ---
    raw_conn = duckdb.connect(database=db_path, read_only=False)
    raw_conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_import_log (
            file_hash VARCHAR PRIMARY KEY,
            source_file VARCHAR,
            file_content_as_text TEXT,
            parsing_recipe_json VARCHAR,
            loaded_at TIMESTAMP
        );
    """)

    # --- 掃描輸入目錄 ---
    log_info(f"掃描輸入目錄: {source_dir}")
    for root, _, files in os.walk(source_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            log_info(f"--- 開始處理檔案: {filename} ---")

            try:
                with open(file_path, 'rb') as f:
                    content_bytes = f.read()

                # 處理 .zip 檔案
                if filename.lower().endswith('.zip'):
                    with zipfile.ZipFile(io.BytesIO(content_bytes), 'r') as z:
                        for internal_filename in z.namelist():
                             if internal_filename.lower().endswith('.csv'):
                                 log_info(f"在ZIP中發現CSV: {internal_filename}")
                                 content_bytes = z.read(internal_filename)
                                 break # 只處理第一個CSV
            except Exception as e:
                log_error(f"讀取檔案 {filename} 時失敗: {e}")
                continue

            if not content_bytes:
                log_warning(f"檔案 {filename} 內容為空，安全跳過。")
                continue

            file_hash = calculate_sha256(content_bytes)

            # 1. 檢查是否已處理過 (SOP)
            res = meta_conn.execute("SELECT 1 FROM processed_log WHERE file_hash = ?", [file_hash]).fetchone()
            if res:
                log_info(f"檔案指紋已存在於作戰日誌，高速跳過。")
                continue

            # 2. 獲取解析配方 (核心升級)
            recipe = None
            # 2.1 嘗試從「配方快取」中調用
            recipe_res = meta_conn.execute("SELECT recipe_json FROM format_recipes WHERE file_hash = ?", [file_hash]).fetchone()
            if recipe_res and recipe_res[0]:
                recipe = json.loads(recipe_res[0])
                log_success(f"效率KPI: 成功從「配方快取」調用作戰經驗。")
            else:
                # 2.2 若無快取，則啟動「智能格式探勘官」
                log_info(f"通用性KPI: 啟動「智能格式探勘」分析新檔案...")
                recipe = determine_parsing_recipe(content_bytes)
                # 將新學到的配方存入快取
                meta_conn.execute("INSERT OR REPLACE INTO format_recipes VALUES (?, ?, ?)",
                                  [file_hash, json.dumps(recipe) if recipe else None, datetime.now()])
                if recipe:
                     log_success(f"通用性KPI: 動態學習成功，新配方已存入快取。")
                else:
                     log_warning(f"健壯性KPI: 格式探勘失敗，判定為「數據毒丸」，安全跳過。")
                     # 記錄已處理，避免重複探勘
                     meta_conn.execute("INSERT OR REPLACE INTO processed_log VALUES (?, ?, ?)", [file_hash, filename, datetime.now()])
                     continue

            # 3. 預翻譯並載入數據艙 (ELT之L)
            try:
                text_content = content_bytes.decode(recipe['encoding'])
                recipe_json = json.dumps(recipe)

                raw_conn.execute("INSERT OR REPLACE INTO raw_import_log VALUES (?, ?, ?, ?, ?)",
                                 [file_hash, filename, text_content, recipe_json, datetime.now()])
                log_success(f"檔案內容與解析配方已成功載入「原始數據艙」。")

                # 4. 記錄到最終作戰日誌
                meta_conn.execute("INSERT OR REPLACE INTO processed_log VALUES (?, ?, ?)", [file_hash, filename, datetime.now()])
                log_info(f"檔案處理完畢，指紋已記錄。")

            except Exception as e:
                log_error(f"在「預翻譯」或載入數據艙時失敗: {e}")


    log_info("--- 高速載入器所有任務執行完畢 ---")
    meta_conn.close()
    raw_conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="高速載入器 - 普羅米修斯版")
    parser.add_argument("--source-dir", required=True, help="包含輸入檔案的源目錄")
    parser.add_argument("--db-path", required=True, help="原始數據艙 (raw_taifex.duckdb) 的路徑")
    parser.add_argument("--metadata-path", required=True, help="元數據與配方快取 (pipeline_metadata.duckdb) 的路徑")
    args = parser.parse_args()
    main(args.source_dir, args.db_path, args.metadata_path)
