# -*- coding: utf-8 -*-
# 高速載入器 (前身：數據精煉廠) v22.0 - 整合指紋驗證與預翻譯
import os
import sys
import argparse
import asyncio
from typing import List, Dict, Optional, Any
import hashlib
import duckdb
import pandas as pd
import io
import zipfile
import pytz
from datetime import datetime

# --- 路徑自我校正樣板碼 ---
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.dirname(current_dir)
    project_root = os.path.dirname(apps_dir)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
except Exception as e:
    print(f"專案路徑校正時發生錯誤: {e}", file=sys.stderr)
# --- 路徑自我校正樣板碼結束 ---

from apps.pipeline_metadata_manager.manager import MetadataManager, calculate_file_fingerprint

# --- 全域日誌與配置 ---
class SimpleLogger:
    def __init__(self, level="INFO"):
        self.level = level.upper()
        self.levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}

    def _log(self, msg, level):
        if self.levels.get(level, 0) >= self.levels.get(self.level, 20):
            timestamp = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] [{level}] {msg}")

    def debug(self, msg): self._log(msg, "DEBUG")
    def info(self, msg): self._log(msg, "INFO")
    def warning(self, msg): self._log(msg, "WARNING")
    def error(self, msg): self._log(msg, "ERROR")
    def success(self, msg): self._log(f"✅ {msg}", "INFO")

logger = SimpleLogger()

RAW_TABLE_NAME = "raw_import_log"

def get_raw_content_from_zip(file_path: str) -> Optional[Dict[str, bytes]]:
    try:
        contents = {}
        with zipfile.ZipFile(file_path, 'r') as zf:
            for member in zf.infolist():
                if not member.is_dir():
                    contents[member.filename] = zf.read(member.filename)
        return contents
    except zipfile.BadZipFile:
        logger.error(f"檔案不是一個有效的 ZIP 檔案: {file_path}")
        return None
    except Exception as e:
        logger.error(f"讀取 ZIP 檔案時發生錯誤 {file_path}: {e}")
        return None

async def load_raw_data_to_db(db_conn: duckdb.DuckDBPyConnection, file_path: str):
    file_name = os.path.basename(file_path)
    is_zip = file_name.lower().endswith('.zip')

    try:
        def decode_content(content_bytes: bytes) -> Optional[str]:
            for encoding in ['big5', 'ms950', 'utf-8']: # 嘗試常用編碼
                try:
                    return content_bytes.decode(encoding)
                except UnicodeDecodeError:
                    continue
            logger.warning(f"無法使用 BIG5, MS950, UTF-8 解碼檔案內容: {file_name} (部分內容可能無法正確解碼)")
            # 如果所有嘗試都失敗，可以選擇返回 None，或者帶有替換字元的字串
            return content_bytes.decode('utf-8', errors='replace')


        if is_zip:
            zip_contents = get_raw_content_from_zip(file_path) # 已修正：移除 await
            if not zip_contents:
                logger.warning(f"ZIP 檔案為空或無法讀取: {file_name}")
                return

            for member_name, content_bytes in zip_contents.items():
                decoded_text = decode_content(content_bytes)
                db_conn.execute(
                    f"INSERT INTO {RAW_TABLE_NAME} (source_file, member_file, file_content_blob, file_content_as_text, processed_at) VALUES (?, ?, ?, ?, ?)",
                    [file_name, member_name, content_bytes, decoded_text, datetime.now(pytz.utc)]
                )
            logger.info(f"成功將 ZIP 檔案 '{file_name}' 中的 {len(zip_contents)} 個成員載入到原始數據艙。")
        else:
            with open(file_path, 'rb') as f:
                content_bytes = f.read()
            decoded_text = decode_content(content_bytes)
            db_conn.execute(
                f"INSERT INTO {RAW_TABLE_NAME} (source_file, member_file, file_content_blob, file_content_as_text, processed_at) VALUES (?, ?, ?, ?, ?)",
                [file_name, None, content_bytes, decoded_text, datetime.now(pytz.utc)]
            )
            logger.info(f"成功將檔案 '{file_name}' 載入到原始數據艙。")

    except Exception as e:
        logger.error(f"將檔案 '{file_name}' 載入到資料庫時失敗: {e}")
        raise

async def main():
    parser = argparse.ArgumentParser(description="TAIFEX 高速載入器 (v22.0 - 含預翻譯)")
    parser.add_argument("--input-files", nargs='+', required=True, help="要處理的輸入檔案路徑列表。")
    parser.add_argument("--db-output-dir", required=True, help="資料庫檔案的輸出目錄。")
    parser.add_argument("--db-name", default="raw_taifex.duckdb", help="原始數據艙資料庫名稱。")
    parser.add_argument("--metadata-db-path", help="元數據資料庫的完整路徑。")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()
    logger.level = args.log_level.upper()

    db_path = os.path.join(args.db_output_dir, args.db_name)
    metadata_db_path = args.metadata_db_path or os.path.join(args.db_output_dir, "pipeline_metadata.duckdb")

    logger.info("--- TAIFEX 高速載入器 v22.0 (含預翻譯) 啟動 ---")
    logger.info(f"原始數據艙位置: {db_path}")
    logger.info(f"元數據資料庫位置: {metadata_db_path}")

    try:
        raw_db_conn = duckdb.connect(database=db_path)
        # 修正：先建立 SEQUENCE
        raw_db_conn.execute(f"CREATE SEQUENCE IF NOT EXISTS seq_raw_import;")
        raw_db_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE_NAME} (
            id UBIGINT PRIMARY KEY DEFAULT nextval('seq_raw_import'),
            source_file VARCHAR,
            member_file VARCHAR,
            file_content_blob BLOB,
            file_content_as_text TEXT, -- 新增的預翻譯欄位
            processed_at TIMESTAMPTZ
        );
        """)
        meta_manager = MetadataManager(metadata_db_path)
    except Exception as e:
        logger.error(f"資料庫或元數據管理器初始化失敗: {e}")
        return

    total_files = len(args.input_files)
    processed_count = 0
    skipped_count = 0

    for file_path in args.input_files:
        if not os.path.exists(file_path):
            logger.warning(f"檔案不存在，跳過: {file_path}")
            continue
        try:
            fingerprint = calculate_file_fingerprint(file_path) # 已修正
            if meta_manager.check_fingerprint_exists(fingerprint):
                logger.info(f"偵測到已處理檔案 (指紋: {fingerprint[:8]}...), 跳過: {os.path.basename(file_path)}")
                skipped_count += 1
                continue

            logger.info(f"處理新檔案: {os.path.basename(file_path)}")
            await load_raw_data_to_db(raw_db_conn, file_path) # await 保留，因為 load_raw_data_to_db 是 async
            meta_manager.write_fingerprint( # 已修正
                fingerprint=fingerprint,
                filename=os.path.basename(file_path),
                filesize=os.path.getsize(file_path),
                etl_version="v35.0-loader-v2"
            )
            processed_count += 1
        except Exception as e:
            logger.error(f"處理檔案 {file_path} 時發生未預期的嚴重錯誤: {e}")

    raw_db_conn.close()
    logger.info("--- 作戰總結 ---")
    logger.success(f"任務完成。總計檔案: {total_files} | 新處理: {processed_count} | 已跳過: {skipped_count}")

if __name__ == "__main__":
    asyncio.run(main())
