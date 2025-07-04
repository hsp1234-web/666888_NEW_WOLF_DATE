# -*- coding: utf-8 -*-
# 數據轉換器 (Transformer) v1.2 - Python 迭代 + 核心解析
import os
import sys
import argparse
import duckdb
import pandas as pd
from io import StringIO
from datetime import datetime
import pytz
import time
import json

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

def create_target_table(conn: duckdb.DuckDBPyConnection, table_name: str):
    """確保目標分析表格存在"""
    conn.execute(f"CREATE SEQUENCE IF NOT EXISTS seq_{table_name};")
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id UBIGINT PRIMARY KEY DEFAULT nextval('seq_{table_name}'),
        trading_date DATE,
        product_id VARCHAR,
        expiry_month VARCHAR,
        strike_price DOUBLE,
        option_type VARCHAR,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        settlement_price DOUBLE,
        volume UBIGINT,
        open_interest UBIGINT,
        trading_session VARCHAR,
        source_file VARCHAR,
        member_file VARCHAR,
        transformed_at TIMESTAMPTZ
    );
    """)

def transform_and_load(raw_content_df: pd.DataFrame, target_conn: duckdb.DuckDBPyConnection, target_table: str, enable_status_updates: bool = False):
    """在 Python 中處理文本內容，並將結構化數據載入目標資料庫"""
    all_clean_dfs = []
    total_files = len(raw_content_df)

    for index, row in raw_content_df.iterrows():
        if enable_status_updates:
            status = {
                "progress": index + 1,
                "total": total_files,
                "message": f"正在轉換檔案 {index + 1}/{total_files} ({row.get('source_file', 'N/A')}/{row.get('member_file', 'N/A')})..."
            }
            print(f"##STATUS##{json.dumps(status, ensure_ascii=False)}")
            sys.stdout.flush()

        content_text = row['file_content_as_text']
        source_file = row['source_file']
        member_file = row['member_file']

        if not content_text or not content_text.strip():
            logger.warning(f"跳過空的文本內容 (來源: {source_file}/{member_file})")
            continue

        try:
            # 使用 Pandas 強大的 CSV 解析器
            df = pd.read_csv(
                StringIO(content_text),
                encoding='utf-8', # 此時應為 UTF-8
                thousands=',', # 處理數字中的逗號
                low_memory=False,
                dtype=str # 將所有欄位先讀取為字串，避免 Pandas 自動推斷類型錯誤
            )
            # 清理欄位名中的空格，並將其轉換為標準的 snake_case 或保持原樣以便後續 .get()
            df.columns = [col.strip().replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]

            # 緊急修復：兼容 '成交日期' 欄位
            # 在 pd.read_csv(...) 之後，且欄位名稱清理之後
            if '成交日期' in df.columns and '交易日期' not in df.columns:
                df.rename(columns={'成交日期': '交易日期'}, inplace=True)
                logger.info(f"成功將欄位 '成交日期' 兼容為 '交易日期' (來源: {source_file}/{member_file})")
            elif '成交日期' in df.columns and '交易日期' in df.columns:
                # 理論上不應該同時存在，但若存在，優先使用 '交易日期'，並記錄警告
                logger.warning(f"欄位 '成交日期' 和 '交易日期' 同時存在於 {source_file}/{member_file}。將優先使用 '交易日期'。")
            # 如果只有 '交易日期'，則什麼都不做，流程正常

            logger.debug(f"DataFrame columns after cleaning and compatibility fix for {source_file}/{member_file}: {list(df.columns)}")
            logger.debug(f"DataFrame head for {source_file}/{member_file}:\n{df.head().to_string()}")

            trading_date_series = df.get('交易日期') # 假設清理後的欄位名仍然是 '交易日期'
            if trading_date_series is None:
                logger.error(f"'交易日期' column not found in df for {source_file}/{member_file} after column cleaning. Available columns: {list(df.columns)}")
                # 嘗試使用原始可能的欄位名（如果清理邏輯有變）
                # trading_date_series = df.get('交易日期') # 這裡的 get 應該基於清理後的名稱
                # 如果真的找不到，就無法繼續處理這個檔案的日期
                continue
            else:
                logger.debug(f"'交易日期' series for {source_file}/{member_file}:\n{trading_date_series.to_string()}")

            # 精準指令：指定 YYYYMMDD 格式
            parsed_dates_series = pd.to_datetime(trading_date_series, format='%Y%m%d', errors='coerce').dt.date
            logger.debug(f"parsed_dates_series for {source_file}/{member_file}:\n{parsed_dates_series.to_string()}")

            df['parsed_trading_date'] = parsed_dates_series
            df_filtered = df[pd.notna(df['parsed_trading_date'])].copy()
            logger.debug(f"df_filtered head for {source_file}/{member_file} (Non-NaT dates):\n{df_filtered.head().to_string()}")


            if df_filtered.empty:
                logger.warning(f"在 {source_file}/{member_file} 中，所有有效數據行都因日期轉換失敗或不存在而被過濾。")
                continue

            # 現在 df_transformed 基於 df_filtered 建立
            df_transformed = pd.DataFrame()
            df_transformed['trading_date'] = df_filtered['parsed_trading_date']

            # 後續的 df.get('契約代碼') 應該改為 df_filtered.get('契約代碼')
            df_transformed['product_id'] = df_filtered.get('契約代碼')
            df_transformed['expiry_month'] = df_filtered.get('到期月份週別')
            df_transformed['strike_price'] = pd.to_numeric(df_filtered.get('履約價'), errors='coerce')
            df_transformed['option_type'] = df_filtered.get('買賣權')
            df_transformed['open'] = pd.to_numeric(df_filtered.get('開盤價'), errors='coerce')
            df_transformed['high'] = pd.to_numeric(df_filtered.get('最高價'), errors='coerce')
            df_transformed['low'] = pd.to_numeric(df_filtered.get('最低價'), errors='coerce')
            df_transformed['close'] = pd.to_numeric(df_filtered.get('收盤價'), errors='coerce')
            df_transformed['settlement_price'] = pd.to_numeric(df_filtered.get('結算價'), errors='coerce')

            volume_series = df_filtered.get('成交量')
            if volume_series is not None:
                df_transformed['volume'] = pd.to_numeric(volume_series.astype(str).str.replace(',', '', regex=False), errors='coerce').astype('Int64')
            else:
                df_transformed['volume'] = pd.Series(dtype='Int64')

            oi_series = df_filtered.get('未沖銷契約數')
            if oi_series is not None:
                df_transformed['open_interest'] = pd.to_numeric(oi_series.astype(str).str.replace(',', '', regex=False), errors='coerce').astype('Int64')
            else:
                df_transformed['open_interest'] = pd.Series(dtype='Int64')

            df_transformed['trading_session'] = df_filtered.get('交易時段')

            df_transformed['source_file'] = source_file # source_file 和 member_file 是循環變數，不是來自 df_filtered
            df_transformed['member_file'] = member_file
            df_transformed['transformed_at'] = datetime.now(pytz.utc)

            if not df_transformed.empty:
                all_clean_dfs.append(df_transformed)

        except Exception as e:
            logger.error(f"解析文本內容時失敗 (來源: {source_file}/{member_file}): {e}")
            import traceback
            logger.error(traceback.format_exc())


    if all_clean_dfs:
        final_df = pd.concat(all_clean_dfs, ignore_index=True)
        logger.info(f"準備將 {len(final_df)} 筆已轉換的記錄載入到 '{target_table}'...")

        # 確保 final_df 中的欄位順序與目標表一致 (如果使用 BY POSITION)
        # 或者使用 BY NAME (DuckDB 0.7.0+ 支持 INSERT INTO table BY NAME SELECT ...)
        try:
            # 為了最大的相容性和明確性，我們可以明確指定欄位列表
            # DuckDB's executemany is not directly for pandas DataFrames in the same way as to_sql.
            # We use DuckDB's ability to query pandas DataFrames directly.
            target_conn.register('final_df_view', final_df)
            target_conn.execute(f"""
                INSERT INTO {target_table} (
                    trading_date, product_id, expiry_month, strike_price, option_type,
                    open, high, low, close, settlement_price, volume, open_interest,
                    trading_session, source_file, member_file, transformed_at
                ) SELECT
                    trading_date, product_id, expiry_month, strike_price, option_type,
                    open, high, low, close, settlement_price, volume, open_interest,
                    trading_session, source_file, member_file, transformed_at
                FROM final_df_view
            """)
            target_conn.unregister('final_df_view')
            return len(final_df)
        except Exception as e:
            logger.error(f"將數據載入到 DuckDB 時發生錯誤: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0

    return 0


def main():
    parser = argparse.ArgumentParser(description="TAIFEX 數據轉換器 (v35.0 - Python 迭代模式)")
    parser.add_argument("--raw-db-path", required=True, help="原始數據艙資料庫的路徑。")
    parser.add_argument("--analytics-db-path", required=True, help="最終分析資料庫的路徑。")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--enable-status-updates", action="store_true", help="啟用狀態更新通訊協議")

    args = parser.parse_args()
    logger.level = args.log_level.upper()

    logger.info("--- TAIFEX 數據轉換器 v35.0 (Python 迭代模式) 啟動 ---")
    logger.info(f"讀取自「原始數據艙」: {args.raw_db_path}")
    logger.info(f"寫入至「分析數據庫」: {args.analytics_db_path}")

    raw_conn = None
    analytics_conn = None
    try:
        # 調整 raw_conn 的連接邏輯，以便處理記憶體資料庫
        if args.raw_db_path.lower() == "memory" or args.raw_db_path == ":memory:":
            logger.info(f"使用記憶體原始數據艙 (非只讀模式初始連接)。")
            raw_conn = duckdb.connect(database=":memory:") # 首次連接記憶體資料庫不能是只讀
        else:
            logger.info(f"從檔案系統連接原始數據艙 (只讀模式): {args.raw_db_path}")
            raw_conn = duckdb.connect(database=args.raw_db_path, read_only=True)

        analytics_conn = duckdb.connect(database=args.analytics_db_path) # 分析資料庫通常需要寫入

        target_table = "daily_ohlc"
        create_target_table(analytics_conn, target_table) # 確保表和序列存在
        logger.success(f"成功連接資料庫並確保目標表 '{target_table}' 存在。")

        logger.info("正在從原始數據艙獲取待處理內容...")
        # 只選擇必要的欄位
        raw_content_df = raw_conn.execute("SELECT source_file, member_file, file_content_as_text FROM raw_import_log WHERE file_content_as_text IS NOT NULL AND file_content_as_text != ''").fetchdf()

        if raw_content_df.empty:
            logger.warning("在原始數據艙中未找到任何待處理的文本內容。")
            inserted_count = 0
        else:
            logger.info(f"發現 {len(raw_content_df)} 筆原始文本記錄，開始轉換與載入...")
            start_time = time.time()
            inserted_count = transform_and_load(raw_content_df, analytics_conn, target_table, args.enable_status_updates)
            duration = time.time() - start_time
            logger.success(f"轉換與載入流程完成，耗時: {duration:.2f} 秒。")

        logger.info(f"驗證：共 {inserted_count} 筆乾淨數據被載入到分析資料庫。")

    except Exception as e:
        logger.error(f"數據轉換過程中發生嚴重錯誤: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if raw_conn: raw_conn.close()
        if analytics_conn: analytics_conn.close()
        logger.info("資料庫連接已關閉。")

    logger.info("--- TAIFEX 數據轉換器任務完成 ---")

if __name__ == "__main__":
    main()
