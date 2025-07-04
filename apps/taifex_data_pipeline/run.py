# -*- coding: utf-8 -*-
# 精煉廠主執行檔 (v20.4 CLI Enabled)
import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
import sys
import json
import hashlib
import warnings
import shutil
import io
import re
import time
import argparse
from datetime import datetime
from typing import Generator, Tuple, Dict, Any, Optional, List, AsyncGenerator, Union
import asyncio
import concurrent.futures
import pathlib

import pandas as pd
import psutil
import pyarrow
import duckdb
import pytz
import zipfile
import csv
import codecs

# --- Start of Integrated Stream Unzipper Logic (v20.3) ---
class InMemoryStreamUnzipper:
    def __init__(self, zip_stream_reader, chunk_size: int = 8192):
        self._zip_stream_reader = zip_stream_reader
        self._buffer = io.BytesIO()
        self._chunk_size = chunk_size
        self._first_file_name: Optional[str] = None

    async def _load_zip_to_buffer(self) -> bool:
        if not self._zip_stream_reader:
            print("InMemoryStreamUnzipper: _load_zip_to_buffer: zip_stream_reader 為空。", file=sys.stderr)
            return False
        if not hasattr(self._zip_stream_reader, '__aiter__') or not hasattr(self._zip_stream_reader, '__anext__'):
             print("InMemoryStreamUnzipper: zip_stream_reader 不是一個有效的 AsyncGenerator。", file=sys.stderr)
             return False
        try:
            async for chunk in self._zip_stream_reader:
                if chunk: self._buffer.write(chunk)
            self._buffer.seek(0)
            if self._buffer.getbuffer().nbytes == 0:
                print("InMemoryStreamUnzipper: ZIP 串流讀取完畢，但緩衝區為空。", file=sys.stderr)
                return False
            return True
        except Exception as e:
            print(f"InMemoryStreamUnzipper: 從串流讀取 ZIP 數據時發生錯誤: {e}", file=sys.stderr)
            return False

    async def get_uncompressed_stream(self) -> Optional[AsyncGenerator[bytes, None]]:
        if not await self._load_zip_to_buffer():
            print("InMemoryStreamUnzipper: get_uncompressed_stream: _load_zip_to_buffer 失敗或緩衝區為空。", file=sys.stderr)
            self.close()
            return None
        try:
            if not zipfile.is_zipfile(self._buffer):
                print("InMemoryStreamUnzipper: 提供的串流不是有效的 ZIP 檔案 (is_zipfile 返回 False)。", file=sys.stderr)
                self.close()
                return None
            self._buffer.seek(0)
            with zipfile.ZipFile(self._buffer, 'r') as zf:
                namelist = zf.namelist()
                if not namelist:
                    print("InMemoryStreamUnzipper: ZIP 檔案為空（不包含任何檔案）。", file=sys.stderr)
                    return None
                self._first_file_name = namelist[0]
                # 篩選掉 __MACOSX 和 .DS_Store 等元數據檔案
                valid_files = [f for f in namelist if not f.startswith('__MACOSX/') and not f.endswith('.DS_Store') and not zf.getinfo(f).is_dir()]
                if not valid_files:
                    print("InMemoryStreamUnzipper: ZIP 檔案中未找到有效的數據檔案。", file=sys.stderr)
                    return None
                self._first_file_name = valid_files[0]
                print(f"InMemoryStreamUnzipper: 正在解壓縮 ZIP 檔案中的第一個有效檔案: {self._first_file_name}", file=sys.stdout)
                member_file_bytes = io.BytesIO(zf.read(self._first_file_name))

            async def async_generator_wrapper():
                try:
                    while True:
                        chunk = member_file_bytes.read(self._chunk_size)
                        if not chunk: break
                        yield chunk
                        await asyncio.sleep(0)
                finally:
                    if member_file_bytes: member_file_bytes.close()
            return async_generator_wrapper()
        except zipfile.BadZipFile:
            print("InMemoryStreamUnzipper: 捕獲到 BadZipFile 錯誤。", file=sys.stderr)
            self.close()
            return None
        except Exception as e:
            print(f"InMemoryStreamUnzipper: get_uncompressed_stream 中發生未預期錯誤: {e}", file=sys.stderr)
            self.close()
            return None

    def close(self) -> None:
        if self._buffer:
            print(f"InMemoryStreamUnzipper: 正在關閉 BytesIO 緩衝區 (用於 {self._first_file_name or '未知 ZIP 檔案'})。", file=sys.stdout)
            self._buffer.close()
            self._buffer = None # type: ignore
# --- End of Integrated Stream Unzipper Logic ---

# --- Path Correction (No changes needed from previous version) ---
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    apps_dir = os.path.dirname(current_dir)
    project_root = os.path.dirname(apps_dir)
    if apps_dir not in sys.path: sys.path.insert(0, apps_dir)
    if project_root not in sys.path: sys.path.insert(0, project_root)
except Exception as e:
    print(f"路徑校正時發生錯誤: {e}", file=sys.stderr)
# --- End Path Correction ---

warnings.filterwarnings("ignore", message=".*_PyDriveImportHook.find_spec.*")

class SimpleLogger:
    def __init__(self, tz_str: str = 'Asia/Taipei', log_level: str = "INFO"):
        self.tz = pytz.timezone(tz_str)
        self.log_level_map = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
        self.current_log_level = self.log_level_map.get(log_level.upper(), 20)
    def _get_timestamp(self) -> str: return datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    def _log(self, message: str, level: str, details: str = ""):
        if self.log_level_map.get(level.upper(), 0) >= self.current_log_level:
            print(f"[{self._get_timestamp()}] [{level.upper()}] {message} {details}")
    def debug(self, m, d=""): self._log(m, "DEBUG", d)
    def info(self, m, d=""): self._log(m, "INFO", d)
    def success(self, m, d=""): self._log(m, "INFO", f"✅ {d}")
    def warning(self, m, d=""): self._log(m, "WARNING", f"⚠️ {d}")
    def error(self, m, d=""): self._log(m, "ERROR", f"❌ {d}")
    def header(self, m): self.info(f"\n{'='*60}\n=== {m.strip()} ===\n{'='*60}")
    def section(self, m): self.info(f"\n--- {m.strip()} ---")
    def hw_log(self, m, p="[HW_MONITOR]"): self.info(m, p)

logger: SimpleLogger = SimpleLogger() # Will be re-initialized in async_main based on CLI args

class HardwareManager: # (No changes needed from previous version)
    def __init__(self, user_max_workers: Optional[int] = None, user_memory_limit_gb: Optional[int] = None):
        self.cpu_cores = os.cpu_count() or 2
        self.total_ram_gb = psutil.virtual_memory().total / (1024**3)
        self.max_workers = user_max_workers if user_max_workers is not None else max(1, round(self.cpu_cores * 0.8))
        self.memory_limit_gb = user_memory_limit_gb if user_memory_limit_gb is not None else int(self.total_ram_gb * 0.5)
    def get_status_line(self) -> str:
        cpu_percent = psutil.cpu_percent(); ram = psutil.virtual_memory(); disk = psutil.disk_usage('/')
        return f"CPU: {cpu_percent:.1f}% | RAM: {ram.percent:.1f}% ({ram.used/(1024**3):.2f}/{self.total_ram_gb:.2f} GB) | Disk: {disk.percent:.1f}%"
    def display_initial_dashboard(self):
        logger.header("硬體狀態與執行參數"); logger.info(f"CPU 核心數: {self.cpu_cores}"); logger.info(f"總記憶體: {self.total_ram_gb:.2f} GB")
        logger.info(f"並行處理核心數 (max_workers): {self.max_workers}"); logger.info(f"DuckDB 記憶體預算 (memory_limit): {self.memory_limit_gb} GB"); logger.info(self.get_status_line())
    def log_event_snapshot(self, event_name: str): logger.hw_log(f"[{event_name}] {self.get_status_line()}", "[HW_SNAPSHOT]")

# --- Table Definitions, Sequences, Unique Indices, Manual Column Names (No changes needed) ---
TABLE_DEFINITIONS = {
    'raw_taifex_data': "CREATE TABLE IF NOT EXISTS raw_taifex_data (id UBIGINT PRIMARY KEY, trading_date DATE, product_id VARCHAR, expiry_month VARCHAR, strike_price DOUBLE, option_type VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, settlement_price DOUBLE, volume UBIGINT, open_interest UBIGINT, trading_session VARCHAR, change DOUBLE, change_percent DOUBLE, source_file VARCHAR, processed_at TIMESTAMP);",
    'daily_ohlc': "CREATE TABLE IF NOT EXISTS daily_ohlc (id UBIGINT PRIMARY KEY, trading_date DATE, product_id VARCHAR, expiry_month VARCHAR, strike_price DOUBLE, option_type VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, settlement_price DOUBLE, volume UBIGINT, open_interest UBIGINT, trading_session VARCHAR, change DOUBLE, change_percent DOUBLE, source VARCHAR);",
    'tick_data': "CREATE TABLE IF NOT EXISTS tick_data (id UBIGINT PRIMARY KEY, trade_datetime TIMESTAMP, product_id VARCHAR, expiry_month VARCHAR, strike_price DOUBLE, option_type VARCHAR, price DOUBLE, volume UBIGINT, source VARCHAR);",
    'institutional_investors': "CREATE TABLE IF NOT EXISTS institutional_investors (id UBIGINT PRIMARY KEY, data_date DATE, product_name VARCHAR, investor_type VARCHAR, instrument_type VARCHAR, option_type VARCHAR, long_pos_vol BIGINT, long_pos_val_twd_k BIGINT, short_pos_vol BIGINT, short_pos_val_twd_k BIGINT, net_pos_vol BIGINT, net_pos_val_twd_k BIGINT, long_oi_vol BIGINT, long_oi_val_twd_k BIGINT, short_oi_vol BIGINT, short_oi_val_twd_k BIGINT, net_oi_vol BIGINT, net_oi_val_twd_k BIGINT, source VARCHAR);",
    'pcr': "CREATE TABLE IF NOT EXISTS pcr (id UBIGINT PRIMARY KEY, data_date DATE, put_volume UBIGINT, call_volume UBIGINT, pcr_volume DOUBLE, put_oi UBIGINT, call_oi UBIGINT, pcr_oi DOUBLE, source VARCHAR);",
    'fx_rates': "CREATE TABLE IF NOT EXISTS fx_rates (id UBIGINT PRIMARY KEY, data_date DATE, usd_twd DOUBLE, cny_twd DOUBLE, eur_usd DOUBLE, usd_jpy DOUBLE, gbp_usd DOUBLE, aud_usd DOUBLE, usd_hkd DOUBLE, usd_cny DOUBLE, usd_zar DOUBLE, nzd_usd DOUBLE, source VARCHAR);"
}
SEQUENCES = {name: f"CREATE SEQUENCE IF NOT EXISTS seq_{name};" for name in TABLE_DEFINITIONS.keys()}
UNIQUE_INDICES = { # Simplified for raw_taifex_data
    'raw_taifex_data': "CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_taifex_data_unique ON raw_taifex_data(trading_date, product_id, expiry_month, strike_price, option_type, trading_session, source_file);", # Added source_file
    'daily_ohlc': "CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_ohlc_unique ON daily_ohlc(trading_date, product_id, expiry_month, strike_price, option_type, trading_session);",
    'tick_data': "CREATE UNIQUE INDEX IF NOT EXISTS idx_tick_data_unique ON tick_data(trade_datetime, product_id, expiry_month, strike_price, option_type, price, volume);",
    'institutional_investors': "CREATE UNIQUE INDEX IF NOT EXISTS idx_inst_inv_unique ON institutional_investors(data_date, product_name, investor_type, instrument_type, option_type);",
    'pcr': "CREATE UNIQUE INDEX IF NOT EXISTS idx_pcr_unique ON pcr(data_date);",
    'fx_rates': "CREATE UNIQUE INDEX IF NOT EXISTS idx_fx_rates_unique ON fx_rates(data_date);"
}
MANUAL_COLUMN_NAMES = { # Simplified for raw_taifex_data, ensure all are present
    'futures_daily': ['trading_date', 'product_id', 'expiry_month', 'open', 'high', 'low', 'close', 'change', 'change_percent', 'volume', 'settlement_price', 'open_interest', 'last_best_bid_price', 'last_best_ask_price', 'historical_high', 'historical_low', 'is_suspended', 'trading_session', 'spread_volume'],
    'options_daily_v1': ['trading_date', 'product_id', 'expiry_month', 'strike_price', 'option_type', 'open', 'high', 'low', 'close', 'volume', 'settlement_price', 'open_interest', 'last_best_bid_price', 'last_best_ask_price', 'historical_high', 'historical_low', 'is_suspended', 'trading_session'],
    'options_daily_v2': ['trading_date', 'product_id', 'expiry_month', 'strike_price', 'option_type', 'open', 'high', 'low', 'close', 'volume', 'settlement_price', 'open_interest', 'last_best_bid_price', 'last_best_ask_price', 'historical_high', 'historical_low', 'is_suspended', 'trading_session', 'change', 'change_percent']
}
# Generic schema for raw data, ensure all columns from MANUAL_COLUMN_NAMES are covered, plus strike_price and option_type
RAW_TAIFEX_SCHEMA_COLUMNS = list(set(
    [col for cols in MANUAL_COLUMN_NAMES.values() for col in cols] +
    ['strike_price', 'option_type']
))


FORMAT_MAP_FILENAME = "format_map.json"
RECIPE_SAMPLE_SIZE_BYTES = 16384

class AsyncBytesGeneratorReader: # (No changes needed from previous version)
    def __init__(self, async_byte_generator: AsyncGenerator[bytes, None], descriptor: str = "AsyncBytesGenReader"):
        self._generator = async_byte_generator
        self._buffer = bytearray()
        self._eof = False
        self._descriptor = descriptor
        logger.debug(f"[{self._descriptor}] AsyncBytesGeneratorReader initialized.")
    async def _fill_buffer_if_needed(self, needed_bytes: int = 0) -> None:
        if self._eof: return
        if not self._buffer or (needed_bytes > 0 and len(self._buffer) < needed_bytes):
            try:
                chunk = await self._generator.__anext__()
                if chunk: self._buffer.extend(chunk)
                else: self._eof = True
            except StopAsyncIteration: self._eof = True
            except Exception as e:
                logger.error(f"[{self._descriptor}] _fill_buffer_if_needed: Error reading from generator: {e}")
                self._eof = True
    async def read(self, n: int = -1) -> bytes:
        if n == 0: return b""
        if self._eof and not self._buffer: return b""
        if n == -1:
            temp_buffer_list = [bytes(self._buffer)]
            self._buffer.clear()
            try:
                async for chunk in self._generator:
                    temp_buffer_list.append(chunk)
            except Exception as e:
                logger.error(f"[{self._descriptor}] Error consuming generator in read(-1): {e}")
            finally: self._eof = True
            return b"".join(temp_buffer_list)
        else:
            while len(self._buffer) < n and not self._eof:
                await self._fill_buffer_if_needed(n)
                if self._eof and len(self._buffer) < n: break
            data_to_return = bytes(self._buffer[:n])
            self._buffer = self._buffer[n:]
            return data_to_return
    async def readchunk(self, size: int = 8192) -> bytes: # (No changes needed)
        if self._eof and not self._buffer: return b""
        if len(self._buffer) >= size:
            data_to_return = bytes(self._buffer[:size])
            self._buffer = self._buffer[size:]
            return data_to_return
        temp_chunk_list = []
        if self._buffer:
            temp_chunk_list.append(bytes(self._buffer))
            self._buffer.clear()
        current_len = sum(len(c) for c in temp_chunk_list)
        while current_len < size and not self._eof:
            try:
                chunk = await self._generator.__anext__()
                if chunk:
                    temp_chunk_list.append(chunk)
                    current_len += len(chunk)
                else: self._eof = True; break
            except StopAsyncIteration: self._eof = True; break
            except Exception as e: logger.error(f"[{self._descriptor}] readchunk: Error reading from generator: {e}"); self._eof = True; break
        final_data = b"".join(temp_chunk_list)
        if len(final_data) > size:
            self._buffer.extend(final_data[size:])
            return final_data[:size]
        return final_data
    async def readline(self) -> bytes: # (No changes needed)
        if self._eof and not self._buffer: return b""
        line_buffer = bytearray()
        while True:
            newline_pos = self._buffer.find(b'\n')
            if newline_pos != -1:
                line_buffer.extend(self._buffer[:newline_pos + 1])
                self._buffer = self._buffer[newline_pos + 1:]
                return bytes(line_buffer)
            else:
                line_buffer.extend(self._buffer)
                self._buffer.clear()
                if self._eof: return bytes(line_buffer) if line_buffer else b""
                try:
                    chunk = await self._generator.__anext__()
                    if chunk: self._buffer.extend(chunk)
                    else: self._eof = True; return bytes(line_buffer) if line_buffer else b""
                except StopAsyncIteration: self._eof = True; return bytes(line_buffer) if line_buffer else b""
                except Exception as e: logger.error(f"[{self._descriptor}] readline: Error reading from generator: {e}"); self._eof = True; return bytes(line_buffer) if line_buffer else b""

    def at_eof(self) -> bool: return self._eof and not self._buffer
    async def release(self): # (No changes needed)
        if not self._eof:
            try:
                async for _ in self._generator: pass
            except Exception: pass
            self._eof = True
        self._buffer.clear()
        logger.debug(f"AsyncBytesGeneratorReader for '{self._descriptor}' released.")

# --- determine_parsing_recipe (Simplified to always return a generic CSV recipe for now) ---
async def determine_parsing_recipe(
    stream_reader: Union[AsyncBytesGeneratorReader, Any],
    descriptor: str,
    sample_size: int = RECIPE_SAMPLE_SIZE_BYTES
) -> Tuple[Optional[Dict[str, Any]], bytes]:
    logger.debug(f"[{descriptor}] 正在從串流讀取最多 {sample_size} 位元組用於配方判斷...")
    try:
        sample_bytes = await stream_reader.read(sample_size)
    except Exception as e:
        logger.error(f"[{descriptor}] 從串流讀取樣本數據時出錯: {e}")
        return None, b""

    if not sample_bytes:
        logger.warning(f"[{descriptor}] 從串流讀取的樣本數據為空。")
        return None, sample_bytes

    # For simplicity in this iteration, assume all CSVs are similar enough
    # and try to detect encoding.
    detected_encoding = 'ms950'
    try:
        sample_bytes.decode('ms950')
    except UnicodeDecodeError:
        try:
            detected_encoding = 'utf-8-sig'
            sample_bytes.decode(detected_encoding)
        except UnicodeDecodeError:
            detected_encoding = 'utf-8'
            try:
                sample_bytes.decode(detected_encoding)
            except UnicodeDecodeError:
                logger.warning(f"檔案 {descriptor} 樣本數據解碼失敗 (嘗試了 ms950, utf-8-sig, utf-8)。")
                return {"parser": "unknown_encoding", "args": {}, "pipeline": "raw_taifex_data"}, sample_bytes

    # Simplified: assume CSV with header at row 0, or no header if it looks like data
    # This part needs careful thought for robust header detection or relying on manual_cols
    # For now, let's assume it's CSV and we'll use manual_cols based on some heuristic or always.
    # Let's try to make it always use a generic CSV parser that will be mapped to raw_taifex_data
    # The actual column mapping will happen in the row processor.

    # Heuristic: if first line contains typical header keywords, assume header=0
    # Otherwise, assume no header (header=None) and we will rely on manual column mapping later.
    header_line_raw = ""
    try:
        temp_text = sample_bytes.decode(detected_encoding)
        first_newline = temp_text.find('\n')
        header_line_raw = temp_text[:first_newline if first_newline != -1 else len(temp_text)].strip()
    except:
        pass # ignore if decoding fails here

    header_option = 0 # Default to assuming header is present at row 0
    # Simple check, can be improved
    if not any(kw in header_line_raw for kw in ['日期', '契約', '商品', '買賣權', '價格']):
        # If common header keywords are NOT in the first line, assume no header
        # header_option = None # This would make pd.read_csv use default int headers
        # For our generic approach, we'll still use header=0 and let the row processor handle it
        # Or, better, use a names_key that maps to RAW_TAIFEX_SCHEMA_COLUMNS
        pass


    # The pipeline will always be 'raw_taifex_data' for this simplified version
    # The parser type will be 'csv_generic'
    # The args will include detected encoding and skipinitialspace
    # The critical part is that parse_content_to_arrow will use PIPELINE_ROW_PROCESSORS['raw_taifex_data']

    generic_csv_args = {
        "encoding": detected_encoding,
        "skipinitialspace": True,
        "thousands": ',',
        "dtype": "str", # Read everything as string first
        "on_bad_lines": "warn",
        "header": header_option, # Let pandas try to infer header, or use 0
        # "names": RAW_TAIFEX_SCHEMA_COLUMNS, # Provide all possible columns, let processor pick
        # "usecols": lambda x: x in RAW_TAIFEX_SCHEMA_COLUMNS # Only read known columns
    }

    # If we are confident it's one of the known manual formats, use that
    # This requires more sophisticated sniffing than the current simplified approach
    # For now, we use a generic CSV recipe and let the row processor map fields.
    # This is a simplification to get the CLI working.

    logger.info(f"[{descriptor}] 使用通用 CSV 配方，編碼: {detected_encoding}, pipeline: raw_taifex_data")
    return {"parser": "csv_generic", "args": generic_csv_args, "pipeline": "raw_taifex_data"}, sample_bytes


# --- _iterate_text_lines, async_generate_rows_from_csv (Modified for generic CSV) ---
async def _iterate_text_lines( # (No changes needed from previous version)
    stream_reader: Union[AsyncBytesGeneratorReader, Any],
    consumed_sample_bytes: bytes,
    encoding: str,
    descriptor: str,
    chunk_size: int = 8192
) -> AsyncGenerator[str, None]:
    decoder = codecs.getincrementaldecoder(encoding)(errors='replace')
    buffer = ""
    if consumed_sample_bytes:
        try: buffer += decoder.decode(consumed_sample_bytes, final=False)
        except UnicodeDecodeError as e:
            logger.warning(f"[{descriptor}] 解碼 consumed_sample_bytes 時出錯: {e}.")
            try: buffer += consumed_sample_bytes.decode(encoding, errors='replace')
            except Exception as e_replace: logger.error(f"[{descriptor}] consumed_sample_bytes 無法用 '{encoding}' (errors='replace') 解碼: {e_replace}，將被忽略。")
    while '\n' in buffer:
        line, _, buffer = buffer.partition('\n')
        yield line.rstrip('\r')
    read_method_to_use = None
    if hasattr(stream_reader, 'readchunk') and callable(stream_reader.readchunk): read_method_to_use = stream_reader.readchunk
    elif hasattr(stream_reader, 'read') and callable(stream_reader.read): read_method_to_use = stream_reader.read # type: ignore
    if not read_method_to_use:
        logger.warning(f"[{descriptor}] _iterate_text_lines: stream_reader 沒有有效的 readchunk 或 read 方法。")
        if buffer: yield buffer.rstrip('\r')
        return
    while True:
        try:
            chunk = await read_method_to_use(chunk_size)
            if not chunk: break
            buffer += decoder.decode(chunk, final=False)
            while '\n' in buffer:
                line, _, buffer = buffer.partition('\n')
                yield line.rstrip('\r')
        except Exception as e:
            logger.error(f"[{descriptor}] 從串流讀取或解碼數據塊時出錯 (_iterate_text_lines): {e}")
            try:
                final_chunk_from_decoder_on_error = decoder.decode(b'', final=True)
                if final_chunk_from_decoder_on_error: buffer += final_chunk_from_decoder_on_error
            except Exception as e_final_decode: logger.error(f"[{descriptor}] 清理解碼器時發生額外錯誤: {e_final_decode}")
            break
    final_chunk_from_decoder = decoder.decode(b'', final=True)
    if final_chunk_from_decoder: buffer += final_chunk_from_decoder
    if buffer:
        while '\n' in buffer:
            line, _, buffer = buffer.partition('\n')
            yield line.rstrip('\r')
        if buffer: yield buffer.rstrip('\r')

async def async_generate_rows_from_generic_csv(
    stream_reader: Union[AsyncBytesGeneratorReader, Any],
    consumed_sample_bytes: bytes,
    recipe_args: Dict[str, Any],
    descriptor: str
) -> AsyncGenerator[Dict[str, Any], None]:
    encoding = recipe_args.get("encoding", "utf-8")
    skip_initial_space = recipe_args.get("skipinitialspace", True)
    header_option = recipe_args.get("header") # This could be 0 or None

    line_iterator = _iterate_text_lines(stream_reader, consumed_sample_bytes, encoding, descriptor)
    current_line_num_for_log = 0
    header_list_cleaned: Optional[List[str]] = None

    try:
        if header_option == 0: # If pandas was told to expect a header at row 0
            try:
                header_line_text = await line_iterator.__anext__()
                current_line_num_for_log +=1
                header_list_raw = next(csv.reader([header_line_text], skipinitialspace=skip_initial_space))
                header_list_cleaned = [str(col).strip().replace(' ', '_').replace('(', '').replace(')', '') for col in header_list_raw]
                logger.debug(f"[{descriptor}] Parsed header: {header_list_cleaned}")
            except StopAsyncIteration:
                logger.warning(f"[{descriptor}] 無法讀取 CSV 表頭行。")
                return
            except csv.Error as e_csv_header:
                logger.error(f"[{descriptor}] 解析 CSV 表頭行 '{header_line_text[:100]}...' 時出錯: {e_csv_header}") # type: ignore
                return
        # If header_option is None, pandas would use default int headers. We will create dicts with int keys.
        # However, our row processor for raw_taifex_data will try to map known column names.

        async for line_text in line_iterator:
            current_line_num_for_log += 1
            if not line_text.strip(): continue
            try:
                row_values = next(csv.reader([line_text], skipinitialspace=skip_initial_space))
            except csv.Error as e_csv_line:
                logger.warning(f"[{descriptor}] CSV generic 解析行 '{line_text[:100]}...' (數據行號 {current_line_num_for_log}) 時出錯: {e_csv_line}")
                continue

            if not any(field and field.strip() for field in row_values): continue

            if header_list_cleaned: # Header was parsed
                # Pad row_values if shorter than header, truncate if longer
                if len(row_values) < len(header_list_cleaned):
                    row_values.extend([None] * (len(header_list_cleaned) - len(row_values)))
                elif len(row_values) > len(header_list_cleaned):
                    row_values = row_values[:len(header_list_cleaned)]
                yield dict(zip(header_list_cleaned, row_values))
            else: # No header parsed (e.g., header_option was None or detection failed)
                  # Create dict with integer keys, or try to use RAW_TAIFEX_SCHEMA_COLUMNS if possible
                  # For now, just use integer keys if no header. The row processor will have to deal with it.
                yield {i: val for i, val in enumerate(row_values)}

    except Exception as e:
        logger.error(f"[{descriptor}] 在 async_generate_rows_from_generic_csv (數據行號 {current_line_num_for_log} 附近) 中發生未知錯誤: {e}")


# --- Row Processors (Simplified for raw_taifex_data) ---
def _clean_and_map_raw_data(row: Dict[str, Any], descriptor: str) -> Optional[Dict[str, Any]]:
    # This function will try to map known column names (from various TAIFEX CSV formats)
    # to a standardized set of columns for the 'raw_taifex_data' table.
    # It also handles type conversion and adds metadata.

    output_row = {}
    # Normalize keys: handle cases where keys might be integers (if no header) or strings

    # Create a mapping from potential input column names/aliases to standardized names
    # This needs to be comprehensive based on observed CSV formats.
    # Example: '交易日期' -> 'trading_date', '契約' -> 'product_id', etc.
    # Also handle English names if present.
    column_map = {
        # Common date columns
        '交易日期': 'trading_date', '日期': 'trading_date', 'Date': 'trading_date',
        # Product ID
        '契約': 'product_id', '商品代號': 'product_id', 'Symbol': 'product_id', '商品名稱': 'product_id',
        # Expiry
        '到期月份(週別)': 'expiry_month', '到期月份／週別': 'expiry_month', '到期月份': 'expiry_month', 'Expiry': 'expiry_month',
        # Strike
        '履約價': 'strike_price', 'Strike': 'strike_price',
        # Option Type
        '買賣權': 'option_type', 'CallPut': 'option_type', 'Type': 'option_type',
        # OHLC
        '開盤價': 'open', 'Open': 'open',
        '最高價': 'high', 'High': 'high',
        '最低價': 'low', 'Low': 'low',
        '收盤價': 'close', 'Close': 'close', '最後成交價': 'close',
        '結算價': 'settlement_price', 'SettlementPrice': 'settlement_price',
        # Volume / OI
        '成交量': 'volume', '成交數量': 'volume', 'Volume': 'volume', '總成交量': 'volume',
        '未沖銷契約量': 'open_interest', '未沖銷契約數': 'open_interest', '未平倉數': 'open_interest', 'OI': 'open_interest',
        # Session
        '交易時段': 'trading_session', 'Session': 'trading_session',
        # Change
        '漲跌價': 'change', '漲跌': 'change', 'Change': 'change',
        '漲跌幅': 'change_percent', '漲跌%': 'change_percent', 'ChangePercent': 'change_percent',
        # Less common, but good to have
        '最後最佳買價': 'last_best_bid_price',
        '最後最佳賣價': 'last_best_ask_price',
        '歷史最高價': 'historical_high',
        '歷史最低價': 'historical_low',
        '是否暫停交易': 'is_suspended',
        '個股期貨近月占整體個股期貨未沖銷契約量比率': 'near_month_oi_ratio_single_stock_futures', # Example of a very specific one
         # From options_daily_v1/v2 explicitly:
        '到期月份_週別': 'expiry_month',
        # From futures_daily explicitly:
        '漲跌percent': 'change_percent', # already covered
        'spread_volume': 'spread_volume'
    }
    # Add integer keys to map if headers were not parsed
    for i in range(len(row)):
        if i not in column_map: # Avoid overwriting string keys if present
             # This is tricky. We need a way to map int keys to sensible column names.
             # For now, we'll only process string keys from a parsed header.
             pass


    for raw_key, raw_value in row.items():
        # Try to map integer keys if they exist (e.g. no header in CSV)
        # This is a placeholder for a more robust mapping strategy for headerless CSVs
        # For now, we primarily rely on string keys from parsed headers.
        standard_key = None
        if isinstance(raw_key, str):
            cleaned_key = raw_key.strip().replace(' ', '_').replace('(', '').replace(')', '')
            standard_key = column_map.get(cleaned_key, column_map.get(raw_key, None)) # Check original and cleaned
            if not standard_key and cleaned_key in RAW_TAIFEX_SCHEMA_COLUMNS: # If cleaned key is already standard
                 standard_key = cleaned_key
        # else: # raw_key is int, try to map based on position (harder)
            # This would require knowing the expected column order for headerless files.
            # For now, we skip int keys if they don't map to something useful.

        if standard_key and standard_key in RAW_TAIFEX_SCHEMA_COLUMNS:
            output_row[standard_key] = str(raw_value).strip() if raw_value is not None else None

    # Data Type Conversions and Validations
    try:
        date_str = output_row.get('trading_date')
        if date_str:
            if re.match(r"^\d{8}$", date_str):
                output_row['trading_date'] = datetime.strptime(date_str, '%Y%m%d').date()
            elif re.match(r"^\d{4}/\d{2}/\d{2}$", date_str):
                output_row['trading_date'] = datetime.strptime(date_str, '%Y/%m/%d').date()
            elif re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
                 output_row['trading_date'] = datetime.strptime(date_str, '%Y-%m-%d').date()
            else:
                logger.warning(f"[{descriptor}] 未知日期格式: {date_str}")
                output_row['trading_date'] = None
        else: # If no trading_date, this row is likely invalid for raw_taifex_data
            logger.debug(f"[{descriptor}] Row skipped: missing trading_date. Original row: {row}")
            return None


        # Convert numeric fields
        for col in ['open', 'high', 'low', 'close', 'settlement_price', 'strike_price', 'change', 'change_percent']:
            if output_row.get(col) is not None:
                try: output_row[col] = float(str(output_row[col]).replace(',', ''))
                except (ValueError, TypeError): output_row[col] = None
        for col in ['volume', 'open_interest']:
            if output_row.get(col) is not None:
                try: output_row[col] = int(str(output_row[col]).replace(',', ''))
                except (ValueError, TypeError): output_row[col] = None

        # Ensure essential keys for raw_taifex_data are present, or it's not a useful row
        if not output_row.get('trading_date') or not output_row.get('product_id'):
            logger.debug(f"[{descriptor}] Row skipped: missing essential trading_date or product_id. Processed row: {output_row}")
            return None

        # Add metadata
        output_row['source_file'] = descriptor
        output_row['processed_at'] = datetime.now(pytz.timezone('UTC'))

        # Filter to only include columns defined in RAW_TAIFEX_SCHEMA_COLUMNS for the final table
        final_output_row = {k: v for k, v in output_row.items() if k in RAW_TAIFEX_SCHEMA_COLUMNS + ['source_file', 'processed_at']}

        return final_output_row

    except Exception as e:
        logger.error(f"[{descriptor}] 清理/轉換原始數據行時出錯: {e}. Original row: {row}, Processed output_row: {output_row}")
        return None


PIPELINE_ROW_PROCESSORS: Dict[str, Any] = {
    "raw_taifex_data": _clean_and_map_raw_data,
    # Add other specific processors if needed, but for now, all go to raw_taifex_data
}

# --- parse_content_to_arrow (Modified for generic CSV and raw_taifex_data pipeline) ---
async def parse_content_to_arrow(
    stream_reader: Union[AsyncBytesGeneratorReader, Any],
    consumed_sample_bytes: bytes,
    recipe: Dict[str, Any],
    descriptor: str,
    batch_size: int = 10000 # Unused in current direct-to-pylist approach
) -> Optional[pyarrow.Table]:
    parser_type = recipe.get("parser")
    args = recipe.get("args", {}).copy()
    pipeline_name = recipe.get("pipeline", "raw_taifex_data") # Default to raw_taifex_data

    if parser_type in ["unknown", "unknown_encoding"]:
        logger.warning(f"跳過 {descriptor} (配方: {parser_type})")
        return None

    row_iterator: Optional[AsyncGenerator[Dict[str, Any], None]] = None
    if parser_type == "csv_generic":
        row_iterator = async_generate_rows_from_generic_csv(stream_reader, consumed_sample_bytes, args, descriptor)
    # Add other parser types (like FWF) if they are reintroduced by determine_parsing_recipe
    # elif parser_type == "fwf":
    #     row_iterator = async_generate_rows_from_fwf(stream_reader, consumed_sample_bytes, args, descriptor)
    else:
        logger.error(f"[{descriptor}] 未知的解析器類型 '{parser_type}'。")
        return None

    if not row_iterator:
        logger.error(f"[{descriptor}] 未能為解析器類型 '{parser_type}' 創建行迭代器。")
        return None

    processed_rows_for_arrow = []
    row_processor = PIPELINE_ROW_PROCESSORS.get(pipeline_name) # Should get _clean_and_map_raw_data

    if not row_processor:
        logger.error(f"[{descriptor}] 未找到針對管線 '{pipeline_name}' 的行處理器。")
        # Fallback: try to append raw rows if no processor, though this is not ideal
        async for raw_row_dict in row_iterator:
            if raw_row_dict: processed_rows_for_arrow.append(raw_row_dict)
    else:
        async for raw_row_dict in row_iterator:
            processed_row = row_processor(raw_row_dict, descriptor) # Call _clean_and_map_raw_data
            if processed_row:
                processed_rows_for_arrow.append(processed_row)
            # else:
                # logger.debug(f"[{descriptor}] Row processor returned None for row: {raw_row_dict}")


    if not processed_rows_for_arrow:
        logger.warning(f"[{descriptor}] {pipeline_name} 串流處理後沒有有效數據行可供轉換為 Arrow。")
        return None

    try:
        # Define schema based on RAW_TAIFEX_SCHEMA_COLUMNS + metadata
        # This ensures consistent schema for pyarrow.Table.from_pylist
        # PyArrow infers schema from the first batch of rows, which can be problematic if types vary.
        # For now, we rely on from_pylist's inference and ensure data types in row_processor.
        # TODO: Explicitly define PyArrow schema for robustness.

        arrow_table = pyarrow.Table.from_pylist(processed_rows_for_arrow)
        logger.success(f"[{descriptor}] ({pipeline_name}串流) 成功將處理後的行直接轉換為 Arrow Table, 行數: {len(arrow_table)}")
        return arrow_table
    except Exception as e:
        logger.error(f"[{descriptor}] ({pipeline_name}串流) 從 pylist 到 Arrow Table 轉換失敗: {e}")
        logger.info(f"前幾行數據預覽 (轉換失敗前): {processed_rows_for_arrow[:2]}")

        # Fallback to Pandas DataFrame then to Arrow, might handle mixed types better initially
        try:
            logger.info(f"[{descriptor}] 嘗試使用 Pandas DataFrame 作為中介進行 Arrow 轉換回退...")
            fallback_df = pd.DataFrame(processed_rows_for_arrow)
            # Ensure columns match the target table, handling missing/extra ones
            # For raw_taifex_data, the schema is somewhat flexible due to various input files
            # We will rely on the INSERT INTO ... SELECT columns FROM view logic to align them.
            arrow_table = pyarrow.Table.from_pandas(fallback_df, preserve_index=False)
            logger.info(f"[{descriptor}] ({pipeline_name}串流-回退) Pandas 中介轉換成功。")
            return arrow_table
        except Exception as e_fallback:
            logger.error(f"[{descriptor}] ({pipeline_name}串流-回退) Pandas 中介轉換也失敗: {e_fallback}")
            return None


# --- process_file_content (Modified to always target 'raw_taifex_data' table) ---
async def process_file_content(
    descriptor: str,
    stream_reader: Union[AsyncBytesGeneratorReader, Any],
    format_map: Dict[str, Any], # format_map is less critical now with generic parsing
    db_conn: duckdb.DuckDBPyConnection,
    hw_mgr: HardwareManager
):
    logger.info(f"開始處理串流: {descriptor}")
    # determine_parsing_recipe will now give a generic CSV recipe targeting 'raw_taifex_data'
    recipe, consumed_sample_bytes = await determine_parsing_recipe(stream_reader, descriptor)
    map_updated_locally = False # format_map usage is minimized

    if not recipe or recipe.get("parser") in ["unknown", "unknown_encoding"]:
        logger.warning(f"因未知/無效配方跳過 {descriptor[:50]}")
        # Even if recipe is bad, we might have consumed sample bytes. Ensure stream_reader is managed.
        if hasattr(stream_reader, 'release') and callable(stream_reader.release):
            await stream_reader.release()
        return {"status": "skipped_invalid_recipe", "descriptor": descriptor, "map_updated": map_updated_locally}

    arrow_table = await parse_content_to_arrow(
        stream_reader,
        consumed_sample_bytes,
        recipe,
        descriptor
    )
    # Ensure stream_reader is fully consumed/released after parse_content_to_arrow
    if hasattr(stream_reader, 'release') and callable(stream_reader.release):
        await stream_reader.release()


    if arrow_table is None or arrow_table.num_rows == 0:
        logger.warning(f"[{descriptor}] 未能從內容生成 Arrow Table 或 Table 為空。")
        return {"status": "skipped_no_arrow_data", "descriptor": descriptor, "map_updated": map_updated_locally}

    # All data now goes to 'raw_taifex_data' table
    table_name = "raw_taifex_data"
    logger.info(f"[{descriptor}] 目標資料庫表: {table_name}")


    try:
        temp_view_name = f"arrow_view_{hashlib.sha256(descriptor.encode()).hexdigest()[:10]}"
        db_conn.register(temp_view_name, arrow_table)

        # Define target columns based on 'raw_taifex_data' schema
        # These are the columns in the CREATE TABLE statement for raw_taifex_data
        # Ensure 'id' is handled by sequence, 'source_file' and 'processed_at' are in arrow_table from row_processor
        db_target_cols_info = db_conn.execute(f"DESCRIBE {table_name};").fetchall()
        db_target_cols = {row[0] for row in db_target_cols_info if row[0] != 'id'} # Exclude 'id' as it's auto-generated

        arrow_cols_in_table = set(arrow_table.schema.names)

        # Columns to insert are those present in both Arrow table and DB table definition
        insert_cols_list = list(db_target_cols.intersection(arrow_cols_in_table))

        if not insert_cols_list:
            logger.warning(f"[{descriptor}] Arrow Table 與目標表 {table_name} 沒有共同的可插入欄位。 Arrow cols: {arrow_cols_in_table}, DB target cols: {db_target_cols}")
            db_conn.unregister(temp_view_name)
            return {"status": "skipped_no_common_columns", "descriptor": descriptor, "map_updated": map_updated_locally}

        insert_cols_str = ", ".join(f'"{c}"' for c in insert_cols_list)
        # SELECT statement must pick columns from the Arrow view that match insert_cols_list
        select_cols_str = ", ".join(f'"{c}"' for c in insert_cols_list) # Assuming view columns match

        sql_insert = f"INSERT INTO {table_name} (id, {insert_cols_str}) SELECT nextval('seq_{table_name}'), {select_cols_str} FROM {temp_view_name}"

        idx_sql = UNIQUE_INDICES.get(table_name)
        uq_cols_str = ""
        if idx_sql:
            match = re.search(r'\((.*?)\)', idx_sql) # Extracts columns from "ON table_name(col1, col2)"
            if match:
                uq_cols_str = match.group(1)
                 # Ensure all unique constraint columns are actually in insert_cols_list
                unique_constraint_cols = {c.strip().replace('"', '') for c in uq_cols_str.split(',')}
                if unique_constraint_cols.issubset(set(insert_cols_list)):
                    sql_insert += f" ON CONFLICT ({uq_cols_str}) DO NOTHING"
                else:
                    logger.warning(f"[{descriptor}] 表 {table_name} 的唯一索引欄位 ({uq_cols_str}) 並非全部存在於待插入欄位 ({insert_cols_list})，將不使用 ON CONFLICT。")
            else:
                logger.warning(f"[{descriptor}] 無法從 UNIQUE_INDICES 解析表 {table_name} 的唯一索引欄位。")


        db_conn.execute(sql_insert)
        logger.success(f"[{descriptor}] 成功將 Arrow Table ({arrow_table.num_rows} 行) 載入到 DuckDB 表 '{table_name}'。")
        db_conn.unregister(temp_view_name)
        if hw_mgr: hw_mgr.log_event_snapshot(f"Processed_stream: {descriptor[:30]}")

        return {"status": "success", "rows_in_arrow": arrow_table.num_rows, "table": table_name, "descriptor": descriptor, "map_updated": map_updated_locally}

    except Exception as e:
        logger.error(f"[{descriptor}] 處理/載入 Arrow Table 到 DuckDB 表 '{table_name}' 時失敗: {e}")
        logger.error(f"Arrow Table Schema: {arrow_table.schema}")
        try: db_conn.unregister(temp_view_name)
        except: pass
        return {"status": "error_loading_to_db", "descriptor": descriptor, "error_msg": str(e), "map_updated": map_updated_locally}

# --- async_main (Modified to use file paths from CLI args) ---
async def async_main(
    input_file_paths: List[str], # Changed from input_streams
    db_output_dir_arg: str,
    db_name_arg: str,
    temp_dir_arg: Optional[str],
    max_workers_arg: Optional[int],
    memory_limit_gb_arg: Optional[int],
    log_level_arg: str
):
    global logger # Make sure global logger is updated
    logger = SimpleLogger(log_level=log_level_arg)
    logger.header(f"TAIFEX Pipeline (Async Full Stream v20.4 CLI Enabled) Starting with {len(input_file_paths)} file(s)")

    db_out_dir = os.path.abspath(db_output_dir_arg)
    db_fpath = os.path.join(db_out_dir, db_name_arg)
    # format_map is now less important but can be kept for future advanced recipe learning
    fmt_map_fpath = os.path.join(db_out_dir, FORMAT_MAP_FILENAME)
    tmp_dir_root = os.path.abspath(temp_dir_arg) if temp_dir_arg else os.path.join(db_out_dir, "temp_pipeline_async")
    duckdb_tmp_p = os.path.join(tmp_dir_root, "duckdb_temp")
    for p_create in [db_out_dir, tmp_dir_root, duckdb_tmp_p]: os.makedirs(p_create, exist_ok=True)

    hw_mgr = HardwareManager(max_workers_arg, memory_limit_gb_arg)
    hw_mgr.display_initial_dashboard()
    t_start = time.time()

    format_map: Dict[str, Any] = {} # Still used by process_file_content, though recipe determination is generic
    if os.path.exists(fmt_map_fpath):
        try:
            with open(fmt_map_fpath, 'r', encoding='utf-8') as f: format_map = json.load(f)
            logger.info(f"已載入 format map: {len(format_map)} 個條目。")
        except Exception as e: logger.warning(f"載入 format map 失敗: {e}")
    else: logger.info("未找到現有 format map，將創建新的。")


    db_conn: Optional[duckdb.DuckDBPyConnection] = None
    try:
        db_conn = duckdb.connect(database=db_fpath, read_only=False)
        db_conn.execute(f"SET memory_limit='{hw_mgr.memory_limit_gb}GB';")
        db_conn.execute(f"SET threads={hw_mgr.max_workers};")
        db_conn.execute(f"SET temp_directory='{duckdb_tmp_p}';")
        for sql_seq in SEQUENCES.values(): db_conn.execute(sql_seq)
        for sql_tbl in TABLE_DEFINITIONS.values(): db_conn.execute(sql_tbl)
        for idx_name, idx_sql_str in UNIQUE_INDICES.items():
            try: db_conn.execute(idx_sql_str)
            except Exception as e_idx: logger.warning(f"創建唯一索引 {idx_name} 失敗: {e_idx}")
        logger.info("DuckDB 資料庫結構已初始化。")
    except Exception as e:
        logger.error(f"DuckDB 初始化失敗: {e}")
        if db_conn: db_conn.close()
        return {"status": "error", "message": f"DuckDB 初始化失敗: {e}"}

    total_files_processed = 0
    total_rows_in_arrow = 0
    overall_map_updated = False # format_map less critical
    errors_encountered = 0
    tasks = []

    # Helper async generator to read file chunks
    async def file_chunk_reader(file_path: str, chunk_size: int = 8192) -> AsyncGenerator[bytes, None]:
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
                    await asyncio.sleep(0) # Yield control
        except Exception as e_fcr:
            logger.error(f"FileChunkReader: Error reading file {file_path}: {e_fcr}")
            # yield b'' # Ensure generator completes if error occurs early

    for file_path_str in input_file_paths:
        file_path = pathlib.Path(file_path_str)
        if not file_path.exists() or not file_path.is_file():
            logger.error(f"輸入檔案不存在或不是一個檔案: {file_path_str}")
            errors_encountered +=1
            continue

        descriptor = file_path.name # Use filename as descriptor

        # Create an async stream reader for the local file
        local_file_stream_reader = file_chunk_reader(str(file_path))

        if descriptor.lower().endswith(".zip"):
            logger.info(f"檢測到 ZIP 檔案: {descriptor}，將使用 InMemoryStreamUnzipper 處理。")
            # Pass the async generator directly to InMemoryStreamUnzipper
            unzipper = InMemoryStreamUnzipper(local_file_stream_reader)
            try:
                member_byte_agen = await unzipper.get_uncompressed_stream() # await here
                if member_byte_agen:
                    member_descriptor = f"{descriptor} -> {unzipper._first_file_name or 'member'}"
                    logger.info(f"準備處理來自 ZIP 的成員串流: {member_descriptor}")
                    adapted_member_stream = AsyncBytesGeneratorReader(member_byte_agen, member_descriptor)
                    task = asyncio.create_task(
                        process_file_content(member_descriptor, adapted_member_stream, format_map, db_conn, hw_mgr)
                    )
                    tasks.append(task)
                else:
                    logger.warning(f"ZIP 檔案 {descriptor} 解壓縮後未獲得有效成員串流。")
                    errors_encountered +=1
            except Exception as e_unzip:
                logger.error(f"處理 ZIP 檔案 {descriptor} 時解壓縮或任務創建失敗: {e_unzip}")
                errors_encountered += 1
            # finally: # unzipper.close() is tricky with async generator lifecycle.
                      # The generator from get_uncompressed_stream needs the buffer.
                      # InMemoryStreamUnzipper.close() should ideally be called after its generator is done.
                      # This is complex. For now, assume process_file_content handles stream closing via AsyncBytesGeneratorReader.release()
                # unzipper.close() # This might close the buffer too early.
        else:
            logger.info(f"準備處理直接檔案串流: {descriptor}")
            adapted_direct_stream = AsyncBytesGeneratorReader(local_file_stream_reader, descriptor)
            task = asyncio.create_task(
                process_file_content(descriptor, adapted_direct_stream, format_map, db_conn, hw_mgr)
            )
            tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, Exception):
            logger.error(f"任務執行時發生未捕獲異常: {res}")
            errors_encountered += 1
        elif res:
            total_files_processed +=1
            if res.get("map_updated"): overall_map_updated = True # Still track if format_map logic is ever used
            if res.get("status") == "success":
                total_rows_in_arrow += res.get("rows_in_arrow", 0)
            elif res.get("status", "").startswith("error_"):
                errors_encountered += 1
                logger.error(f"處理檔案/串流 {res.get('descriptor')} 時發生錯誤: {res.get('error_msg', '未知錯誤')}")
            elif res.get("status", "").startswith("skipped_"):
                 logger.info(f"串流 {res.get('descriptor')} 被跳過: {res.get('status')}")

    logger.header("資料處理階段摘要")
    logger.success(f"總共處理的檔案/串流 (包括ZIP成員) 數量: {total_files_processed}")
    logger.success(f"從 Arrow 表嘗試載入的總行數: {total_rows_in_arrow:,}")
    logger.info(f"錯誤發生次數: {errors_encountered}")

    if overall_map_updated : # If format_map logic were to be used and updated
        try:
            with open(fmt_map_fpath, 'w', encoding='utf-8') as f: json.dump(format_map, f, indent=4, ensure_ascii=False)
            logger.info(f"Format map 已儲存至 {fmt_map_fpath}")
        except Exception as e: logger.error(f"儲存 format map 失敗: {e}")

    if db_conn: db_conn.close()
    logger.header(f"Pipeline finished. Total time: {time.time() - t_start:.2f}s. DB: {db_fpath}")

    if errors_encountered > 0:
        logger.error("管線執行期間發生錯誤。")
        return {"status": "error", "message": "管線執行期間發生一個或多個錯誤。", "processed": total_files_processed, "loaded_rows": total_rows_in_arrow, "errors": errors_encountered }

    return {"status": "success", "processed": total_files_processed, "loaded_rows": total_rows_in_arrow, "db_path": db_fpath}

# --- main (CLI argument parsing and calling async_main) ---
def main():
    parser = argparse.ArgumentParser(description="TAIFEX Pipeline (Async Full Stream v20.4 CLI Enabled)")
    parser.add_argument("--input-files", nargs='+', required=True, help="一個或多個輸入檔案的路徑 (ZIP 或 CSV)。")
    parser.add_argument("--db-output-dir", required=True, help="DuckDB 資料庫檔案的輸出目錄。")
    parser.add_argument("--db-name", default="taifex_cli_pipeline.duckdb", help="DuckDB 資料庫檔案的名稱。")
    parser.add_argument("--temp-dir", default=None, help="臨時檔案目錄 (可選)。")
    parser.add_argument("--max-workers", type=int, default=None, help="最大並行處理核心數 (可選)。")
    parser.add_argument("--memory-limit-gb", type=int, default=None, help="DuckDB 記憶體預算 (GB, 可選)。")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="日誌級別。")

    args = parser.parse_args()

    # Initialize logger here so it's available before async_main
    global logger
    logger = SimpleLogger(log_level=args.log_level)

    logger.info(f"TAIFEX Pipeline v20.4 CLI 啟動。 輸入檔案: {args.input_files}")

    try:
        asyncio.run(async_main(
            input_file_paths=args.input_files,
            db_output_dir_arg=args.db_output_dir,
            db_name_arg=args.db_name,
            temp_dir_arg=args.temp_dir,
            max_workers_arg=args.max_workers,
            memory_limit_gb_arg=args.memory_limit_gb,
            log_level_arg=args.log_level
        ))
    except Exception as e:
        logger.error(f"執行管線時發生頂層錯誤: {e}")
        sys.exit(1)
    logger.info("TAIFEX Pipeline v20.4 CLI 執行完畢。")

if __name__ == "__main__":
    main()
