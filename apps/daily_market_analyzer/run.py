# -*- coding: utf-8 -*-
"""
每日市場分析儀 主執行入口。

接收命令列參數，協調 YFinanceClient 進行數據擷取與考古，
使用 DBManager 將數據存入資料庫，透過 AnalysisEngine 分析數據，
最後使用 ReportGenerator 生成每日市場洞察報告。
"""
import argparse
import sys
import os
from datetime import datetime, timedelta # timedelta 也被 dynamic_worker 中的錯誤日誌使用
import pandas as pd
# import concurrent.futures # 被 multiprocessing 取代
from tqdm import tqdm
import io
import contextlib
import multiprocessing
import queue # For queue.Empty
import psutil
from multiprocessing import shared_memory # 方案 B
import numpy as np # 方案 B
import uuid # 方案 B

# 設定專案路徑，確保可以正確匯入其他模組
def setup_project_path():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

setup_project_path()

try:
    from apps.daily_market_analyzer.yfinance_client import YFinanceClient
    from apps.daily_market_analyzer.db_manager import DBManager
    from apps.daily_market_analyzer.analysis_engine import AnalysisEngine
    from apps.daily_market_analyzer.report_generator import ReportGenerator
except ModuleNotFoundError as e:
    print(f"錯誤：導入模組時發生錯誤 (ModuleNotFoundError): {e}")
    raise

# --- process_single_ticker 函數定義 ---
def process_single_ticker(ticker, start_date, end_date, db_path, cache_db_path, table_name, force_refresh, verbose_mode):
    """
    處理單一金融標的的完整數據回填與寫入邏輯。
    此函數將在獨立的進程中執行。
    """
    pid = os.getpid()

    if not verbose_mode:
        log_capture_string = io.StringIO()
        with contextlib.redirect_stdout(log_capture_string), contextlib.redirect_stderr(log_capture_string):
            try:
                print(f"--- [PID:{pid}] (靜默) 開始處理標的: {ticker} ---")
                db_manager_process = DBManager(db_path=db_path)
                yf_client_process = YFinanceClient(db_manager=db_manager_process, cache_db_path=cache_db_path)
                hydrated_df, execution_log = yf_client_process.hydrate_data_range(
                    ticker, start_date, end_date,
                    db_table_name=table_name,
                    force_refresh=force_refresh
                )
                print(f"--- [PID:{pid}] (靜默) 標的: {ticker} 處理完畢 ---")
            except Exception as e:
                print(f"--- [PID:{pid}] (靜默) 標的: {ticker} 發生嚴重錯誤: {e} ---")
                hydrated_df = None
                error_log = {}
                temp_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                current_date_obj = temp_date_obj
                while current_date_obj <= end_date_obj:
                    date_str = current_date_obj.strftime("%Y-%m-%d")
                    error_log.setdefault(date_str, {}).setdefault(ticker, {
                        "status": "hydration_error_in_silent_mode",
                        "message": str(e),
                        "count": 0,
                        "interval": None
                    })
                    current_date_obj += timedelta(days=1)
                execution_log = error_log
        worker_logs = log_capture_string.getvalue()
        return hydrated_df, execution_log, worker_logs
    else:
        print(f"--- [PID:{pid}] (詳細) 開始處理標的: {ticker} ---")
        try:
            db_manager_process = DBManager(db_path=db_path)
            yf_client_process = YFinanceClient(db_manager=db_manager_process, cache_db_path=cache_db_path)
            hydrated_df, execution_log = yf_client_process.hydrate_data_range(
                ticker, start_date, end_date,
                db_table_name=table_name,
                force_refresh=force_refresh
            )
            print(f"--- [PID:{pid}] (詳細) 標的: {ticker} 處理完畢 ---")
        except Exception as e:
            print(f"--- [PID:{pid}] (詳細) 標的: {ticker} 發生嚴重錯誤: {e} ---")
            hydrated_df = None
            error_log = {}
            temp_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            current_date_obj = temp_date_obj
            while current_date_obj <= end_date_obj:
                date_str = current_date_obj.strftime("%Y-%m-%d")
                error_log.setdefault(date_str, {}).setdefault(ticker, {
                    "status": "hydration_error_in_verbose_mode",
                    "message": str(e),
                    "count": 0,
                    "interval": None
                })
                current_date_obj += timedelta(days=1)
            execution_log = error_log
        return hydrated_df, execution_log, ""

# --- dynamic_worker 函數定義 ---
def dynamic_worker(task_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue,
                   start_date: str, end_date: str, db_path: str, cache_db_path: str,
                   table_name: str, force_refresh: bool, verbose_mode: bool, worker_id: int):
    """
    動態 worker 函數，從任務隊列中獲取標的進行處理。
    如果處理成功且有數據，則將數據寫入共享記憶體，並將元數據放入結果隊列。
    """
    if verbose_mode:
        print(f"--- [WorkerID:{worker_id}] 動態 Worker 啟動 (共享記憶體模式) ---")

    numeric_cols_for_shm = ['datetime', 'open', 'high', 'low', 'close', 'volume']

    while True:
        shm_name_local = None
        shm_instance_local = None
        processed_ticker = "未設定"

        try:
            ticker = task_queue.get_nowait()
            processed_ticker = ticker
            if verbose_mode:
                print(f"--- [WorkerID:{worker_id}] (詳細) Worker 取得任務: {ticker} ---")

            hydrated_df, execution_log, worker_logs_str = process_single_ticker(
                ticker, start_date, end_date, db_path, cache_db_path,
                table_name, force_refresh, verbose_mode
            )

            shm_meta = None
            if hydrated_df is not None and not hydrated_df.empty:
                if verbose_mode:
                    print(f"--- [WorkerID:{worker_id}, Ticker:{ticker}] (詳細) 數據獲取成功 ({len(hydrated_df)} 行)，準備寫入共享記憶體... ---")

                original_df_columns = list(hydrated_df.columns)
                if verbose_mode and 'datetime' in hydrated_df.columns:
                    print(f"DEBUG [WorkerID:{worker_id}, Ticker:{ticker}] Original datetime HEAD:\n{hydrated_df['datetime'].head()}")

                if pd.api.types.is_datetime64_any_dtype(hydrated_df['datetime']):
                    # 1. 確保 datetime 是 naive UTC
                    if hydrated_df['datetime'].dt.tz is not None:
                        hydrated_df['datetime'] = hydrated_df['datetime'].dt.tz_convert('UTC').dt.tz_localize(None)
                    # else: # 如果是 naive，則假定它已经是 UTC-like or an appropriate representation
                        # print(f"DEBUG [WorkerID:{worker_id}, Ticker:{ticker}] datetime is naive, assuming UTC-like for int64 conversion.")

                    # 2. 無論原始精度，先統一轉換為 datetime64[ns]
                    hydrated_df['datetime'] = hydrated_df['datetime'].astype('datetime64[ns]')
                    if verbose_mode:
                         print(f"DEBUG [WorkerID:{worker_id}, Ticker:{ticker}] datetime astype('datetime64[ns]') before int64 conversion HEAD:\n{hydrated_df['datetime'].head()}")

                    # 3. 轉換為 int64 (nanoseconds since epoch)
                    hydrated_df['datetime'] = hydrated_df['datetime'].astype(np.int64)
                    if verbose_mode:
                        print(f"DEBUG [WorkerID:{worker_id}, Ticker:{ticker}] int64 datetime HEAD for SHM:\n{hydrated_df['datetime'].head()}")

                data_for_shm_numpy = hydrated_df[numeric_cols_for_shm].to_numpy()

                shm_name_local = f"shm_DMA_{worker_id}_{uuid.uuid4().hex}"
                shm_instance_local = shared_memory.SharedMemory(name=shm_name_local, create=True, size=data_for_shm_numpy.nbytes)

                shared_array_view_worker = np.ndarray(data_for_shm_numpy.shape, dtype=data_for_shm_numpy.dtype, buffer=shm_instance_local.buf)
                shared_array_view_worker[:] = data_for_shm_numpy[:]

                shm_meta = {
                    "name": shm_name_local,
                    "shape": data_for_shm_numpy.shape,
                    "dtype": data_for_shm_numpy.dtype.name,
                    "columns_numeric": numeric_cols_for_shm,
                    "original_columns": original_df_columns,
                    "interval_value": hydrated_df['interval'].iloc[0] if 'interval' in hydrated_df.columns and not hydrated_df.empty else "unknown_worker_interval"
                }
                if verbose_mode:
                    print(f"--- [WorkerID:{worker_id}, Ticker:{ticker}] (詳細) 數據已寫入共享記憶體: {shm_name_local}, Interval: {shm_meta['interval_value']} ---")
            else:
                if verbose_mode:
                    print(f"--- [WorkerID:{worker_id}, Ticker:{ticker}] (詳細) 無數據返回，不使用共享記憶體。 ---")

            result_queue.put({
                "ticker": ticker,
                "shm_meta": shm_meta,
                "execution_log": execution_log,
                "worker_logs_str": worker_logs_str
            })

            if verbose_mode:
                print(f"--- [WorkerID:{worker_id}, Ticker:{ticker}] (詳細) Worker 完成任務，已將元數據/結果放入隊列 ---")

        except queue.Empty:
            if verbose_mode:
                print(f"--- [WorkerID:{worker_id}] (詳細) 任務隊列已空，Worker 準備退出 ---")
            break
        except Exception as e:
            print(f"--- [WorkerID:{worker_id}, Ticker:{processed_ticker}] (詳細) Worker 處理任務時發生嚴重錯誤: {type(e).__name__} - {e} ---")
            error_log_for_worker_failure = {}
            temp_date_obj_worker = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj_worker = datetime.strptime(end_date, "%Y-%m-%d")
            current_date_obj_worker = temp_date_obj_worker
            while current_date_obj_worker <= end_date_obj_worker:
                date_str = current_date_obj_worker.strftime("%Y-%m-%d")
                error_log_for_worker_failure.setdefault(date_str, {}).setdefault(processed_ticker, {
                    "status": "dynamic_worker_shm_exception",
                    "message": f"WorkerID {worker_id} (Ticker: {processed_ticker}) 發生嚴重錯誤: {type(e).__name__} - {str(e)}",
                    "count": 0, "interval": None
                })
                current_date_obj_worker += timedelta(days=1)

            result_queue.put({
                "ticker": processed_ticker,
                "shm_meta": None,
                "execution_log": error_log_for_worker_failure,
                "worker_logs_str": f"WorkerID {worker_id} (Ticker: {processed_ticker}) Error: {type(e).__name__} - {e}"
            })
            break
        finally:
            if shm_instance_local:
                shm_instance_local.close()
                if verbose_mode:
                    print(f"--- [WorkerID:{worker_id}, Ticker:{processed_ticker}] (詳細) Worker 已關閉其共享記憶體連接: {shm_name_local if shm_name_local else '無名稱'} ---")

    if verbose_mode:
        print(f"--- [WorkerID:{worker_id}] 動態 Worker (共享記憶體模式) 正常結束 ---")

def main():
    """
    主執行函數 for Daily Market Analyzer。
    """
    parser = argparse.ArgumentParser(description="每日市場洞察報告與智能數據考古引擎。")
    parser.add_argument("--tickers", required=True, help="要分析的標的列表，以逗號分隔 (例如: AAPL,MSFT)。")
    parser.add_argument("--start-date", help="數據獲取分析起始日期 (格式: YYYY-MM-DD)。若非 --report-only 模式則為必需。")
    parser.add_argument("--end-date", help="數據獲取分析結束日期 (格式: YYYY-MM-DD)。若非 --report-only 模式則為必需。")
    parser.add_argument("--db-path", default="data_workspace/daily_market_analyzer.duckdb",
                        help="主分析資料庫的完整路徑 (例如: data_workspace/daily_market_analysis.duckdb)。")
    parser.add_argument("--db-name", default="daily_market_analysis.duckdb",
                        help="主分析資料庫的檔案名稱 (非完整路徑)。")
    parser.add_argument("--cache-db-path",
                        help="DuckDB 快取資料庫的最終存檔路徑。")
    parser.add_argument("--table-name", default="market_ohlcv_data",
                        help="資料庫中儲存 OHLCV 數據的表格名稱。")
    parser.add_argument("--force-refresh", action="store_true",
                        help="若指定，則強制重新獲取所有數據，忽略快取。")
    parser.add_argument("--process-uploads", action="store_true",
                        help="若指定，則處理 'uploads' 資料夾 (此功能待實現)。")
    parser.add_argument("--verbose", action="store_true", help="啟用詳細日誌模式，將所有進程的即時日誌打印到主控台。")
    parser.add_argument("--data-only", action="store_true", help="若啟用，僅執行數據獲取與儲存，不生成報告。")
    parser.add_argument("--report-only", action="store_true", help="若啟用，僅從資料庫讀取數據並生成報告，不進行數據獲取。")
    parser.add_argument("--report-start-date", help="與 --report-only 配合使用，定義報告的起始日期 (格式: YYYY-MM-DD)。")
    parser.add_argument("--report-end-date", help="與 --report-only 配合使用，定義報告的結束日期 (格式: YYYY-MM-DD)。")
    parser.add_argument("--duckdb-memory-limit", type=str, default=None,
                        help="直接設定 DuckDB 的記憶體限制 (例如 '1GB', '512MB')。若設定此項，則忽略 --duckdb-memory-limit-ratio。")
    parser.add_argument("--duckdb-memory-limit-ratio", type=float, default=0.7,
                        help="設定 DuckDB 可使用的系統可用記憶體比例 (0.1 至 1.0)。預設值: 0.7。僅在未設定 --duckdb-memory-limit 時生效。")

    args = parser.parse_args()

    duckdb_mem_limit_str = None
    if args.duckdb_memory_limit:
        duckdb_mem_limit_str = args.duckdb_memory_limit
        print(f"INFO: 使用指定的 DuckDB 記憶體限制: {duckdb_mem_limit_str}")
    else:
        if not (0.1 <= args.duckdb_memory_limit_ratio <= 1.0):
            print(f"警告: --duckdb-memory-limit-ratio 提供的比例 {args.duckdb_memory_limit_ratio} 超出有效範圍 (0.1-1.0)。將使用預設值 0.7。")
            args.duckdb_memory_limit_ratio = 0.7
        available_memory_bytes = psutil.virtual_memory().available
        calculated_limit_bytes = int(available_memory_bytes * args.duckdb_memory_limit_ratio)
        calculated_limit_mb = calculated_limit_bytes / (1024 * 1024)
        duckdb_mem_limit_str = f"{int(calculated_limit_mb)}MB"
        print(f"INFO: 系統可用記憶體: {available_memory_bytes / (1024*1024):.2f} MB。動態設定 DuckDB 記憶體限制為可用記憶體的 {args.duckdb_memory_limit_ratio*100:.0f}%: {duckdb_mem_limit_str}")

    if args.data_only and args.report_only:
        print("錯誤：--data-only 和 --report-only 選項不能同時啟用。請選擇一個或都不選（完整流程）。")
        sys.exit(1)
    if args.report_only and (not args.report_start_date or not args.report_end_date):
        print("錯誤：使用 --report-only 時，必須同時提供 --report-start-date 和 --report-end-date。")
        sys.exit(1)
    if not args.report_only and (not args.start_date or not args.end_date):
        print("錯誤：在數據處理或完整流程模式下，必須提供 --start-date 和 --end-date。")
        sys.exit(1)
    if (args.report_start_date or args.report_end_date) and not args.report_only and not (args.data_only is False and args.report_only is False):
        if args.data_only :
             print("警告：--report-start-date 和 --report-end-date 參數在 --data-only 模式下無效。")

    print("--- 每日市場洞察報告引擎 v32.0 ---") # 版本更新
    overall_start_time = datetime.now()
    print(f"任務開始時間: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    mode = "完整流程"
    if args.data_only: mode = "數據處理模式 (--data-only)"
    elif args.report_only: mode = "報告生成模式 (--report-only)"
    print(f"執行模式: {mode}")
    print(f"通用參數: 資料庫='{args.db_path}', 資料表='{args.table_name}'")
    if not args.report_only:
        print(f"數據處理參數: 標的='{args.tickers}', 起始日='{args.start_date}', 結束日='{args.end_date}'")
    if not args.data_only:
        report_s = args.report_start_date if args.report_only else args.start_date
        report_e = args.report_end_date if args.report_only else args.end_date
        report_t = args.tickers
        print(f"報告生成參數: 標的='{report_t}', 報告起始日='{report_s}', 報告結束日='{report_e}'")

    if args.process_uploads:
        print("資訊：--process-uploads 選項已指定，但此功能尚在開發中，將被略過。")

    db_manager = DBManager(db_path=args.db_path, memory_limit=duckdb_mem_limit_str)
    data_processed_successfully = False # 初始化數據處理成功標記
    overall_execution_log = {} # 初始化 overall_execution_log 以確保它在所有分支中都定義

    # 數據獲取與處理階段 (僅在非 --report-only 模式下執行)
    if not args.report_only:
        print("\n--- [階段開始] 數據獲取與處理 ---")

        db_manager.create_ohlcv_table(table_name=args.table_name, target_db_path=args.db_path)
        if args.cache_db_path:
             db_manager.create_ohlcv_table(table_name=args.table_name, target_db_path=args.cache_db_path)
        else:
            print("警告: 未提供 --cache-db-path，快取功能將受限或無法運作。")

        tickers_list_data = [ticker.strip().upper() for ticker in args.tickers.split(',')]
        overall_execution_log_data = {}
        all_hydrated_dfs_data = []

        manager = multiprocessing.Manager()
        task_queue = manager.Queue()
        result_queue = manager.Queue()

        for ticker_task in tickers_list_data:
            task_queue.put(ticker_task)

        num_workers = os.cpu_count() or 4
        print(f"INFO: 啟動動態負載均衡模式，使用 {num_workers} 個 worker 進程處理 {len(tickers_list_data)} 個標的。")

        processes = []
        for i in range(num_workers):
            p = multiprocessing.Process(target=dynamic_worker, args=(
                task_queue, result_queue, args.start_date, args.end_date,
                args.db_path, args.cache_db_path, args.table_name,
                args.force_refresh, args.verbose, i
            ))
            processes.append(p)
            p.start()

        pbar = tqdm(total=len(tickers_list_data), desc="動態負載數據處理中")

        for _ in range(len(tickers_list_data)):
            try:
                result_item = result_queue.get(timeout=1800)
                pbar.update(1)

                res_ticker = result_item["ticker"]
                shm_meta = result_item["shm_meta"]
                ticker_execution_log_single = result_item["execution_log"]
                # worker_logs = result_item["worker_logs_str"]

                hydrated_df_single = None

                if shm_meta:
                    shm_instance_main = None
                    try:
                        if args.verbose:
                            print(f"--- [Main, Ticker:{res_ticker}] (詳細) 接收到共享記憶體元數據: {shm_meta['name']} ---")

                        shm_instance_main = shared_memory.SharedMemory(name=shm_meta["name"])
                        reconstructed_np_array = np.ndarray(
                            shm_meta["shape"], dtype=shm_meta["dtype"], buffer=shm_instance_main.buf
                        ).copy()

                        if args.verbose and reconstructed_np_array.shape[1] > 0 : # 假設 datetime 是第一列
                             print(f"DEBUG [Main, Ticker:{res_ticker}] Reconstructed np_array datetime (int64) column HEAD:\n{reconstructed_np_array[:5, 0]}")

                        temp_df = pd.DataFrame(reconstructed_np_array, columns=shm_meta["columns_numeric"])

                        if 'datetime' in temp_df.columns and shm_meta["columns_numeric"][0] == 'datetime':
                            if args.verbose:
                                print(f"DEBUG [Main, Ticker:{res_ticker}] temp_df datetime (int64) before pd.to_datetime HEAD:\n{temp_df['datetime'].head()}")
                            temp_df['datetime'] = pd.to_datetime(temp_df['datetime'], unit='ns', utc=True)
                            if args.verbose:
                                print(f"DEBUG [Main, Ticker:{res_ticker}] temp_df datetime (UTC) after pd.to_datetime HEAD:\n{temp_df['datetime'].head()}")

                        temp_df['ticker'] = res_ticker
                        temp_df['interval'] = shm_meta.get("interval_value", "unknown_main_interval")

                        if "original_columns" in shm_meta:
                            final_cols_order = list(shm_meta["original_columns"])
                            if 'ticker' not in final_cols_order: final_cols_order.append('ticker')
                            if 'interval' not in final_cols_order: final_cols_order.append('interval')
                            temp_df = temp_df.reindex(columns=final_cols_order)

                        hydrated_df_single = temp_df
                        if args.verbose:
                            print(f"--- [Main, Ticker:{res_ticker}] (詳細) 成功從共享記憶體 {shm_meta['name']} 重建 DataFrame，大小: {hydrated_df_single.shape} ---")
                    except FileNotFoundError:
                        print(f"錯誤: [Main, Ticker:{res_ticker}] 無法找到共享記憶體區塊: {shm_meta['name']}. 可能已被過早释放。")
                    except Exception as e_shm_main:
                        print(f"錯誤: [Main, Ticker:{res_ticker}] 處理共享記憶體 {shm_meta.get('name', '未知SHM')} 時發生錯誤: {e_shm_main}")
                    finally:
                        if shm_instance_main:
                            shm_instance_main.close()
                            shm_instance_main.unlink()
                            if args.verbose:
                                print(f"--- [Main, Ticker:{res_ticker}] (詳細) 主進程已關閉並解除共享記憶體連接: {shm_meta['name']} ---")

                if ticker_execution_log_single:
                    for date_key, log_val_per_day in ticker_execution_log_single.items():
                        overall_execution_log_data.setdefault(date_key, {}).update(log_val_per_day)

                if hydrated_df_single is not None and not hydrated_df_single.empty:
                    all_hydrated_dfs_data.append(hydrated_df_single)
                elif hydrated_df_single is None and shm_meta is not None:
                    print(f"資訊：標的 {res_ticker} 有共享記憶體元數據但未能成功重建 DataFrame。")
                elif hydrated_df_single is None and shm_meta is None:
                    print(f"資訊：標的 {res_ticker} 的動態 worker 未產生共享記憶體數據。")
            except queue.Empty:
                print("警告：從結果隊列獲取結果超時。")
                break
            except Exception as exc:
                print(f"錯誤：主進程在處理 result_queue 中的項目時發生例外: {exc}")
        pbar.close()

        for p in processes:
            p.join(timeout=60)
            if p.is_alive():
                print(f"警告: Worker 進程 {p.pid} 在 join 超時後仍存活，嘗試終止。")
                p.terminate()
                p.join()

        print("\n--- 所有動態 worker 進程處理完成，正在合併數據... ---")
        if all_hydrated_dfs_data:
            for i, df_item in enumerate(all_hydrated_dfs_data):
                if 'datetime' in df_item.columns and pd.api.types.is_datetime64_any_dtype(df_item['datetime']):
                    if df_item['datetime'].dt.tz is None:
                        all_hydrated_dfs_data[i]['datetime'] = df_item['datetime'].dt.tz_localize('UTC')
                    elif str(df_item['datetime'].dt.tz) != 'UTC':
                        all_hydrated_dfs_data[i]['datetime'] = df_item['datetime'].dt.tz_convert('UTC')

            final_master_df = pd.concat(all_hydrated_dfs_data, ignore_index=True)
            if not final_master_df.empty:
                print(f"DEBUG: 合併後的 final_master_df info:")
                final_master_df.info()
                print(f"DEBUG: final_master_df 中的 Tickers: {final_master_df['ticker'].unique()}")
                # print(f"INFO: 數據合併完成，總共 {len(final_master_df)} 筆數據。準備一次性寫入主分析資料庫 '{args.db_path}'...")

                # 修改為按 ticker 分組寫入
                print(f"INFO: 數據合併完成，總共 {len(final_master_df)} 筆數據。準備分批按 ticker 寫入主分析資料庫 '{args.db_path}'...")
                any_upsert_failed = False
                for ticker_name, group_df in final_master_df.groupby('ticker'):
                    print(f"INFO: 正在寫入標的 {ticker_name} 的數據 ({len(group_df)} 筆)...")
                    if ticker_name == "GOOG" and args.verbose: # 為 GOOG 添加額外調試日誌
                        print(f"DEBUG: GOOG group_df info BEFORE upsert:")
                        group_df.info()
                        print(f"DEBUG: GOOG group_df head BEFORE upsert:\n{group_df.head()}")
                        print(f"DEBUG: GOOG group_df tail BEFORE upsert:\n{group_df.tail()}")
                    try:
                        db_manager.upsert_data(group_df, table_name=args.table_name, target_db_path=args.db_path)
                        # upsert_data 內部已有成功日誌，這裡不再重複打印 "標的 ... 成功寫入"
                    except Exception as e_group_upsert:
                        print(f"錯誤：寫入標的 {ticker_name} 的數據到主分析資料庫失敗: {e_group_upsert}")
                        any_upsert_failed = True
                        # 即使一個 ticker 失敗，也嘗試繼續其它 ticker

                if not any_upsert_failed:
                    print(f"INFO: 所有標的數據均已成功嘗試寫入主分析資料庫。")
                    data_processed_successfully = True
                else:
                    print(f"警告：部分標的數據寫入主分析資料庫時發生錯誤。")
                    data_processed_successfully = False
            else:
                print("INFO: 合併後的 DataFrame 為空，無需寫入主分析資料庫。")
                data_processed_successfully = True # 也算處理完成，只是沒數據
        else:
            print("INFO: 本次執行未獲取到任何新的數據可寫入主分析資料庫。")
            data_processed_successfully = True # 也算處理完成

        data_processing_end_time = datetime.now()
        data_task_duration_seconds = (data_processing_end_time - overall_start_time).total_seconds()
        print(f"\n--- [階段結束] 所有數據處理與寫入完成 ---")
        print(f"數據處理階段結束時間: {data_processing_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"數據處理階段執行時長: {data_task_duration_seconds:.2f} 秒")

        if args.data_only:
            print("\n--- --data-only 模式啟用，任務結束 ---")
            sys.exit(0)

        if not data_processed_successfully:
            print("\n錯誤：數據處理階段未能成功完成，將跳過報告生成。")
            sys.exit(1)

        overall_execution_log = overall_execution_log_data # 將收集到的日誌傳遞給報告階段

    # 報告生成階段 (在完整流程或 --report-only 模式下執行)
    if not args.data_only:
        print("\n--- [階段開始] 市場分析報告生成 ---")
        report_start_time_actual = datetime.now()
        report_generation_time_for_filename = datetime.now()

        analysis_engine = AnalysisEngine(db_manager_instance=db_manager)

        current_execution_log = overall_execution_log if not args.report_only else {}
        if args.report_only:
            print("INFO: --report-only 模式，將基於資料庫數據生成報告，無實時執行日誌。")

        report_tickers_list = [ticker.strip().upper() for ticker in args.tickers.split(',')]
        report_start_date_str = args.report_start_date if args.report_only else args.start_date
        report_end_date_str = args.report_end_date if args.report_only else args.end_date
        # 確保 data_task_duration_seconds 在 report_only 模式下為 0
        task_duration_seconds_for_report = 0
        if not args.report_only and 'data_task_duration_seconds' in locals():
             task_duration_seconds_for_report = data_task_duration_seconds

        report_gen = ReportGenerator(execution_log=current_execution_log,
                                     analysis_engine_instance=analysis_engine)
        print(f"INFO: 準備生成報告，標的: {report_tickers_list}, 日期範圍: [{report_start_date_str} to {report_end_date_str}]")

        current_report_time = datetime.now()
        final_report_str = report_gen.generate_full_report(
            overall_start_date_str=report_start_date_str,
            overall_end_date_str=report_end_date_str,
            report_generation_time=current_report_time,
            task_duration_seconds=task_duration_seconds_for_report,
            target_tickers=report_tickers_list,
            db_table_name=args.table_name
        )

        print("\n--- 市場分析報告內容預覽 ---")
        preview_lines = final_report_str.splitlines()[:30]
        for line in preview_lines: print(line)
        if len(final_report_str.splitlines()) > 30: print("... (報告內容過長，已截斷預覽) ...")

        report_output_dir = os.path.join("data_workspace", "reports")
        os.makedirs(report_output_dir, exist_ok=True)
        report_filename_dt_str = report_generation_time_for_filename.strftime('%Y%m%d_%H%M%S')
        report_mode_tag = "FULL" if not args.report_only else "REPORTONLY"
        tickers_for_filename = "_".join(report_tickers_list).replace("^","").replace("=","_")[:30]
        date_range_for_filename = f"{report_start_date_str}_to_{report_end_date_str}"
        report_filename = f"market_analysis_{report_mode_tag}_{tickers_for_filename}_{date_range_for_filename}_{report_filename_dt_str}.md"
        report_filepath = os.path.join(report_output_dir, report_filename)

        try:
            with open(report_filepath, "w", encoding="utf-8") as f: f.write(final_report_str)
            print(f"\n報告已成功儲存至：{report_filepath}")
        except IOError as e:
            print(f"\n錯誤：儲存報告至檔案失敗：{e}")

        report_generation_end_time = datetime.now()
        report_task_duration_seconds = (report_generation_end_time - report_start_time_actual).total_seconds()
        print(f"\n--- [階段結束] 市場分析報告生成完畢 ---")
        print(f"報告生成階段結束時間: {report_generation_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"報告生成階段執行時長: {report_task_duration_seconds:.2f} 秒")

    overall_end_time_final = datetime.now()
    total_script_duration_seconds = (overall_end_time_final - overall_start_time).total_seconds()
    print(f"\n--- 每日市場洞察報告引擎任務總執行完畢 ---")
    print(f"總任務結束時間: {overall_end_time_final.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"總執行時長: {total_script_duration_seconds:.2f} 秒")

    # 在 run.py 結束前，對主數據庫執行 CHECKPOINT
    if not args.report_only and data_processed_successfully: # 只在有數據實際寫入主數據庫時執行
        if db_manager: # 確保 db_manager 實例存在
            db_manager.checkpoint_db() # 調用 DBManager 的新方法，它會使用 self.db_path

    # 同樣對快取資料庫執行 CHECKPOINT (如果使用了快取)
    # 注意：快取資料庫的 checkpoint 可能更適合在每個 worker 完成對它的寫入後，
    # 或者由一個專門的快取管理機制來處理，以避免這裡的單點 CHECKPOINT 成為瓶頸或引入複雜性。
    # 但為了確保測試中快取數據的持久化，這裡也添加一個。
    if not args.report_only and args.cache_db_path:
        if db_manager: # 假設同一個 db_manager 實例可以用來 checkpoint 不同的 db 檔案路徑
            # 理想情況下，應該有一個專門針對 cache_db 的 manager 實例或方法
            # 為了簡單起見，暫時這樣調用，如果 DBManager.checkpoint_db() 接受 target_db_path
            # (是的，我剛才讓它接受了 target_db_path)
             db_manager.checkpoint_db(target_db_path=args.cache_db_path)


if __name__ == "__main__":
    main()
