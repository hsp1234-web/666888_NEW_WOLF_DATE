# -*- coding: utf-8 -*-
"""
YFinanceClient for Data Hydrator
================================

負責從 yfinance 下載指定時間範圍內的歷史數據，核心功能包括：
1.  **時間分塊 (Chunking)**：將長的時間範圍切成 yfinance API 允許的小塊。
2.  **迭代降級 (Fallback)**：從最精細的數據顆粒度開始嘗試，如果失敗則自動嘗試更粗的顆粒度。
3.  **數據標準化**：統一欄位名、處理缺失的 volume、確保時區為 UTC。
4.  **快取整合**：利用 DBManager 檢查快取，只請求缺失數據。
5.  **強制刷新**：提供選項以忽略快取並重新獲取所有數據。

設計思路：
- `hydrate_data_range` 是主要的外部接口，它協調整個數據回填過程。
- `_get_chunk_size_for_interval` 和 `_split_date_range_into_chunks` 是時間分塊的輔助方法。
- `fetch_single_chunk` 負責抓取單個時間區塊的特定顆粒度數據。
- 降級邏輯在 `hydrate_data_range` 中實現，遍歷 `FALLBACK_INTERVALS`。
- `_convert_missing_dates_to_ranges` 用於將離散的缺失日期合併為連續區間。
"""
import yfinance as yf
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from .db_manager import DBManager

def _convert_missing_dates_to_ranges(missing_dates: list[str]) -> list[tuple[str, str]]:
    if not missing_dates:
        return []
    sorted_missing_dates = sorted(list(set(missing_dates)), key=lambda d: datetime.strptime(d, "%Y-%m-%d"))
    ranges = []
    if not sorted_missing_dates:
        return ranges
    start_of_range = sorted_missing_dates[0]
    end_of_range = sorted_missing_dates[0]
    for i in range(1, len(sorted_missing_dates)):
        current_date_obj = datetime.strptime(sorted_missing_dates[i], "%Y-%m-%d").date()
        prev_date_obj = datetime.strptime(end_of_range, "%Y-%m-%d").date()
        if (current_date_obj - prev_date_obj).days == 1:
            end_of_range = sorted_missing_dates[i]
        else:
            ranges.append((start_of_range, end_of_range))
            start_of_range = sorted_missing_dates[i]
            end_of_range = sorted_missing_dates[i]
    ranges.append((start_of_range, end_of_range))
    print(f"DEBUG (_convert_missing_dates_to_ranges): missing_dates={missing_dates}, ranges={ranges}")
    return ranges

class YFinanceClient:
    def __init__(self, db_manager: DBManager, cache_dir="data_workspace/cache/yfinance_hydrator"):
        self.db_manager = db_manager
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self.FALLBACK_INTERVALS = ['1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo']
        self.HISTORICAL_FALLBACK = ['1d', '1wk', '1mo'] # 新增：歷史數據回溯策略
        print(f"INFO: YFinanceClient (Data Hydrator v33.0) 初始化完畢。標準 Fallback: {self.FALLBACK_INTERVALS}, 歷史 Fallback: {self.HISTORICAL_FALLBACK}")

    def _get_chunk_size_for_interval(self, interval: str) -> int:
        if interval == '1m': return 6
        elif interval in ['2m', '5m', '15m', '30m']: return 55
        elif interval in ['60m', '90m', '1h']: return 700
        elif interval in ['1d', '5d', '1wk']: return 365 * 2
        elif interval in ['1mo', '3mo']: return 365 * 5
        else:
            print(f"警告: 未知的 interval '{interval}'，預設 chunk_size_days 為 30。")
            return 30

    def _split_date_range_into_chunks(self, start_date_str: str, end_date_str: str, chunk_size_days: int) -> list[tuple[str, str]]:
        chunks = []
        try:
            current_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            final_end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError as e:
            print(f"錯誤: 日期格式錯誤 ({start_date_str}, {end_date_str})。請使用 YYYY-MM-DD 格式。 {e}")
            return []
        while current_start_date <= final_end_date:
            chunk_actual_end_date = current_start_date + timedelta(days=chunk_size_days - 1)
            if chunk_actual_end_date > final_end_date:
                chunk_actual_end_date = final_end_date
            yfinance_end_date = chunk_actual_end_date + timedelta(days=1)
            chunks.append((current_start_date.strftime("%Y-%m-%d"), yfinance_end_date.strftime("%Y-%m-%d")))
            current_start_date = chunk_actual_end_date + timedelta(days=1)
        return chunks

    def fetch_single_chunk(self, ticker: str, chunk_start_date_str: str, chunk_end_date_str: str, interval: str) -> pd.DataFrame | None:
        print(f"INFO: fetch_single_chunk: Ticker={ticker}, Interval={interval}, Start={chunk_start_date_str}, End(Exclusive)={chunk_end_date_str}")
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(start=chunk_start_date_str, end=chunk_end_date_str, interval=interval, auto_adjust=True, prepost=False)
            if data is None or data.empty:
                print(f"警告: fetch_single_chunk: {ticker} 在 {chunk_start_date_str} 到 {chunk_end_date_str} (間隔: {interval}) 無數據返回。")
                return None
            if isinstance(data.index, pd.DatetimeIndex):
                data = data.reset_index()
            data.columns = [col.lower() for col in data.columns]
            if 'date' in data.columns and 'datetime' not in data.columns:
                data.rename(columns={'date': 'datetime'}, inplace=True)
            elif 'Datetime' in data.columns and 'datetime' not in data.columns: # Legacy check
                 data.rename(columns={'Datetime': 'datetime'}, inplace=True)
            if 'datetime' not in data.columns:
                print(f"錯誤: fetch_single_chunk: 標準化後 DataFrame 中缺少 'datetime' 欄位。股票: {ticker}, 間隔: {interval}。可用欄位: {data.columns.tolist()}")
                return None
            try:
                data['datetime'] = pd.to_datetime(data['datetime'])
                if data['datetime'].dt.tz is None:
                    data['datetime'] = data['datetime'].dt.tz_localize('UTC')
                else:
                    data['datetime'] = data['datetime'].dt.tz_convert('UTC')
            except Exception as e:
                print(f"錯誤: fetch_single_chunk: 轉換 'datetime' 欄位時出錯: {e}. 股票: {ticker}, 間隔: {interval}.")
                return None
            if 'volume' not in data.columns: data['volume'] = 0
            data['volume'] = data['volume'].fillna(0).astype('int64')
            data['interval'] = interval
            data['ticker'] = ticker
            final_columns = ['datetime', 'ticker', 'interval', 'open', 'high', 'low', 'close', 'volume']
            missing_ohlc_cols = [col for col in ['open', 'high', 'low', 'close'] if col not in data.columns]
            if missing_ohlc_cols:
                print(f"警告: fetch_single_chunk: DataFrame 缺少部分OHLC欄位: {missing_ohlc_cols}。股票: {ticker}, 間隔: {interval}。將嘗試填充為0。")
                for col in missing_ohlc_cols: data[col] = 0.0
            try:
                data = data[final_columns]
            except KeyError as e:
                print(f"錯誤: fetch_single_chunk: 選取最終欄位時發生 KeyError: {e}。股票: {ticker}, 間隔: {interval}。可用欄位: {data.columns.tolist()}")
                return None
            print(f"INFO: fetch_single_chunk: 成功獲取並標準化 {len(data)} 筆數據。")
            return data
        except Exception as e:
            print(f"錯誤: fetch_single_chunk: 抓取或處理 {ticker} ({interval}, {chunk_start_date_str}-{chunk_end_date_str}) 失敗: {type(e).__name__} - {e}")
            return None

    def hydrate_data_range(self, ticker: str, start_date_str: str, end_date_str: str, db_table_name: str = "market_ohlcv_analyzer", force_refresh: bool = False) -> tuple[pd.DataFrame | None, dict]:
        print(f"===== 開始數據回填任務 (v33.0 Intelligent Hound): Ticker={ticker}, Range=[{start_date_str} to {end_date_str}], ForceRefresh={force_refresh} =====")
        overall_execution_log = {}
        try:
            start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            request_date_objects = pd.date_range(start_date_obj, end_date_obj)
            request_date_range_str_list = [d.strftime("%Y-%m-%d") for d in request_date_objects]
        except ValueError as e:
            print(f"錯誤 (hydrate_data_range): 無效的 start_date_str 或 end_date_str: {e}")
            overall_execution_log["error"] = f"Invalid date range: {start_date_str} to {end_date_str}. Details: {e}"
            return None, overall_execution_log

        for date_str_in_range in request_date_range_str_list:
            overall_execution_log.setdefault(date_str_in_range, {}).setdefault(ticker, {
                "status": "pending", "interval": None, "count": 0, "message": "Awaiting processing"
            })

        # --- 「智能考古」：存在性預檢 ---
        # 僅對超過30天的歷史數據執行
        if (end_date_obj - start_date_obj).days > 30:
            print(f"INFO: hydrate_data_range: Ticker={ticker}. Performing existence pre-flight check for historical range [{start_date_str} to {end_date_str}] (requested >30 days).")
            # 使用最粗顆粒度 '1mo' 請求整個範圍
            # yfinance 的 end date 是 exclusive, 所以要加一天
            preflight_end_date_str = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            preflight_data = self.fetch_single_chunk(ticker, start_date_str, preflight_end_date_str, '1mo')
            if preflight_data is None or preflight_data.empty:
                print(f"INFO: Pre-flight check for {ticker} from {start_date_str} to {end_date_str} (1mo) failed. Skipping all intervals.")
                for date_str_in_log_range in request_date_range_str_list:
                    overall_execution_log[date_str_in_log_range][ticker].update({
                        "status": "preflight_failed_empty",
                        "interval": "1mo_preflight",
                        "count": 0,
                        "message": f"Pre-flight check for {ticker} over [{start_date_str}-{end_date_str}] returned no data with '1mo'. Assuming no data in this historical range."
                    })
                # print(f"DEBUG: PREFLIGHT FAILED, ATTEMPTING TO RETURN NOW FOR TICKER {ticker}") # 移除調試日誌
                print(f"===== 數據回填任務結束 (預檢失敗): Ticker={ticker} =====")
                return None, overall_execution_log
            else:
                print(f"INFO: Pre-flight check for {ticker} from {start_date_str} to {end_date_str} (1mo) successful. Proceeding with detailed fetch.")
        # --- 結束 「存在性預檢」 ---

        # --- 「智能考古」：時間感知回溯 ---
        request_start_date_actual = start_date_obj.date()
        thirty_days_ago_date = (datetime.now() - timedelta(days=30)).date()

        current_fallback_intervals = self.FALLBACK_INTERVALS
        if request_start_date_actual < thirty_days_ago_date:
            print(f"INFO: hydrate_data_range: Ticker={ticker}. Request for historical data (start date {start_date_str} is older than 30 days). Using HISTORICAL_FALLBACK: {self.HISTORICAL_FALLBACK}")
            current_fallback_intervals = self.HISTORICAL_FALLBACK
        else:
            print(f"INFO: hydrate_data_range: Ticker={ticker}. Request for recent data (start date {start_date_str} is within 30 days). Using FALLBACK_INTERVALS: {self.FALLBACK_INTERVALS}")
        # --- 結束 「時間感知回溯」 ---

        for interval in current_fallback_intervals:
            print(f"\nINFO: hydrate_data_range: Ticker={ticker}. 正在評估顆粒度 '{interval}' for range [{start_date_str} to {end_date_str}]...")
            chunk_size_days = self._get_chunk_size_for_interval(interval)
            if chunk_size_days <= 0: continue

            print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. 檢查資料庫快取...")
            cached_df, missing_dates_list = self.db_manager.check_cache(
                ticker=ticker, start_date_str=start_date_str, end_date_str=end_date_str,
                interval=interval, table_name=db_table_name
            )

            if force_refresh:
                print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Force refresh ENABLED. Ignoring cache and fetching all dates in range.")
                missing_dates_list = request_date_range_str_list[:]
                cached_df = pd.DataFrame()

            if not force_refresh and cached_df is not None and not cached_df.empty:
                if 'datetime' in cached_df.columns and pd.api.types.is_datetime64_any_dtype(cached_df['datetime']):
                    if cached_df['datetime'].dt.tz is None: cached_df['datetime'] = cached_df['datetime'].dt.tz_localize('UTC')
                    else: cached_df['datetime'] = cached_df['datetime'].dt.tz_convert('UTC')
                    unique_cached_dates = cached_df['datetime'].dt.normalize().unique()
                    for date_obj_in_cached_df in unique_cached_dates:
                        log_date_str = date_obj_in_cached_df.strftime('%Y-%m-%d')
                        if log_date_str in overall_execution_log and ticker in overall_execution_log[log_date_str]:
                            daily_rows_cached = cached_df[cached_df['datetime'].dt.date == date_obj_in_cached_df.date()]
                            overall_execution_log[log_date_str][ticker].update({
                                "status": "cached", "interval": interval, "count": len(daily_rows_cached),
                                "message": f"Data for {log_date_str} found in cache with {interval} ({len(daily_rows_cached)} rows)."
                            })
                else:
                    print(f"警告 (hydrate_data_range): cached_df for {ticker} ({interval}) 'datetime' column issue, log update for cache might be incomplete.")

            if not force_refresh and not missing_dates_list:
                print(f"成功: hydrate_data_range: Ticker={ticker}, Interval={interval}. 所有請求數據 ({start_date_str} to {end_date_str}) 均在快取中。")
                for log_date_str in request_date_range_str_list:
                    count_for_day = 0
                    if cached_df is not None and not cached_df.empty and \
                       'datetime' in cached_df.columns and \
                       pd.api.types.is_datetime64_any_dtype(cached_df['datetime']):
                        daily_rows = cached_df[cached_df['datetime'].dt.strftime('%Y-%m-%d') == log_date_str]
                        count_for_day = len(daily_rows)
                    current_interval_in_log = overall_execution_log[log_date_str][ticker].get('interval', interval)
                    if overall_execution_log[log_date_str][ticker].get('status') == 'cached':
                        current_interval_in_log = overall_execution_log[log_date_str][ticker]['interval']
                    overall_execution_log[log_date_str][ticker].update({
                        "status": "cached_full_hit_verified", "interval": current_interval_in_log, "count": count_for_day,
                        "message": f"Verified full cache hit for {log_date_str} with {current_interval_in_log} ({count_for_day} rows)."
                    })
                print(f"===== 數據回填任務結束 (完全快取命中): Ticker={ticker}, Interval={interval} =====")
                return cached_df, overall_execution_log

            if missing_dates_list: # Only proceed if there are dates to fetch (either genuinely missing or due to force_refresh)
                if not force_refresh: # Only print this if not forcing refresh
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. 快取中缺失 {len(missing_dates_list)} 天的數據。準備從 API 獲取...")
            else: # No missing dates and not force_refresh (this case should be caught by 'if not force_refresh and not missing_dates_list:')
                 print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. No missing dates and not forcing refresh. Should have returned earlier.")
                 # This path should ideally not be hit if logic above is correct.
                 # If it is, it means cached_df was empty or None, but missing_dates_list was also empty.
                 # Treat as "no data for interval"
                 if not force_refresh: # Ensure we don't overwrite force_refresh logic
                    for log_date_str in request_date_range_str_list:
                        if overall_execution_log[log_date_str][ticker]['status'] in ['pending', 'cached']:
                             overall_execution_log[log_date_str][ticker].update({
                                "status": "no_data_for_interval_final", "interval": interval, "count": 0,
                                "message": f"No data ultimately found or fetched for {log_date_str} with {interval} (empty cache, no missing dates reported)." })
                    print(f"===== 數據回填任務結束 (無數據): Ticker={ticker}, Interval={interval} =====")
                    return pd.DataFrame(), overall_execution_log # Return empty DataFrame

            missing_date_ranges = _convert_missing_dates_to_ranges(missing_dates_list)
            newly_fetched_data_all_ranges_dfs = []
            all_missing_ranges_fetched_successfully = True
            thirty_days_ago_date = (datetime.now() - timedelta(days=30)).date()

            for range_idx, (range_start_str, range_end_str) in enumerate(missing_date_ranges):
                print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Fetching missing range {range_idx+1}/{len(missing_date_ranges)}: [{range_start_str} to {range_end_str}]")
                date_chunks_for_missing_range = self._split_date_range_into_chunks(range_start_str, range_end_str, chunk_size_days)
                if not date_chunks_for_missing_range:
                    print(f"警告: hydrate_data_range: Ticker={ticker}, Interval={interval}. 無法為缺失日期範圍 [{range_start_str}-{range_end_str}] 生成有效日期區塊。此顆粒度嘗試終止。")
                    all_missing_ranges_fetched_successfully = False; break

                current_missing_range_all_chunks_dfs = []
                current_missing_range_fetch_ok = True
                for chunk_idx, (chunk_start_str, chunk_end_exclusive_str) in enumerate(date_chunks_for_missing_range):
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Processing chunk {chunk_idx+1}/{len(date_chunks_for_missing_range)} for missing range: [{chunk_start_str} to {chunk_end_exclusive_str} exclusive]")
                    chunk_start_date_obj_for_check = datetime.strptime(chunk_start_str, "%Y-%m-%d").date()
                    if interval == '1m' and chunk_start_date_obj_for_check < thirty_days_ago_date:
                        print(f"INFO: hydrate_data_range: Ticker={ticker}. Chunk [{chunk_start_str} to {chunk_end_exclusive_str}) for '1m' data is outside 30-day window. Skipping this chunk for '1m'.")
                        current_missing_range_fetch_ok = False
                        temp_log_date = datetime.strptime(chunk_start_str, "%Y-%m-%d")
                        temp_log_end_date = datetime.strptime(chunk_end_exclusive_str, "%Y-%m-%d") - timedelta(days=1)
                        while temp_log_date <= temp_log_end_date:
                            log_d_str = temp_log_date.strftime("%Y-%m-%d")
                            if log_d_str in overall_execution_log and (force_refresh or overall_execution_log[log_d_str][ticker]['status'] not in ['success', 'cached_full_hit_verified']):
                                overall_execution_log[log_d_str][ticker].update({
                                    "status": "skipped_1m_api_due_to_30day_limit", "interval": "1m", "count": 0,
                                    "message": f"API fetch for 1m data on {log_d_str} skipped (30-day limit)." })
                            temp_log_date += timedelta(days=1)
                        break

                    chunk_df = self.fetch_single_chunk(ticker, chunk_start_str, chunk_end_exclusive_str, interval)
                    temp_log_date = datetime.strptime(chunk_start_str, "%Y-%m-%d")
                    temp_log_end_date = datetime.strptime(chunk_end_exclusive_str, "%Y-%m-%d") - timedelta(days=1)
                    while temp_log_date <= temp_log_end_date:
                        log_d_str = temp_log_date.strftime("%Y-%m-%d")
                        if log_d_str in overall_execution_log and \
                           (force_refresh or overall_execution_log[log_d_str][ticker]['status'] not in ['success', 'cached_full_hit_verified']) and \
                           overall_execution_log[log_d_str][ticker]['status'] != 'skipped_1m_api_due_to_30day_limit' :
                            if chunk_df is not None and not chunk_df.empty:
                                daily_rows_in_fetched_chunk = chunk_df[chunk_df['datetime'].dt.date == temp_log_date.date()]
                                overall_execution_log[log_d_str][ticker].update({
                                    "status": "api_success_partial_range" if len(date_chunks_for_missing_range) > 1 else "api_success_full_range",
                                    "interval": interval, "count": len(daily_rows_in_fetched_chunk),
                                    "message": f"API fetched {len(daily_rows_in_fetched_chunk)} rows for {log_d_str} with {interval}." })
                            else:
                                overall_execution_log[log_d_str][ticker].update({
                                    "status": "api_failed_chunk", "interval": interval, "count": 0,
                                    "message": f"API fetch failed for chunk covering {log_d_str} with {interval}."})
                        temp_log_date += timedelta(days=1)

                    if chunk_df is not None and not chunk_df.empty:
                        current_missing_range_all_chunks_dfs.append(chunk_df)
                    else: # chunk_df is None or empty
                        print(f"警告: hydrate_data_range: Ticker={ticker}, Interval={interval}. Chunk [{chunk_start_str}-{chunk_end_exclusive_str}) 數據抓取失敗或為空。此 missing range 的 '{interval}' 嘗試終止。")
                        current_missing_range_fetch_ok = False; break

                if not current_missing_range_fetch_ok:
                    all_missing_ranges_fetched_successfully = False; break

                if current_missing_range_all_chunks_dfs:
                    single_missing_range_df = pd.concat(current_missing_range_all_chunks_dfs, ignore_index=True)
                    if not single_missing_range_df.empty:
                        print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Storing {len(single_missing_range_df)} newly fetched rows for range [{range_start_str}-{range_end_str}] to DB.")
                        self.db_manager.upsert_data(single_missing_range_df, table_name=db_table_name)
                        newly_fetched_data_all_ranges_dfs.append(single_missing_range_df)
                elif current_missing_range_fetch_ok and not current_missing_range_all_chunks_dfs :
                     print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Missing range [{range_start_str}-{range_end_str}] 所有區塊均未返回數據。")

            if all_missing_ranges_fetched_successfully:
                all_dfs_to_concat = []
                if not force_refresh and cached_df is not None and not cached_df.empty: all_dfs_to_concat.append(cached_df)
                if newly_fetched_data_all_ranges_dfs: all_dfs_to_concat.extend(newly_fetched_data_all_ranges_dfs)

                if not all_dfs_to_concat:
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. No data found in cache and no new data fetched.")
                    final_df = pd.DataFrame()
                    if not force_refresh: # Only update to "no_data_for_interval_final" if not a force_refresh that simply found no data
                        for log_date_str in request_date_range_str_list:
                            if overall_execution_log[log_date_str][ticker]['status'] in ['pending', 'cached', 'api_failed_chunk', f"failed_interval_{interval}", 'skipped_1m_api_due_to_30day_limit']:
                                 overall_execution_log[log_date_str][ticker].update({
                                    "status": "no_data_for_interval_final", "interval": interval, "count": 0,
                                    "message": f"No data ultimately found or fetched for {log_date_str} with {interval}."})
                    print(f"===== 數據回填任務結束 (無新數據或已全在快取): Ticker={ticker}, Interval={interval} =====")
                    return final_df, overall_execution_log

                final_df = pd.concat(all_dfs_to_concat, ignore_index=True)
                if 'datetime' not in final_df.columns or not pd.api.types.is_datetime64_any_dtype(final_df['datetime']):
                    try:
                        if 'datetime' in final_df.columns: final_df['datetime'] = pd.to_datetime(final_df['datetime'], utc=True)
                        else: raise ValueError("final_df is missing 'datetime' column for final log update")
                    except Exception as e_final_conv:
                         print(f"錯誤(hydrate_data_range): final_df['datetime'] 處理失敗 for {ticker} ({interval}): {e_final_conv}")
                         return final_df, overall_execution_log

                unique_dates_in_final_df = final_df['datetime'].dt.normalize().unique()
                for date_obj_in_final_df in unique_dates_in_final_df:
                    date_str_in_final_df_range = date_obj_in_final_df.strftime('%Y-%m-%d')
                    if date_str_in_final_df_range in overall_execution_log and ticker in overall_execution_log[date_str_in_final_df_range]:
                        daily_rows_final = final_df[final_df['datetime'].dt.date == date_obj_in_final_df.date()]
                        if not daily_rows_final.empty:
                            overall_execution_log[date_str_in_final_df_range][ticker].update({
                                "status": "success", "interval": interval, "count": len(daily_rows_final),
                                "message": f"Final data for {date_str_in_final_df_range} with {interval} ({len(daily_rows_final)} rows) from cache/API."})
                        elif overall_execution_log[date_str_in_final_df_range][ticker].get('status') != 'success':
                             existing_msg = overall_execution_log[date_str_in_final_df_range][ticker].get("message", "")
                             overall_execution_log[date_str_in_final_df_range][ticker]['message'] = existing_msg + f" No data for {date_str_in_final_df_range} in final combined df with {interval} (unexpected)."

                if not final_df.empty:
                    final_df.drop_duplicates(subset=['ticker', 'interval', 'datetime'], keep='first', inplace=True)
                    final_df.sort_values(by='datetime', ascending=True, inplace=True)
                    final_df.reset_index(drop=True, inplace=True)

                print(f"成功: hydrate_data_range: Ticker={ticker}, Interval={interval}. 已完成數據回填 [{start_date_str} to {end_date_str}]。最終共 {len(final_df)} 筆。")
                print(f"===== 數據回填任務結束 (成功): Ticker={ticker}, Interval={interval} =====")
                return final_df, overall_execution_log
            else:
                 print(f"INFO: hydrate_data_range: Ticker={ticker}. 顆粒度 '{interval}' 未能成功獲取所有缺失數據。嘗試下一個更粗的顆粒度。")
                 for log_date_str in request_date_range_str_list:
                    current_status = overall_execution_log[log_date_str][ticker]['status']
                    if force_refresh or current_status in ['pending', 'api_success_partial_range', 'api_success_full_range',
                                                          'api_failed_chunk', 'skipped_1m_api_due_to_30day_limit',
                                                          f'failed_interval_{interval}', 'cached']:
                        existing_message = overall_execution_log[log_date_str][ticker].get("message", "")
                        failure_message_part = f" Interval {interval} failed to provide data for {log_date_str}."
                        if failure_message_part not in existing_message :
                             existing_message += failure_message_part
                        overall_execution_log[log_date_str][ticker].update({
                            "status": f"failed_interval_{interval}", "message": existing_message })
            time.sleep(0.5)

        print(f"錯誤: hydrate_data_range: Ticker={ticker}. 所有降級顆粒度 {self.FALLBACK_INTERVALS} 均無法為 [{start_date_str} to {end_date_str}] 回填完整數據。")
        print(f"===== 數據回填任務結束 (所有 Interval 均失敗): Ticker={ticker} =====")
        for log_date_str in request_date_range_str_list:
            final_status = overall_execution_log[log_date_str][ticker]['status']
            if 'success' not in final_status and 'cached_full_hit_verified' not in final_status :
                 current_message = overall_execution_log[log_date_str][ticker].get("message","")
                 all_fail_msg_part = f" All intervals failed for {log_date_str}."
                 if all_fail_msg_part not in current_message: current_message += all_fail_msg_part
                 overall_execution_log[log_date_str][ticker].update({ "status": "failed_all_intervals", "interval": None, "count": 0, "message": current_message})
        return None, overall_execution_log

if __name__ == '__main__':
    print("--- YFinanceClient (Daily Market Analyzer) 測試 ---")
    # 此處的 __main__ 僅為示例，實際測試應通過 test_yfinance_client.py 進行
    # 需要一個 DBManager 實例來運行
    # from apps.daily_market_analyzer.db_manager import DBManager
    # db_man = DBManager("data_workspace/temp/test_main_yfc.duckdb")
    # client = YFinanceClient(db_manager=db_man)
    # client.hydrate_data_range("AAPL", "2024-01-01", "2024-01-05", force_refresh=True)
    print("INFO: __main__ 測試部分需要 DBManager 實例。由於依賴關係，請通過整合測試來驗證 YFinanceClient 的新快取邏輯。")
    print("--- YFinanceClient (Daily Market Analyzer) __main__ 測試部分已簡化/跳過 ---")
    test_ticker_aapl = "AAPL"
    end_date_dt_recent = datetime.now() - timedelta(days=1)
    start_date_dt_recent = end_date_dt_recent - timedelta(days=2)
    test_start_recent = start_date_dt_recent.strftime("%Y-%m-%d")
    test_end_recent = end_date_dt_recent.strftime("%Y-%m-%d")
    print(f"\n--- 示例呼叫 (不執行): {test_ticker_aapl}, 近期範圍: [{test_start_recent} to {test_end_recent}] ---")
    print("\n--- YFinanceClient (Daily Market Analyzer) 測試完畢 ---")
