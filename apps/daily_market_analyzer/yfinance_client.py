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
    def __init__(self, db_manager: DBManager, cache_db_path: str):
        self.db_manager = db_manager
        self.cache_db_path = cache_db_path # 新增快取路徑屬性
        # os.makedirs(self.cache_dir, exist_ok=True) # 假設 cache_db_path 的目錄由 DBManager 或其他機制處理
        self.FALLBACK_INTERVALS = ['1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo']
        # 更新日誌訊息以包含主DB和快取DB的路徑
        print(f"INFO: YFinanceClient (Data Hydrator v2.1) 初始化完畢，主DB: {db_manager.db_path}, 快取DB: {cache_db_path}")

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
        print(f"===== 開始數據回填任務 (v3.1 Intelligent Archaeology): Ticker={ticker}, Range=[{start_date_str} to {end_date_str}], ForceRefresh={force_refresh} =====") # 版本更新
        overall_execution_log = {}
        RECENT_DATA_THRESHOLD_DAYS = 30 # 「近期數據」的時間閾值
        YFINANCE_1M_DATA_MAX_DAYS_OLD = 30 # yfinance '1m' 數據通常只能獲取最近30天（保守值，有時是60天，但請求範圍限制為7天內）
        # YFINANCE_1M_DATA_MAX_RANGE_DAYS = 7 # yfinance '1m' 數據單次請求的最大天數範圍

        try:
            start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            request_date_objects = pd.date_range(start_date_obj, end_date_obj)
            request_date_range_str_list = [d.strftime("%Y-%m-%d") for d in request_date_objects]
        except Exception as e:
            print(f"錯誤 (hydrate_data_range): 無效的 start_date_str 或 end_date_str: {e}")
            overall_execution_log["error"] = f"Invalid date range: {start_date_str} to {end_date_str}. Details: {e}"
            return None, overall_execution_log

        for date_str_in_range in request_date_range_str_list:
            overall_execution_log.setdefault(date_str_in_range, {}).setdefault(ticker, {
                "status": "pending", "interval": None, "count": 0, "message": "Awaiting processing"
            })

        # 判斷是否為歷史數據 (早於 RECENT_DATA_THRESHOLD_DAYS 之前)
        # 我們關心的是請求範圍的起始點是否在“近期”之外
        is_historical_data = (datetime.now() - start_date_obj).days > RECENT_DATA_THRESHOLD_DAYS

        # 1. 「存在性預檢 (Existence Pre-flight Check)」 for historical data
        if is_historical_data and not force_refresh: # 預檢只對非強制刷新的歷史數據有意義
            print(f"INFO: hydrate_data_range (Pre-flight): Ticker={ticker}. 歷史數據 ({start_date_str} to {end_date_str})，執行 '1mo' 存在性預檢...")
            # 預檢使用整個請求範圍，用 '1mo' 嘗試獲取一次
            # 注意：fetch_single_chunk 的 chunk_end_date_str 是 exclusive 的
            preflight_end_date_exclusive_str = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            preflight_df = self.fetch_single_chunk(ticker, start_date_str, preflight_end_date_exclusive_str, '1mo')

            if preflight_df is None or preflight_df.empty:
                print(f"關鍵: hydrate_data_range (Pre-flight): Ticker={ticker}. '1mo' 預檢在 [{start_date_str} to {end_date_str}] 未返回任何數據。判定此標的在該時段不存在。")
                for date_str in request_date_range_str_list:
                    overall_execution_log[date_str][ticker].update({
                        "status": "preflight_no_data", "interval": "1mo", "count": 0,
                        "message": f"'1mo' pre-flight check indicated no data for this period."
                    })
                print(f"===== 數據回填任務結束 (預檢無數據): Ticker={ticker} =====")
                return pd.DataFrame(), overall_execution_log # 返回空 DataFrame

        # 2. 決定回溯策略
        if is_historical_data:
            # 歷史數據：逆轉回溯順序，從 '1d' 開始，如果連日線都失敗，不嘗試分鐘線
            # 考慮到 yfinance 對極久遠數據的小時/分鐘線支持很差，這裡只到 '1d', '1wk', '1mo'
            current_fallback_intervals = ['1d', '1wk', '1mo']
            print(f"INFO: hydrate_data_range: Ticker={ticker}. 歷史數據模式，使用回溯策略: {current_fallback_intervals}")
        else:
            # 近期數據：維持現有策略
            current_fallback_intervals = self.FALLBACK_INTERVALS[:] # 複製一份以防意外修改
            print(f"INFO: hydrate_data_range: Ticker={ticker}. 近期數據模式，使用回溯策略: {current_fallback_intervals}")


        for interval in current_fallback_intervals:
            print(f"\nINFO: hydrate_data_range: Ticker={ticker}. 正在評估顆粒度 '{interval}' for range [{start_date_str} to {end_date_str}]...")
            chunk_size_days = self._get_chunk_size_for_interval(interval)
            if chunk_size_days <= 0: continue

            # 特別處理 '1m' 數據的獲取限制 (yfinance 限制)
            if interval == '1m':
                # 檢查請求的整個範圍是否都在 YFINANCE_1M_DATA_MAX_DAYS_OLD 內
                if (datetime.now() - end_date_obj).days > YFINANCE_1M_DATA_MAX_DAYS_OLD :
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval='1m'. 請求範圍 [{start_date_str}-{end_date_str}] 超出 {YFINANCE_1M_DATA_MAX_DAYS_OLD} 天的 '1m' 數據獲取窗口。跳過此顆粒度。")
                    for date_str in request_date_range_str_list:
                         if overall_execution_log[date_str][ticker]['status'] == 'pending': # 只更新未被其他邏輯處理的日期
                            overall_execution_log[date_str][ticker].update({
                                "status": "skipped_1m_due_to_age_limit", "interval": "1m", "count": 0,
                                "message": f"'1m' data request for {date_str} skipped (too old)."
                            })
                    continue # 跳到下一個 interval

            print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. 檢查資料庫快取...")
            cached_df, missing_dates_list = self.db_manager.check_cache(
                ticker=ticker, start_date_str=start_date_str, end_date_str=end_date_str,
                interval=interval, table_name=db_table_name, target_db_path=self.cache_db_path
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
                            # 確保只更新那些尚未被成功狀態覆蓋的日誌
                            if overall_execution_log[log_date_str][ticker]['status'] not in ['success', 'cached_full_hit_verified', 'preflight_no_data']:
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
                    # 只有當之前的狀態是 pending 或 cached (非最終成功狀態) 時才更新為 cached_full_hit_verified
                    if overall_execution_log[log_date_str][ticker]['status'] in ['pending', 'cached']:
                        count_for_day = 0
                        if cached_df is not None and not cached_df.empty and \
                           'datetime' in cached_df.columns and \
                           pd.api.types.is_datetime64_any_dtype(cached_df['datetime']):
                            daily_rows = cached_df[cached_df['datetime'].dt.strftime('%Y-%m-%d') == log_date_str]
                            count_for_day = len(daily_rows)

                        current_interval_in_log = overall_execution_log[log_date_str][ticker].get('interval')
                        if not current_interval_in_log or overall_execution_log[log_date_str][ticker]['status'] == 'pending':
                             current_interval_in_log = interval # 如果之前沒有 interval (pending), 或只是 cached 但沒有 interval 細節

                        overall_execution_log[log_date_str][ticker].update({
                            "status": "cached_full_hit_verified", "interval": current_interval_in_log, "count": count_for_day,
                            "message": f"Verified full cache hit for {log_date_str} with {current_interval_in_log} ({count_for_day} rows)."
                        })
                print(f"===== 數據回填任務結束 (完全快取命中): Ticker={ticker}, Interval={interval} =====")
                # 即使完全快取命中，也需要返回合併後的數據，因為調用者可能依賴這個返回
                # 確保返回的 cached_df 是針對請求範圍的，並且已進行必要的清理
                if cached_df is not None and not cached_df.empty:
                    # 過濾 cached_df 以確保只包含請求日期範圍內的數據
                    # (DBManager.check_cache 應該已經處理了這個，但再次確認無妨)
                    # 並進行去重和排序
                    cached_df.drop_duplicates(subset=['ticker', 'interval', 'datetime'], keep='first', inplace=True)
                    cached_df.sort_values(by='datetime', ascending=True, inplace=True)
                    cached_df.reset_index(drop=True, inplace=True)
                return cached_df, overall_execution_log

            if missing_dates_list:
                if not force_refresh:
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. 快取中缺失 {len(missing_dates_list)} 天的數據。準備從 API 獲取...")
            else: # No missing dates and not force_refresh
                 print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. No missing dates and not forcing refresh (should have been caught by 'cached_full_hit_verified' logic).")
                 if not force_refresh:
                    for log_date_str in request_date_range_str_list:
                        if overall_execution_log[log_date_str][ticker]['status'] in ['pending', 'cached']:
                             overall_execution_log[log_date_str][ticker].update({
                                "status": "no_data_for_interval_final", "interval": interval, "count": 0,
                                "message": f"No data ultimately found or fetched for {log_date_str} with {interval} (empty cache, no missing dates reported)." })
                    print(f"===== 數據回填任務結束 (無數據): Ticker={ticker}, Interval={interval} =====")
                    return pd.DataFrame(), overall_execution_log

            missing_date_ranges = _convert_missing_dates_to_ranges(missing_dates_list)
            newly_fetched_data_all_ranges_dfs = []
            all_missing_ranges_fetched_successfully = True
            # thirty_days_ago_date = (datetime.now() - timedelta(days=YFINANCE_1M_DATA_MAX_DAYS_OLD)).date() # 改用 YFINANCE_1M_DATA_MAX_DAYS_OLD

            for range_idx, (range_start_str_missing, range_end_str_missing) in enumerate(missing_date_ranges):
                print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Fetching missing range {range_idx+1}/{len(missing_date_ranges)}: [{range_start_str_missing} to {range_end_str_missing}]")

                # 對於 '1m' 數據，再次確認此 missing range 是否符合 yfinance 的限制
                if interval == '1m':
                    missing_range_start_obj = datetime.strptime(range_start_str_missing, "%Y-%m-%d")
                    # missing_range_end_obj = datetime.strptime(range_end_str_missing, "%Y-%m-%d")
                    if (datetime.now() - missing_range_start_obj).days > YFINANCE_1M_DATA_MAX_DAYS_OLD :
                        print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval='1m'. Missing range [{range_start_str_missing}-{range_end_str_missing}] is too old for '1m' data. Skipping this range for '1m'.")
                        # 更新日誌，標記此範圍內的日期為跳過
                        temp_log_date = missing_range_start_obj
                        temp_log_end_date_obj = datetime.strptime(range_end_str_missing, "%Y-%m-%d")
                        while temp_log_date <= temp_log_end_date_obj:
                            log_d_str = temp_log_date.strftime("%Y-%m-%d")
                            if log_d_str in overall_execution_log and overall_execution_log[log_d_str][ticker]['status'] == 'pending':
                                overall_execution_log[log_d_str][ticker].update({
                                    "status": "skipped_1m_api_due_to_age_limit_in_range", "interval": "1m", "count": 0,
                                    "message": f"API fetch for 1m data on {log_d_str} skipped (too old for this specific missing range)." })
                            temp_log_date += timedelta(days=1)
                        continue # 處理下一個 missing_date_range

                date_chunks_for_missing_range = self._split_date_range_into_chunks(range_start_str_missing, range_end_str_missing, chunk_size_days)
                if not date_chunks_for_missing_range:
                    print(f"警告: hydrate_data_range: Ticker={ticker}, Interval={interval}. 無法為缺失日期範圍 [{range_start_str_missing}-{range_end_str_missing}] 生成有效日期區塊。此顆粒度嘗試終止。")
                    all_missing_ranges_fetched_successfully = False; break

                current_missing_range_all_chunks_dfs = []
                current_missing_range_fetch_ok = True
                for chunk_idx, (chunk_start_str, chunk_end_exclusive_str) in enumerate(date_chunks_for_missing_range):
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Processing chunk {chunk_idx+1}/{len(date_chunks_for_missing_range)} for missing range: [{chunk_start_str} to {chunk_end_exclusive_str} exclusive]")

                    # 之前的 '1m' 30天限制邏輯已移到更早的位置，此處不再需要重複檢查 chunk_start_date_obj_for_check < thirty_days_ago_date
                    # 因為如果整個請求範圍都不符合 '1m' 的年限，前面就已經跳過了。
                    # 如果是請求範圍內的部分 missing range 不符合，也在 for range_idx 循環開始時跳過了。

                    chunk_df = self.fetch_single_chunk(ticker, chunk_start_str, chunk_end_exclusive_str, interval)
                    temp_log_date = datetime.strptime(chunk_start_str, "%Y-%m-%d")
                    temp_log_end_date = datetime.strptime(chunk_end_exclusive_str, "%Y-%m-%d") - timedelta(days=1) # chunk_end_exclusive_str 是 yf 的 end，所以實際數據到前一天

                    current_date_log_iter = temp_log_date
                    while current_date_log_iter <= temp_log_end_date:
                        log_d_str = current_date_log_iter.strftime("%Y-%m-%d")
                        # 確保 log_d_str 在請求的日期範圍內，並且其狀態允許被 API 結果更新
                        if log_d_str in request_date_range_str_list and \
                           overall_execution_log[log_d_str][ticker]['status'] not in ['success', 'cached_full_hit_verified', 'preflight_no_data', 'skipped_1m_api_due_to_age_limit', 'skipped_1m_api_due_to_age_limit_in_range']:
                            if chunk_df is not None and not chunk_df.empty:
                                daily_rows_in_fetched_chunk = chunk_df[chunk_df['datetime'].dt.date == current_date_log_iter.date()]
                                overall_execution_log[log_d_str][ticker].update({
                                    "status": "api_success_partial_range" if len(date_chunks_for_missing_range) > 1 else "api_success_full_range",
                                    "interval": interval, "count": len(daily_rows_in_fetched_chunk),
                                    "message": f"API fetched {len(daily_rows_in_fetched_chunk)} rows for {log_d_str} with {interval}." })
                            else: # chunk_df is None or empty for this chunk
                                overall_execution_log[log_d_str][ticker].update({
                                    "status": "api_failed_chunk", "interval": interval, "count": 0,
                                    "message": f"API fetch failed or returned no data for chunk covering {log_d_str} with {interval}."})
                        current_date_log_iter += timedelta(days=1)

                    if chunk_df is not None and not chunk_df.empty:
                        current_missing_range_all_chunks_dfs.append(chunk_df)
                    else:
                        print(f"警告: hydrate_data_range: Ticker={ticker}, Interval={interval}. Chunk [{chunk_start_str}-{chunk_end_exclusive_str}) 數據抓取失敗或為空。此 missing range 的 '{interval}' 嘗試終止。")
                        current_missing_range_fetch_ok = False; break

                if not current_missing_range_fetch_ok:
                    all_missing_ranges_fetched_successfully = False; break

                if current_missing_range_all_chunks_dfs:
                    single_missing_range_df = pd.concat(current_missing_range_all_chunks_dfs, ignore_index=True)
                    if not single_missing_range_df.empty:
                        print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. 儲存 {len(single_missing_range_df)} 筆新獲取的數據 (範圍 [{range_start_str_missing}-{range_end_str_missing}]) 至快取資料庫。")
                        self.db_manager.upsert_data(single_missing_range_df, table_name=db_table_name, target_db_path=self.cache_db_path)
                        newly_fetched_data_all_ranges_dfs.append(single_missing_range_df)
                elif current_missing_range_fetch_ok and not current_missing_range_all_chunks_dfs :
                     print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. Missing range [{range_start_str_missing}-{range_end_str_missing}] 所有區塊均未返回數據。")

            if all_missing_ranges_fetched_successfully:
                all_dfs_to_concat = []
                # 如果不是 force_refresh，且快取中有數據，則加入
                if not force_refresh and cached_df is not None and not cached_df.empty:
                    # 需要確保 cached_df 只包含請求日期範圍內的數據，並且 interval 與當前 interval 一致
                    # check_cache 應該已經處理了 interval，但日期範圍可能需要再次過濾
                    # 實際上，DBManager.check_cache 返回的 cached_df 應該已經是過濾好的
                    all_dfs_to_concat.append(cached_df)

                if newly_fetched_data_all_ranges_dfs: all_dfs_to_concat.extend(newly_fetched_data_all_ranges_dfs)

                if not all_dfs_to_concat:
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. No data found in cache and no new data fetched for this interval.")
                    final_df = pd.DataFrame() # 初始化為空 DataFrame
                    # 即使這裡沒有數據，也不意味著整個 hydrate_data_range 失敗，可能是這個 interval 沒數據而已
                    # 日誌更新邏輯需要小心，不要覆蓋掉 preflight_no_data 等狀態
                    if not force_refresh:
                        for log_date_str in request_date_range_str_list:
                            if overall_execution_log[log_date_str][ticker]['status'] in ['pending', 'cached', 'api_failed_chunk', f"failed_interval_{interval}", 'skipped_1m_api_due_to_age_limit', 'skipped_1m_api_due_to_age_limit_in_range']:
                                 overall_execution_log[log_date_str][ticker].update({
                                    "status": "no_data_for_current_interval", # 更精確的狀態
                                    "interval": interval, "count": 0,
                                    "message": f"No data found or fetched for {log_date_str} with current interval {interval}."})
                    # 如果 is_historical_data 且 interval 是 '1d' 且沒有獲取到數據，這是一個重要信號
                    if is_historical_data and interval == '1d' and final_df.empty:
                        print(f"關鍵: hydrate_data_range: Ticker={ticker}. 歷史數據模式下，'1d' 顆粒度未獲取到任何數據。終止此標的的後續降級嘗試。")
                        for log_date_str in request_date_range_str_list:
                            if overall_execution_log[log_date_str][ticker]['status'] not in ['success', 'cached_full_hit_verified', 'preflight_no_data']:
                                overall_execution_log[log_date_str][ticker].update({
                                "status": "failed_historical_1d", "interval": "1d", "count": 0,
                                "message": f"Historical data fetch failed at '1d' interval for {log_date_str}. No finer intervals will be attempted."})
                        print(f"===== 數據回填任務結束 (歷史數據 '1d' 失敗): Ticker={ticker} =====")
                        return pd.DataFrame(), overall_execution_log # 返回空 DataFrame
                    # 對於近期數據，或歷史數據但非 '1d'，繼續嘗試下一個 interval
                    # 此處不應 return，而是讓 for interval 循環繼續
                    # print(f"===== 數據回填任務中間狀態 (當前 interval 無數據): Ticker={ticker}, Interval={interval} =====")
                    # return final_df, overall_execution_log # <== 錯誤：不應該在這裡返回，除非是特殊終止條件

                if not all_dfs_to_concat: # 如果在嘗試了 concat 之後仍然是空的
                    # (上面的邏輯修改後，這裡的 final_df 可能是空的，但不一定代表結束)
                    # 這裡的處理邏輯與上面 "No data found in cache and no new data fetched for this interval." 類似
                    # 需要判斷是否是歷史數據的 '1d' 失敗情況
                    if is_historical_data and interval == '1d':
                        # (這部分邏輯已包含在上面 final_df.empty 的判斷中，此處可視為冗餘或備用)
                        print(f"冗餘檢查: 歷史數據 '1d' 失敗，但 all_dfs_to_concat 為空。")
                        # ... (省略與上面類似的日誌更新和返回) ...
                        # return pd.DataFrame(), overall_execution_log
                    # 否則，只是這個 interval 沒數據，繼續下一個 interval
                    print(f"INFO: hydrate_data_range: Ticker={ticker}, Interval={interval}. All_dfs_to_concat is empty. Continuing to next interval if any.")
                    # 不需要特別做什麼，讓 for interval 循環繼續
                else: # all_dfs_to_concat 有數據
                    final_df = pd.concat(all_dfs_to_concat, ignore_index=True)
                    if 'datetime' not in final_df.columns or not pd.api.types.is_datetime64_any_dtype(final_df['datetime']):
                        try:
                            if 'datetime' in final_df.columns: final_df['datetime'] = pd.to_datetime(final_df['datetime'], utc=True)
                            else: raise ValueError("final_df is missing 'datetime' column for final log update")
                        except Exception as e_final_conv:
                             print(f"錯誤(hydrate_data_range): final_df['datetime'] 處理失敗 for {ticker} ({interval}): {e_final_conv}")
                             # 即使轉換失敗，也嘗試返回已有的 execution_log
                             return final_df if 'final_df' in locals() and final_df is not None else pd.DataFrame(), overall_execution_log

                    # 更新 overall_execution_log 的 'success' 狀態
                    # 只有當數據確實來自這個 interval 的 API 請求或這個 interval 的快取時，才標記為 success for this interval
                    # 如果數據是從更粗糙的 interval 快取來的，這裡不應覆蓋
                    unique_dates_in_final_df = final_df['datetime'].dt.normalize().unique()
                    for date_obj_in_final_df in unique_dates_in_final_df:
                        date_str_in_final_df_range = date_obj_in_final_df.strftime('%Y-%m-%d')
                        if date_str_in_final_df_range in request_date_range_str_list: #確保只處理請求範圍內的日期
                            # 檢查此日期的數據是否真的由此 interval (或其快取) 提供
                            # 我們可以檢查 overall_execution_log 中此日期的狀態是否還是 'pending' 或 'api_success_partial/full_range' for this interval
                            current_log_status = overall_execution_log[date_str_in_final_df_range][ticker]['status']
                            is_from_current_interval_api = f"api_success" in current_log_status and overall_execution_log[date_str_in_final_df_range][ticker]['interval'] == interval
                            is_from_current_interval_cache = current_log_status == "cached" and overall_execution_log[date_str_in_final_df_range][ticker]['interval'] == interval

                            if is_from_current_interval_api or is_from_current_interval_cache or current_log_status == "pending": # 或者是首次成功
                                daily_rows_final = final_df[final_df['datetime'].dt.date == date_obj_in_final_df.date()]
                                if not daily_rows_final.empty:
                                    overall_execution_log[date_str_in_final_df_range][ticker].update({
                                        "status": "success", "interval": interval, "count": len(daily_rows_final),
                                        "message": f"Final data for {date_str_in_final_df_range} with {interval} ({len(daily_rows_final)} rows) from cache/API."})
                                # 如果 daily_rows_final 為空但之前狀態是 pending/api_success，這不應該發生，但作為防禦
                                elif overall_execution_log[date_str_in_final_df_range][ticker].get('status') != 'success':
                                     existing_msg = overall_execution_log[date_str_in_final_df_range][ticker].get("message", "")
                                     overall_execution_log[date_str_in_final_df_range][ticker]['message'] = existing_msg + f" No data for {date_str_in_final_df_range} in final combined df with {interval} (unexpected)."


                    if not final_df.empty:
                        final_df.drop_duplicates(subset=['ticker', 'interval', 'datetime'], keep='first', inplace=True)
                        final_df.sort_values(by='datetime', ascending=True, inplace=True)
                        final_df.reset_index(drop=True, inplace=True)

                    print(f"成功: hydrate_data_range: Ticker={ticker}, Interval={interval}. 已完成數據回填 [{start_date_str} to {end_date_str}]。最終共 {len(final_df)} 筆。")
                    print(f"===== 數據回填任務結束 (成功): Ticker={ticker}, Interval={interval} =====")
                    return final_df, overall_execution_log # 成功獲取數據，返回

            else: # all_missing_ranges_fetched_successfully is False for current interval
                 print(f"INFO: hydrate_data_range: Ticker={ticker}. 顆粒度 '{interval}' 未能成功獲取所有缺失數據。嘗試下一個更粗的顆粒度。")
                 for log_date_str in request_date_range_str_list:
                    current_status = overall_execution_log[log_date_str][ticker]['status']
                    # 更新日誌，標記此 interval 失敗，除非已有更明確的失敗原因或成功狀態
                    if current_status not in ['success', 'cached_full_hit_verified', 'preflight_no_data', 'failed_historical_1d', 'skipped_1m_api_due_to_age_limit', 'skipped_1m_api_due_to_age_limit_in_range'] and not current_status.startswith("failed_interval_"):
                        existing_message = overall_execution_log[log_date_str][ticker].get("message", "")
                        failure_message_part = f" Interval {interval} failed to provide data for {log_date_str}."
                        if failure_message_part not in existing_message :
                             existing_message += failure_message_part
                        overall_execution_log[log_date_str][ticker].update({
                            "status": f"failed_interval_{interval}", "message": existing_message, "interval": interval }) # 記錄失敗的 interval
            time.sleep(0.5) # 避免 API 過快請求

        # 如果循環結束後仍然沒有成功返回
        print(f"錯誤: hydrate_data_range: Ticker={ticker}. 所有嘗試的降級顆粒度 ({current_fallback_intervals}) 均無法為 [{start_date_str} to {end_date_str}] 回填完整數據。")
        print(f"===== 數據回填任務結束 (所有 Interval 均失敗): Ticker={ticker} =====")
        for log_date_str in request_date_range_str_list:
            final_status = overall_execution_log[log_date_str][ticker]['status']
            if final_status not in ['success', 'cached_full_hit_verified', 'preflight_no_data', 'failed_historical_1d']: # 這些是最終狀態
                 current_message = overall_execution_log[log_date_str][ticker].get("message","")
                 all_fail_msg_part = f" All attempted intervals ({current_fallback_intervals}) failed for {log_date_str}."
                 if all_fail_msg_part not in current_message: current_message += all_fail_msg_part
                 overall_execution_log[log_date_str][ticker].update({ "status": "failed_all_intervals", "interval": None, "count": 0, "message": current_message})

        # 決定最終返回的 DataFrame。如果中間某個 interval 成功了，應該已經返回了。
        # 如果執行到這裡，意味著所有 interval 都失敗了，或者 preflight 就失敗了。
        # 在 preflight 失敗的情況下，我們已經返回了空的 DataFrame。
        # 如果是所有 interval 失敗，也應該返回空的 DataFrame。
        return pd.DataFrame(), overall_execution_log # 確保在所有路徑都有 DataFrame 返回

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
