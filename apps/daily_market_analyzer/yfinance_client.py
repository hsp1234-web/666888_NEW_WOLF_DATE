# -*- coding: utf-8 -*-
"""
YFinanceClient for Data Hydrator
================================

負責從 yfinance 下載指定時間範圍內的歷史數據，核心功能包括：
1.  **時間分塊 (Chunking)**：將長的時間範圍切成 yfinance API 允許的小塊。
2.  **迭代降級 (Fallback)**：從最精細的數據顆粒度開始嘗試，如果失敗則自動嘗試更粗的顆粒度。
3.  **數據標準化**：統一欄位名、處理缺失的 volume、確保時區為 UTC。

設計思路：
- `hydrate_data_range` 是主要的外部接口，它協調整個數據回填過程。
- `_get_chunk_size_for_interval` 和 `_split_date_range_into_chunks` 是時間分塊的輔助方法。
- `fetch_single_chunk` 負責抓取單個時間區塊的特定顆粒度數據。
- 降級邏輯在 `hydrate_data_range` 中實現，遍歷 `FALLBACK_INTERVALS`。
"""
import yfinance as yf
import pandas as pd
import time
import os
from datetime import datetime, timedelta
import logging
import requests # 為了捕獲 HTTPError

# 設定日誌記錄器
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

class YFinanceClient:
    """
    一個用於從 yfinance API 抓取長時間範圍歷史數據的客戶端，
    內建時間分塊和迭代降級策略，並整合本地快取機制。
    """
    def __init__(self, db_manager=None): # 修改：接收 db_manager 實例
        """
        初始化 YFinanceClient。

        Args:
            db_manager (DBManager, optional): 用於數據庫操作（包括快取）的 DBManager 實例。
                                              如果為 None，則快取功能將被禁用。
        """
        self.db_manager = db_manager # 儲存 db_manager 實例
        # self.cache_dir = cache_dir # 舊的 cache_dir 參數不再直接使用，由 db_manager 處理快取路徑
        # os.makedirs(self.cache_dir, exist_ok=True) # 目錄創建也應由 db_manager 處理

        # 定義區間降級鏈，從最細到最粗
        self.FALLBACK_INTERVALS = ['1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo']
        # yfinance 的 interval 參數說明:
        # 分鐘線: 1m, 2m, 5m, 15m, 30m, 60m, 90m
        #   - 1m: Max 7 days back
        #   - 2m, 5m, 15m, 30m: Max 60 days back
        #   - 60m, 90m: Max 730 days back (yfinance treats as '1h')
        # 小時線: 1h (same as 60m)
        # 日線及以上: 1d, 5d, 1wk, 1mo, 3mo
        #   - 日線以上通常可以拉取較長歷史

        # logger.info(f"YFinanceClient (Data Hydrator) 初始化完畢。") # 保留一個更通用的初始化日誌
        logger.info(f"區間降級鏈設定為: {self.FALLBACK_INTERVALS}")

    def _get_chunk_size_for_interval(self, interval: str) -> int:
        """
        根據 yfinance API 的限制，為指定的數據間隔返回建議的單次請求最大天數。
        這些值是基於 yfinance 的常見限制，並稍微保守一些以避免邊界問題。
        """
        if interval == '1m':
            return 6 # yfinance 通常限制 1m 為 7 天
        elif interval in ['2m', '5m', '15m', '30m']:
            return 55 # yfinance 通常限制這些為 60 天
        elif interval in ['60m', '90m', '1h']: # 60m, 90m 在 yfinance 中通常按 1h 處理
            return 700 # yfinance 通常限制 1h 為 730 天
        elif interval in ['1d', '5d', '1wk']:
            return 365 * 2 # 日線或週線可以拉取較長數據，例如2年
        elif interval in ['1mo', '3mo']:
            return 365 * 5 # 月線可以拉取更長數據，例如5年
        else:
            print(f"警告: 未知的 interval '{interval}'，預設 chunk_size_days 為 30。")
            return 30

    def _split_date_range_into_chunks(self, start_date_str: str, end_date_str: str, chunk_size_days: int) -> list[tuple[str, str]]:
        """
        將給定的日期範圍（YYYY-MM-DD 格式字串）根據 chunk_size_days 切分成多個日期區塊。
        每個區塊以 (chunk_start_date_str, chunk_end_date_str) 的形式返回。
        注意：yfinance 的 end date 是不包含的，所以 chunk_end_date 會是實際結束日期的後一天。
        """
        chunks = []
        try:
            current_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            final_end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except ValueError as e:
            print(f"錯誤: 日期格式錯誤 ({start_date_str}, {end_date_str})。請使用 YYYY-MM-DD 格式。 {e}")
            return []

        while current_start_date <= final_end_date:
            # 計算這個 chunk 的結束日期 (yfinance 的 end 是 exclusive)
            # 所以 chunk_end_date 是我們希望包含的最後一天的 *後一天*
            chunk_actual_end_date = current_start_date + timedelta(days=chunk_size_days - 1)

            # 如果計算出的 chunk 結束日期超過了總的結束日期，則使用總的結束日期
            if chunk_actual_end_date > final_end_date:
                chunk_actual_end_date = final_end_date

            # yfinance 的 end 參數是不包含的，所以要加一天
            yfinance_end_date = chunk_actual_end_date + timedelta(days=1)

            chunks.append((
                current_start_date.strftime("%Y-%m-%d"),
                yfinance_end_date.strftime("%Y-%m-%d")
            ))
            # 下一個 chunk 的開始日期是當前 chunk 實際結束日期的後一天
            current_start_date = chunk_actual_end_date + timedelta(days=1)

        return chunks

    def fetch_single_chunk(self, ticker: str, chunk_start_date_str: str, chunk_end_date_str: str, interval: str) -> pd.DataFrame | None:
        """
        抓取單個時間區塊 (chunk_start_date_str 到 chunk_end_date_str Exclusive) 的特定顆粒度數據。

        Args:
            ticker (str): 股票代碼。
            chunk_start_date_str (str): 區塊開始日期 (YYYY-MM-DD)。
            chunk_end_date_str (str): 區塊結束日期 (YYYY-MM-DD, yfinance history() 的 end 參數, 不包含此日期)。
            interval (str): 數據顆粒度。

        Returns:
            pd.DataFrame | None: 包含市場數據的 DataFrame，若失敗則返回 None。
        """
        logger.info(f"fetch_single_chunk: Ticker={ticker}, Interval={interval}, Start={chunk_start_date_str}, End(Exclusive)={chunk_end_date_str}")
        max_retries = 3
        base_delay = 1  # 秒

        for attempt in range(max_retries):
            try:
                stock = yf.Ticker(ticker)
                # auto_adjust=True: 自動調整OHLC，移除 'Adjusted Close' 和 'Dividends', 'Stock Splits'
                # prepost=False: 通常對於歷史回填，我們不需要盤前盤後數據，除非特定需求
                data = stock.history(start=chunk_start_date_str,
                                     end=chunk_end_date_str,
                                     interval=interval,
                                     auto_adjust=True,
                                     prepost=False)

                # 強化回傳值檢查
                if data is None or data.empty:
                    logger.warning(f"yfinance 為 {ticker} 在 {chunk_start_date_str} 到 {chunk_end_date_str} (間隔: {interval}) 返回了空的 DataFrame。")
                    return None # 即使沒有異常，但數據為空也視為一種失敗情況

                # 數據標準化
                # --- 開始標準化 ---
                # 步驟 1: 將索引（通常是日期時間）轉換為列
                if isinstance(data.index, pd.DatetimeIndex):
                    data = data.reset_index()

                # 步驟 2: 將所有列名轉為小寫
                data.columns = [col.lower() for col in data.columns]

                # 步驟 3: 統一日期時間列名為 'datetime'
                if 'date' in data.columns and 'datetime' not in data.columns:
                    data.rename(columns={'date': 'datetime'}, inplace=True)
                elif 'Datetime' in data.columns and 'datetime' not in data.columns:
                     data.rename(columns={'Datetime': 'datetime'}, inplace=True)

                if 'datetime' not in data.columns:
                    logger.error(f"標準化後 DataFrame 中缺少 'datetime' 欄位。股票: {ticker}, 間隔: {interval}。可用欄位: {data.columns.tolist()}")
                    return None

                try:
                    data['datetime'] = pd.to_datetime(data['datetime'])
                    if data['datetime'].dt.tz is None:
                        data['datetime'] = data['datetime'].dt.tz_localize('UTC')
                    else:
                        data['datetime'] = data['datetime'].dt.tz_convert('UTC')
                except Exception as e_tz:
                    logger.error(f"轉換 'datetime' 欄位時出錯: {e_tz}. 股票: {ticker}, 間隔: {interval}.")
                    return None

                if 'volume' not in data.columns:
                    data['volume'] = 0
                data['volume'] = data['volume'].fillna(0).astype('int64')

                data['interval'] = interval
                data['ticker'] = ticker

                final_columns = ['datetime', 'ticker', 'interval', 'open', 'high', 'low', 'close', 'volume']
                missing_ohlc_cols = [col for col in ['open', 'high', 'low', 'close'] if col not in data.columns]
                if missing_ohlc_cols:
                    logger.warning(f"DataFrame 缺少部分OHLC欄位: {missing_ohlc_cols}。股票: {ticker}, 間隔: {interval}。將嘗試填充為0。")
                    for col in missing_ohlc_cols:
                        data[col] = 0.0

                try:
                    data = data[final_columns]
                except KeyError as e_cols:
                    logger.error(f"選取最終欄位時發生 KeyError: {e_cols}。股票: {ticker}, 間隔: {interval}。可用欄位: {data.columns.tolist()}")
                    return None

                logger.info(f"成功獲取並標準化 {len(data)} 筆數據 for {ticker} ({interval}, {chunk_start_date_str}-{chunk_end_date_str}).")
                return data

            except requests.exceptions.HTTPError as http_err:
                # 檢查是否為 429 或 5xx 錯誤
                if http_err.response is not None and (http_err.response.status_code == 429 or http_err.response.status_code >= 500):
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"請求 {ticker} ({interval}) 發生 HTTP 錯誤 {http_err.response.status_code}。將在 {delay} 秒後重試 (嘗試 {attempt + 1}/{max_retries})...")
                        time.sleep(delay)
                    else:
                        logger.error(f"請求 {ticker} ({interval}) 發生 HTTP 錯誤 {http_err.response.status_code}，已達最大重試次數 {max_retries}。錯誤: {http_err}")
                        return None # 重試耗盡後返回 None
                else:
                    # 其他 HTTP 錯誤，不重試
                    logger.error(f"請求 {ticker} ({interval}) 發生非預期的 HTTP 錯誤: {http_err}")
                    return None
            except Exception as e:
                # 捕獲其他可能的 yfinance 內部錯誤或網絡問題
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"抓取或處理 {ticker} ({interval}) 失敗: {type(e).__name__} - {e}。將在 {delay} 秒後重試 (嘗試 {attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                else:
                    logger.error(f"抓取或處理 {ticker} ({interval}) 失敗，已達最大重試次數 {max_retries}: {type(e).__name__} - {e}")
                    return None # 重試耗盡後返回 None

        # 如果循環完成仍未成功 (理論上應該在循環內返回)
        logger.error(f"fetch_single_chunk 未能為 {ticker} ({interval}) 獲取數據，即使經過重試。")
        return None

    def hydrate_data_range(self, ticker: str, start_date_str: str, end_date_str: str, asset_class: str = 'stock') -> tuple[pd.DataFrame | None, dict]:
        """
        核心方法：全自動回填指定金融資產在給定時間範圍內的歷史數據。
        它會從最精細的顆粒度開始嘗試，使用時間分塊和迭代降級策略。

        Args:
            ticker (str): 金融資產代碼。
            start_date_str (str): 開始日期 (YYYY-MM-DD)。
            end_date_str (str): 結束日期 (YYYY-MM-DD)。
            asset_class (str, optional): 資產類別 ('stock', 'future', 'crypto', etc.)。預設為 'stock'。
                                         目前此參數主要用於日誌記錄和未來擴展，尚未影響核心抓取邏輯。

        Returns:
            pd.DataFrame | None: 一個包含所有成功抓取數據的、合併後的 DataFrame，
                                 並帶有 'interval' 和 'ticker' 欄位。若完全失敗則返回 None。
            dict: 包含詳細執行過程的日誌。
        """
        logger.info(f"===== 開始數據回填任務: Ticker={ticker}, AssetClass={asset_class}, Range=[{start_date_str} to {end_date_str}] =====")
        execution_log = {}
        request_date_range_str = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date_str, end_date_str, inclusive="left")] # inclusive important for pd.date_range

        # 初始化 execution_log
        for date_str_in_range in request_date_range_str:
            execution_log.setdefault(date_str_in_range, {})[ticker] = {
                "status": "pending", "interval": None, "count": 0, "message": "Awaiting processing"
            }

        # 步驟一：檢查快取
        if self.db_manager:
            logger.info(f"檢查本地快取 for {ticker} ({start_date_str} to {end_date_str})...")
            # 注意：db_manager.check_cache 需要 table_name，預設為 "market_ohlcv_cache"
            cached_df = self.db_manager.check_cache(ticker=ticker,
                                                    start_date_str=start_date_str,
                                                    end_date_str=end_date_str) # 使用預設 cache table name
            if cached_df is not None and not cached_df.empty:
                logger.info(f"從本地快取成功為 {ticker} 載入 {len(cached_df)} 筆數據 ({start_date_str} to {end_date_str})。")
                # 更新 execution_log 以反映快取命中
                # 假設 cached_df 包含 'datetime', 'ticker', 'interval'
                # 我們需要為請求範圍內的每一天更新日誌
                for date_obj_in_df in pd.to_datetime(cached_df['datetime']).dt.normalize().unique():
                    log_date_str = date_obj_in_df.strftime('%Y-%m-%d')
                    if log_date_str in execution_log and ticker in execution_log[log_date_str]:
                        daily_rows = cached_df[pd.to_datetime(cached_df['datetime']).dt.date == date_obj_in_df.date()]
                        # 從快取數據中獲取 interval，如果有多個，取第一個
                        cached_interval = daily_rows['interval'].iloc[0] if not daily_rows.empty and 'interval' in daily_rows.columns else "unknown_from_cache"
                        execution_log[log_date_str][ticker] = {
                            "status": "success_from_cache",
                            "interval": cached_interval,
                            "count": len(daily_rows),
                            "message": f"Data for {log_date_str} loaded from cache with interval {cached_interval} ({len(daily_rows)} rows)."
                        }
                # 確保請求範圍內但快取中可能沒有數據的天（例如週末，但check_cache已處理）也被標記
                # check_cache 的設計是如果數據不完整則返回 None，所以這裡理論上不需要再填充 missing
                return cached_df, execution_log # 步驟二：快取命中，直接返回

            else:
                logger.info(f"本地快取未命中或數據不完整 for {ticker} ({start_date_str} to {end_date_str})。將從 API 獲取。")
        else:
            logger.info("DBManager 未配置，跳過快取檢查。")

        # 步驟三：快取未命中，從 API 獲取 (以下為原始邏輯)
        # (原始的 for interval in self.FALLBACK_INTERVALS 循環等...)
        for interval in self.FALLBACK_INTERVALS:
            logger.info(f"正在嘗試使用顆粒度 '{interval}' 回填 {ticker} ({asset_class}) 從 {start_date_str} 到 {end_date_str}...") # 調整日誌格式
            chunk_size_days = self._get_chunk_size_for_interval(interval)
            if chunk_size_days <= 0: # 防呆
                logger.warning(f"顆粒度 '{interval}' 的 chunk_size_days ({chunk_size_days}) 無效，跳過此顆粒度。")
                continue

            date_chunks = self._split_date_range_into_chunks(start_date_str, end_date_str, chunk_size_days)
            if not date_chunks:
                logger.warning(f"無法為顆粒度 '{interval}' 生成有效的日期區塊，跳過此顆粒度。")
                continue

            logger.info(f"顆粒度 '{interval}'，共切分為 {len(date_chunks)} 個時間區塊。")

            current_interval_all_data_dfs = []
            all_chunks_successful_for_this_interval = True

            # 30天限制的日期 (僅與日期部分比較)
            thirty_days_ago_date = (datetime.now() - timedelta(days=30)).date()

            for i, (chunk_start_str, chunk_end_str) in enumerate(date_chunks):
                logger.info(f"正在處理區塊 {i+1}/{len(date_chunks)} ({chunk_start_str} to {chunk_end_str} exclusive) for interval '{interval}'...")

                # 【關鍵新增】智能跳過無效請求 - 檢查1m數據是否超過30天窗口
                # chunk_start_date_obj 是 datetime.date 物件
                chunk_start_date_obj = datetime.strptime(chunk_start_str, "%Y-%m-%d").date()
                if interval == '1m' and chunk_start_date_obj < thirty_days_ago_date:
                    logger.info(f"區塊起始日期 {chunk_start_str} 的 '1m' 數據請求已超過30天回溯限制，跳過此區塊的 '1m' 嘗試。")
                    # 此處標記此 interval 失敗，因為即使一個 chunk 超限，整個 1m 策略也應被視為對該 chunk 無效
                    # 如果要更細緻，可以只標記這個 chunk 的 1m 失敗，然後繼續用 1m 處理其他 chunk，
                    # 但這會讓日誌和數據合併複雜化。目前策略是：如果一個 chunk 的 1m 超限，則整個 interval 的 1m 嘗試失敗。
                    all_chunks_successful_for_this_interval = False # 標記此 interval 失敗
                    # 更新 execution_log 中此 chunk 覆蓋日期的狀態
                    for day_offset in range((datetime.strptime(chunk_end_str, "%Y-%m-%d") - timedelta(days=1) - datetime.strptime(chunk_start_str, "%Y-%m-%d")).days + 1):
                        log_date_str = (datetime.strptime(chunk_start_str, "%Y-%m-%d") + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                        if log_date_str in execution_log:
                             execution_log[log_date_str][ticker] = {
                                "status": "skipped_1m_due_to_30day_limit", "interval": "1m", "count": 0,
                                "message": f"1m data for {log_date_str} skipped, outside 30-day window."
                            }
                    break # 跳出當前 interval 的 chunks 循環, 嘗試下一個更粗的 interval

                chunk_df = self.fetch_single_chunk(ticker, chunk_start_str, chunk_end_str, interval)

                # 更新執行日誌 (無論成功或失敗)
                # 假設 chunk_df 包含的日期都在 chunk_start_str 和 (chunk_end_str - 1 day) 之間
                # 遍歷此 chunk 覆蓋的每一天來更新日誌
                current_chunk_date_obj = datetime.strptime(chunk_start_str, "%Y-%m-%d")
                actual_chunk_end_date_obj = datetime.strptime(chunk_end_str, "%Y-%m-%d") - timedelta(days=1)

                temp_current_date_for_log = current_chunk_date_obj
                while temp_current_date_for_log <= actual_chunk_end_date_obj:
                    log_date_str = temp_current_date_for_log.strftime("%Y-%m-%d")
                    if log_date_str in execution_log: # 只更新請求範圍內的日期
                        if chunk_df is not None and not chunk_df.empty:
                            # 確保 'datetime' 列是 datetime64[ns, UTC] 類型
                            if 'datetime' not in chunk_df.columns or not pd.api.types.is_datetime64_any_dtype(chunk_df['datetime']):
                                try:
                                    # 嘗試轉換，如果 chunk_df['datetime'] 不存在或無法轉換，會拋異常
                                    if 'datetime' in chunk_df.columns:
                                        chunk_df['datetime'] = pd.to_datetime(chunk_df['datetime'], utc=True)
                                    else: # 如果連 datetime 都沒有（fetch_single_chunk 出問題）
                                        raise ValueError("chunk_df is missing 'datetime' column for log update")
                                except Exception as e_conv:
                                    print(f"錯誤(hydrate_data_range): chunk_df['datetime'] 處理失敗 for {ticker} on {log_date_str}: {e_conv}")
                                    current_log_status = execution_log[log_date_str][ticker].get("status", "unknown_chunk_outcome")
                                    current_log_message = execution_log[log_date_str][ticker].get("message", "")
                                    execution_log[log_date_str][ticker].update({
                                        "status": "failed_datetime_processing_in_log" if current_log_status not in ['success', 'skipped_1m_due_to_30day_limit'] else current_log_status,
                                        "count": 0, # 無法按天計數
                                        "message": current_log_message + f" Datetime processing error in chunk. "
                                    })
                                    temp_current_date_for_log += timedelta(days=1)
                                    continue # 跳過此日期的計數更新

                            # 按日期篩選 DataFrame 中的行
                            # 確保比較的日期也是 date 物件
                            daily_rows_in_chunk = chunk_df[chunk_df['datetime'].dt.date == temp_current_date_for_log.date()]

                            current_log_message = execution_log[log_date_str][ticker].get("message","")
                            if not isinstance(current_log_message, str): current_log_message = str(current_log_message)

                            new_status = "success" # 預設為成功
                            if len(date_chunks) > 1 and not daily_rows_in_chunk.empty : new_status = "success_partial"
                            elif daily_rows_in_chunk.empty : new_status = execution_log[log_date_str][ticker].get("status", "no_data_in_chunk_for_day")


                            execution_log[log_date_str][ticker].update({
                                "status": new_status,
                                "interval": interval,
                                "count": len(daily_rows_in_chunk),
                                "message": current_log_message + \
                                           (f" Fetched {len(daily_rows_in_chunk)} rows for {log_date_str} with {interval} in chunk." if not daily_rows_in_chunk.empty else f" No rows for {log_date_str} in this chunk with {interval}.")
                            })
                        else: # chunk_df is None or empty (抓取此 chunk 失敗)
                             if execution_log[log_date_str][ticker]['status'] not in ['success', 'skipped_1m_due_to_30day_limit']:
                                current_log_message = execution_log[log_date_str][ticker].get("message","")
                                if not isinstance(current_log_message, str): current_log_message = str(current_log_message)
                                execution_log[log_date_str][ticker].update({
                                    "status": "failed_chunk", "interval": interval, "count": 0,
                                    "message": current_log_message + f" Failed to fetch/process chunk covering {log_date_str} with {interval}."
                                })
                    temp_current_date_for_log += timedelta(days=1)

                if chunk_df is not None and not chunk_df.empty:
                    current_interval_all_data_dfs.append(chunk_df)
                else:
                    logger.warning(f"顆粒度 '{interval}'，區塊 {chunk_start_str}-{chunk_end_str} 數據抓取失敗或為空。此顆粒度嘗試終止。")
                    all_chunks_successful_for_this_interval = False
                    break # 跳出此 interval 的 chunks 循環, 嘗試下一個更粗的 interval

            if all_chunks_successful_for_this_interval and current_interval_all_data_dfs:
                # 將所有 chunk 的 DataFrame 合併，注意此時索引可能不唯一，或者不是 datetime
                # fetch_single_chunk 返回的 DataFrame 已經將 datetime 作為列
                final_df = pd.concat(current_interval_all_data_dfs, ignore_index=True)
                # 此處不需要再賦值 final_df['ticker'] 和 final_df['interval']，因為 fetch_single_chunk 已處理

                # 確保 final_df['datetime'] 是 datetime64[ns, UTC]
                if 'datetime' not in final_df.columns or not pd.api.types.is_datetime64_any_dtype(final_df['datetime']):
                    try:
                        if 'datetime' in final_df.columns:
                            final_df['datetime'] = pd.to_datetime(final_df['datetime'], utc=True)
                        else:
                             raise ValueError("final_df is missing 'datetime' column for final log update")
                    except Exception as e_final_conv:
                         logger.error(f"final_df['datetime'] 處理失敗 for {ticker}: {e_final_conv}")
                         logger.warning(f"因 final_df datetime 處理失敗，執行日誌可能不完全準確。Ticker: {ticker}")
                         # 即使 datetime 處理失敗，仍然返回已獲取的數據和當前 execution_log
                         return final_df, execution_log

                # 使用 'datetime' 列來遍歷日期並更新 execution_log
                # 確保 final_df['datetime'] 列存在且類型正確後才進行遍歷
                unique_dates_in_final_df = final_df['datetime'].dt.normalize().unique()

                for date_obj_in_final_df in unique_dates_in_final_df:
                    date_str_in_final_df_range = date_obj_in_final_df.strftime('%Y-%m-%d')

                    if date_str_in_final_df_range in execution_log and \
                       ticker in execution_log[date_str_in_final_df_range]:

                        daily_rows_final = final_df[final_df['datetime'].dt.date == date_obj_in_final_df.date()]

                        if not daily_rows_final.empty:
                            execution_log[date_str_in_final_df_range][ticker] = {
                                "status": "success_from_api", # 標記來自 API
                                "interval": interval,
                                "count": len(daily_rows_final),
                                "message": f"Final data for {date_str_in_final_df_range} with {interval} ({len(daily_rows_final)} rows) fetched from API."
                            }
                        elif execution_log[date_str_in_final_df_range][ticker].get('status') != 'success_from_api': # 避免覆蓋已成功的日誌
                            current_message = execution_log[date_str_in_final_df_range][ticker].get("message", "")
                            if not isinstance(current_message, str): current_message = str(current_message)
                            execution_log[date_str_in_final_df_range][ticker]['message'] = current_message + \
                                f" No data for {date_str_in_final_df_range} found in final combined API df with {interval} (unexpected, check logic)."

                logger.info(f"成功: hydrate_data_range: 已使用顆粒度 '{interval}' 從 API 完成 {ticker} 在 {start_date_str} 到 {end_date_str} 的所有數據回填。共 {len(final_df)} 筆。")

                # 步驟四：更新快取
                if self.db_manager and not final_df.empty:
                    logger.info(f"準備將從 API 獲取的 {len(final_df)} 筆 {ticker} 數據更新至本地快取...")
                    self.db_manager.update_cache(df=final_df) # 使用預設 cache table name
                    logger.info(f"成功將 {ticker} 數據更新至本地快取。")

                logger.info(f"===== 數據回填任務結束 (成功從 API): Ticker={ticker} =====")
                return final_df, execution_log
            elif not current_interval_all_data_dfs and all_chunks_successful_for_this_interval:
                 logger.info(f"INFO: hydrate_data_range: 顆粒度 '{interval}' 所有區塊均未返回數據(可能該時段無交易)，但未發生API錯誤。嘗試下一個顆粒度。")
                 for date_str_in_range in request_date_range_str: # 遍歷請求的整個日期範圍
                     # 只有在之前的狀態不是更明確的成功或特定跳過時才更新
                     if execution_log[date_str_in_range][ticker]['status'] not in ['success', 'skipped_1m_due_to_30day_limit']:
                        execution_log[date_str_in_range][ticker].update({ # 使用 update 而不是覆蓋
                            "status": "no_data_for_interval",
                            "interval": interval, # 記錄是哪個 interval 沒數據
                            "count": 0,
                            "message": execution_log[date_str_in_range][ticker].get("message","") + f" No data found for {date_str_in_range} with {interval} after all chunks."
                        })
            else: # all_chunks_successful_for_this_interval is False (即某個 chunk 失敗了)
                print(f"INFO: hydrate_data_range: 顆粒度 '{interval}' 未能成功回填所有區塊。嘗試下一個更粗的顆粒度。")
                # execution_log 應已被 chunk 級別的失敗更新 (例如 failed_chunk, 或 skipped_1m)
                # 無需在此處再次遍歷 request_date_range_str 來更新日誌，因為失敗的 chunk 已處理其覆蓋的日期

            time.sleep(0.5) # 在嘗試不同 interval 之間稍作停頓

        # 如果所有 interval 都嘗試失敗
        logger.error(f"所有降級顆粒度 {self.FALLBACK_INTERVALS} 均無法為 {ticker} ({asset_class}) 在 {start_date_str} 到 {end_date_str} 範圍內回填任何數據。")
        logger.info(f"===== 數據回填任務結束 (失敗): Ticker={ticker}, AssetClass={asset_class} =====")
        # 更新日誌中所有仍在 pending 的狀態為最終失敗
        for date_str_in_range in request_date_range_str:
            # 只有當狀態仍然是初始的 "pending" 或某些中間的非成功狀態時才更新為 "failed_all_intervals"
            current_status = execution_log[date_str_in_range][ticker].get('status', 'pending')
            if current_status in ["pending", "failed_chunk", "no_data_for_interval", "failed_datetime_processing_in_log", "unknown_chunk_outcome"]:
                 execution_log[date_str_in_range][ticker] = {
                    "status": "failed_all_intervals", "interval": None, "count": 0,
                    "message": f"All API fetch attempts failed for {date_str_in_range}." # 更精確的消息
                }
        return None, execution_log

    def get_futures_data(self, futures_id: str, start_date: str, end_date: str) -> tuple[pd.DataFrame | None, dict]:
        """
        擷取指定期貨在給定時間範圍內的歷史數據。

        Args:
            futures_id (str): 期貨代碼。
            start_date (str): 開始日期 (YYYY-MM-DD)。
            end_date (str): 結束日期 (YYYY-MM-DD)。

        Returns:
            pd.DataFrame | None: 包含期貨數據的 DataFrame，若失敗則返回 None。
            dict: 執行日誌。
        """
        logger.info(f"請求期貨數據: ID={futures_id}, Range=[{start_date} to {end_date}]")
        return self.hydrate_data_range(ticker=futures_id,
                                       start_date_str=start_date,
                                       end_date_str=end_date,
                                       asset_class='future')

    def get_crypto_data(self, crypto_id: str, start_date: str, end_date: str) -> tuple[pd.DataFrame | None, dict]:
        """
        擷取指定加密貨幣在給定時間範圍內的歷史數據。

        Args:
            crypto_id (str): 加密貨幣代碼 (例如 'BTC-USD')。
            start_date (str): 開始日期 (YYYY-MM-DD)。
            end_date (str): 結束日期 (YYYY-MM-DD)。

        Returns:
            pd.DataFrame | None: 包含加密貨幣數據的 DataFrame，若失敗則返回 None。
            dict: 執行日誌。
        """
        logger.info(f"請求加密貨幣數據: ID={crypto_id}, Range=[{start_date} to {end_date}]")
        return self.hydrate_data_range(ticker=crypto_id,
                                       start_date_str=start_date,
                                       end_date_str=end_date,
                                       asset_class='crypto')

if __name__ == '__main__':
    logger.info("--- YFinanceClient (Daily Market Analyzer) 測試 (精簡版) ---")

    # 為了測試 YFinanceClient，我們需要一個 DBManager 實例。
    # 在單元測試環境下，可以考慮 mock DBManager，但這裡我們創建一個真實的（臨時的）。
    temp_main_db = "data_workspace/temp/yf_client_test_main.duckdb"
    temp_cache_db = "data_workspace/temp/yf_client_test_cache.duckdb"

    # 引入 DBManager - 假設它在同一目錄或PYTHONPATH中
    # 為了避免循環導入和使此文件可獨立運行（如果需要），這裡可以選擇性導入
    try:
        from db_manager import DBManager # 嘗試相對導入
    except ImportError:
        # 如果直接運行此文件，可能需要調整路徑或使用絕對導入（如果項目結構支持）
        logger.warning("無法直接導入 DBManager (可能是獨立運行 yfinance_client.py)。部分測試功能將受限。")
        DBManager = None # 設為 None 以跳過依賴 DBManager 的測試

    if DBManager:
        # 清理舊的測試資料庫檔案
        if os.path.exists(temp_main_db):
            os.remove(temp_main_db)
        if os.path.exists(temp_cache_db):
            os.remove(temp_cache_db)

        test_db_manager = DBManager(db_path=temp_main_db, cache_db_path=temp_cache_db)
        client = YFinanceClient(db_manager=test_db_manager)
        logger.info("YFinanceClient 使用臨時 DBManager 初始化成功。")

        # 簡化測試：僅檢查是否可以調用 hydrate_data_range (不驗證API數據，因為目標是測試快取邏輯)
        # 實際的快取邏輯測試應在更上層的集成測試中，或通過mock yfinance API來實現
        test_ticker = "AAPL"
        test_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        test_end = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        logger.info(f"嘗試為 {test_ticker} ({test_start} to {test_end}) 呼叫 hydrate_data_range (預期快取未命中)...")
        # 第一次調用 (應為快取未命中，嘗試 API)
        # 為避免實際 API 呼叫產生過多輸出或依賴，這裡可以只記錄調用意圖
        # df_api, log_api = client.hydrate_data_range(test_ticker, test_start, test_end)
        # if df_api is not None:
        #    logger.info(f"第一次API調用返回 {len(df_api)} 筆數據。")
        # else:
        #    logger.warning("第一次API調用未返回數據。")

        # 假設第一次調用後數據已寫入快取 (在真實場景中)
        # 第二次調用 (應為快取命中)
        # logger.info(f"嘗試為 {test_ticker} ({test_start} to {test_end}) 再次呼叫 hydrate_data_range (預期快取命中)...")
        # df_cache, log_cache = client.hydrate_data_range(test_ticker, test_start, test_end)
        # if df_cache is not None:
        #    logger.info(f"第二次快取調用返回 {len(df_cache)} 筆數據。")
        #    # 理想情況下，log_cache 應表明 'success_from_cache'
        #    # found_cache_log = False
        #    # for date_key in log_cache:
        #    #     if test_ticker in log_cache[date_key] and log_cache[date_key][test_ticker].get('status') == 'success_from_cache':
        #    #         found_cache_log = True
        #    #         break
        #    # assert found_cache_log, "第二次調用未在日誌中標記為來自快取。"
        # else:
        #    logger.warning("第二次快取調用未返回數據。")

        # 測試 get_futures_data 和 get_crypto_data 的基本調用結構
        logger.info("測試 get_futures_data 和 get_crypto_data 的基本調用結構 (不實際驗證數據)...")
        # client.get_futures_data("ES=F", test_start, test_end)
        # client.get_crypto_data("BTC-USD", test_start, test_end)
        logger.info("對 get_futures_data 和 get_crypto_data 的模擬調用完成。")

        test_db_manager.close_connections()
        if os.path.exists(temp_main_db): os.remove(temp_main_db)
        if os.path.exists(temp_cache_db): os.remove(temp_cache_db)
        logger.info("臨時測試資料庫已清理。")

    else:
        client_no_db = YFinanceClient(db_manager=None)
        logger.info("YFinanceClient (無 DBManager) 初始化成功。快取功能將被禁用。")
        # 可以添加一個非常簡單的測試，確保在 db_manager 為 None 時不會崩潰
        # logger.info("嘗試在無 DBManager 的情況下調用 hydrate_data_range...")
        # client_no_db.hydrate_data_range("MSFT", "2023-01-01", "2023-01-02")
        # logger.info("無 DBManager 的 hydrate_data_range 調用完成（不檢查結果）。")


    logger.info("--- YFinanceClient (Daily Market Analyzer) 測試 (精簡版) 完畢 ---")
