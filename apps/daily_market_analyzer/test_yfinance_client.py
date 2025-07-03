# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from datetime import datetime, timedelta # Keep standard imports for test logic

# Path adjustments
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.daily_market_analyzer.yfinance_client import YFinanceClient, _convert_missing_dates_to_ranges
from apps.daily_market_analyzer.db_manager import DBManager

def create_sample_df(dates: list[str], ticker="TEST", interval="1d", data_prefix=100) -> pd.DataFrame:
    if not dates: return pd.DataFrame()
    dts = pd.to_datetime(dates, utc=True)
    data = {'datetime': dts, 'ticker': [ticker]*len(dts), 'interval': [interval]*len(dts),
            'open': [data_prefix+i for i in range(len(dts))], 'high': [data_prefix+i+1 for i in range(len(dts))],
            'low': [data_prefix+i-1 for i in range(len(dts))], 'close': [data_prefix+i+0.5 for i in range(len(dts))],
            'volume': [1000+i*100 for i in range(len(dts))]}
    return pd.DataFrame(data)

class TestYFinanceClient(unittest.TestCase):
    def setUp(self):
        self.mock_db_manager = MagicMock(spec=DBManager)
        self.client = YFinanceClient(db_manager=self.mock_db_manager, cache_dir="temp_test_cache")
        self.ticker = "TEST_TICKER"
        self.table_name = "test_ohlcv"

    def test_convert_missing_dates_to_ranges_empty(self):
        self.assertEqual(_convert_missing_dates_to_ranges([]), [])
    def test_convert_missing_dates_to_ranges_single_date(self):
        self.assertEqual(_convert_missing_dates_to_ranges(["2023-01-01"]), [("2023-01-01", "2023-01-01")])
    def test_convert_missing_dates_to_ranges_continuous(self):
        self.assertEqual(_convert_missing_dates_to_ranges(["2023-01-01", "2023-01-02", "2023-01-03"]), [("2023-01-01", "2023-01-03")])
    def test_convert_missing_dates_to_ranges_scattered(self):
        self.assertEqual(_convert_missing_dates_to_ranges(["2023-01-01", "2023-01-03", "2023-01-04", "2023-01-06"]),
                         [("2023-01-01", "2023-01-01"), ("2023-01-03", "2023-01-04"), ("2023-01-06", "2023-01-06")])
    def test_convert_missing_dates_to_ranges_with_duplicates_and_unsorted(self):
        self.assertEqual(_convert_missing_dates_to_ranges(["2023-01-03", "2023-01-01", "2023-01-03", "2023-01-02"]),
                         [("2023-01-01", "2023-01-03")])

    @patch('apps.daily_market_analyzer.yfinance_client.YFinanceClient.fetch_single_chunk')
    def test_hydrate_data_range_full_cache_hit(self, mock_fetch_single_chunk):
        start_date_str = "2023-01-01"; end_date_str = "2023-01-02"
        hit_interval = self.client.FALLBACK_INTERVALS[0]
        sample_cached_data = create_sample_df([start_date_str, end_date_str], interval=hit_interval)
        self.mock_db_manager.check_cache.return_value = (sample_cached_data.copy(), [])
        result_df, exec_log = self.client.hydrate_data_range(self.ticker, start_date_str, end_date_str, db_table_name=self.table_name)
        self.mock_db_manager.check_cache.assert_called_once_with(
            ticker=self.ticker, start_date_str=start_date_str, end_date_str=end_date_str,
            interval=hit_interval, table_name=self.table_name)
        mock_fetch_single_chunk.assert_not_called()
        self.mock_db_manager.upsert_data.assert_not_called()
        pd.testing.assert_frame_equal(result_df, sample_cached_data, check_dtype=False)
        self.assertEqual(exec_log[start_date_str][self.ticker]['status'], "cached_full_hit_verified")
        self.assertEqual(exec_log[end_date_str][self.ticker]['status'], "cached_full_hit_verified")
        self.assertEqual(exec_log[start_date_str][self.ticker]['interval'], hit_interval)

    @patch('apps.daily_market_analyzer.yfinance_client.YFinanceClient.fetch_single_chunk')
    @patch('apps.daily_market_analyzer.yfinance_client.datetime')
    def test_hydrate_data_range_full_miss_fetch_success(self, mock_dt_module, mock_fetch_single_chunk):
        from datetime import datetime as real_datetime, timedelta as real_timedelta, date as real_date
        mock_dt_module.now.return_value = real_datetime(2023, 3, 1)
        mock_dt_module.strptime = real_datetime.strptime
        mock_dt_module.timedelta = real_timedelta
        mock_dt_module.date = real_date
        mock_dt_module.datetime = real_datetime # Ensure the datetime class itself is the real one

        start_date_str = "2023-01-01"; end_date_str = "2023-01-02"
        skipped_interval = self.client.FALLBACK_INTERVALS[0]
        fetch_interval = self.client.FALLBACK_INTERVALS[1]
        self.mock_db_manager.check_cache.side_effect = [
            (pd.DataFrame(), [start_date_str, end_date_str]),
            (pd.DataFrame(), [start_date_str, end_date_str])]
        api_data = create_sample_df([start_date_str, end_date_str], interval=fetch_interval, data_prefix=200)
        mock_fetch_single_chunk.return_value = api_data.copy()
        result_df, exec_log = self.client.hydrate_data_range(self.ticker, start_date_str, end_date_str, db_table_name=self.table_name)

        self.assertEqual(self.mock_db_manager.check_cache.call_count, 2)
        self.mock_db_manager.check_cache.assert_any_call(ticker=self.ticker, start_date_str=start_date_str, end_date_str=end_date_str, interval=skipped_interval, table_name=self.table_name)
        self.mock_db_manager.check_cache.assert_any_call(ticker=self.ticker, start_date_str=start_date_str, end_date_str=end_date_str, interval=fetch_interval, table_name=self.table_name)

        expected_yfinance_end = (real_datetime.strptime(end_date_str, "%Y-%m-%d") + real_timedelta(days=1)).strftime("%Y-%m-%d")
        mock_fetch_single_chunk.assert_called_once_with(self.ticker, start_date_str, expected_yfinance_end, fetch_interval)
        self.mock_db_manager.upsert_data.assert_called_once()
        pd.testing.assert_frame_equal(result_df, api_data, check_dtype=False)
        self.assertEqual(exec_log[start_date_str][self.ticker]['status'], "success")
        self.assertEqual(exec_log[start_date_str][self.ticker]['interval'], fetch_interval)
        self.assertTrue(int(exec_log[start_date_str][self.ticker]['count']) > 0)

    @patch('apps.daily_market_analyzer.yfinance_client.YFinanceClient.fetch_single_chunk')
    @patch('apps.daily_market_analyzer.yfinance_client.datetime')
    def test_hydrate_data_range_partial_hit_fetch_success(self, mock_dt_module, mock_fetch_single_chunk):
        from datetime import datetime as real_datetime, timedelta as real_timedelta, date as real_date
        mock_dt_module.now.return_value = real_datetime(2023, 3, 1)
        mock_dt_module.strptime = real_datetime.strptime
        mock_dt_module.timedelta = real_timedelta
        mock_dt_module.date = real_date
        mock_dt_module.datetime = real_datetime

        start_date_str="2023-01-01"; mid_date_str="2023-01-02"; end_date_str="2023-01-03"
        skipped_interval = self.client.FALLBACK_INTERVALS[0]
        fetch_interval = self.client.FALLBACK_INTERVALS[1]

        cached_day1_1m = create_sample_df([start_date_str], interval=skipped_interval, data_prefix=100)
        cached_day1_5m = create_sample_df([start_date_str], interval=fetch_interval, data_prefix=150)
        self.mock_db_manager.check_cache.side_effect = [
             (cached_day1_1m.copy(), [mid_date_str, end_date_str]),
             (cached_day1_5m.copy(), [mid_date_str, end_date_str])]

        api_missing_data = create_sample_df([mid_date_str, end_date_str], interval=fetch_interval, data_prefix=200)
        mock_fetch_single_chunk.return_value = api_missing_data.copy()
        result_df, exec_log = self.client.hydrate_data_range(self.ticker, start_date_str, end_date_str, db_table_name=self.table_name)

        self.assertEqual(self.mock_db_manager.check_cache.call_count, 2)
        expected_yfinance_end = (real_datetime.strptime(end_date_str, "%Y-%m-%d") + real_timedelta(days=1)).strftime("%Y-%m-%d")
        mock_fetch_single_chunk.assert_called_once_with(self.ticker, mid_date_str, expected_yfinance_end, fetch_interval)
        self.mock_db_manager.upsert_data.assert_called_once()

        expected_df = pd.concat([cached_day1_5m, api_missing_data], ignore_index=True).sort_values(by=['datetime']).reset_index(drop=True)
        result_df = result_df.sort_values(by=['datetime']).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
        self.assertEqual(exec_log[start_date_str][self.ticker]['interval'], fetch_interval)
        self.assertEqual(exec_log[mid_date_str][self.ticker]['status'], "success")

    @patch('apps.daily_market_analyzer.yfinance_client.YFinanceClient.fetch_single_chunk')
    @patch('apps.daily_market_analyzer.yfinance_client.datetime')
    def test_hydrate_data_range_partial_miss_api_fails_fallback_succeeds(self, mock_dt_module, mock_fetch_single_chunk):
        from datetime import datetime as real_datetime, timedelta as real_timedelta, date as real_date
        mock_dt_module.now.return_value = real_datetime(2023, 3, 1)
        mock_dt_module.strptime = real_datetime.strptime
        mock_dt_module.timedelta = real_timedelta
        mock_dt_module.date = real_date
        mock_dt_module.datetime = real_datetime

        start_date_str = "2023-01-01"; end_date_str = "2023-01-01"
        int_1m = self.client.FALLBACK_INTERVALS[0]
        int_5m = self.client.FALLBACK_INTERVALS[1]
        int_15m = self.client.FALLBACK_INTERVALS[2]
        self.mock_db_manager.check_cache.side_effect = [
            (pd.DataFrame(), [start_date_str]),(pd.DataFrame(), [start_date_str]),(pd.DataFrame(), [start_date_str])]

        api_data_15m = create_sample_df([start_date_str], interval=int_15m, data_prefix=300)
        mock_fetch_single_chunk.side_effect = [None, api_data_15m.copy()]
        result_df, exec_log = self.client.hydrate_data_range(self.ticker, start_date_str, end_date_str, db_table_name=self.table_name)

        self.assertEqual(self.mock_db_manager.check_cache.call_count, 3)
        yfinance_exclusive_end = (real_datetime.strptime(end_date_str, "%Y-%m-%d") + real_timedelta(days=1)).strftime("%Y-%m-%d")
        expected_fetch_calls = [
            call(self.ticker, start_date_str, yfinance_exclusive_end, int_5m),
            call(self.ticker, start_date_str, yfinance_exclusive_end, int_15m)]
        mock_fetch_single_chunk.assert_has_calls(expected_fetch_calls)
        self.assertEqual(mock_fetch_single_chunk.call_count, 2)
        self.mock_db_manager.upsert_data.assert_called_once()
        pd.testing.assert_frame_equal(result_df, api_data_15m, check_dtype=False)
        self.assertEqual(exec_log[start_date_str][self.ticker]['status'], "success")
        self.assertEqual(exec_log[start_date_str][self.ticker]['interval'], int_15m)
        self.assertTrue(int(exec_log[start_date_str][self.ticker]['count']) > 0)

if __name__ == '__main__':
    unittest.main()
