# 版本: v2.0 (Genesis)
# 職責: 具備智能請求機制的數據採集官，移植自 v27.0 核心邏輯。
import requests
import time
import os
import random
import argparse
import threading
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

USER_AGENTS = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36']
BASE_URL = "https://www.taifex.com.tw"
log_lock = threading.Lock()

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def log_info(message):
    with log_lock:
        print(f"[{get_timestamp()}] [DOWNLOADER] [INFO] {message}")

def log_success(message):
    with log_lock:
        print(f"[{get_timestamp()}] [DOWNLOADER] [SUCCESS] ✅ {message}")

def log_error(message):
    with log_lock:
        print(f"[{get_timestamp()}] [DOWNLOADER] [ERROR] ❌ {message}")

def execute_download_task(session, task_info, output_dir):
    """
    執行單一檔案下載任務的核心邏輯 (移植自 v27.0)
    能夠處理 GET/POST 請求，並應對直接下載或「查無資料」等情況。
    """
    url = task_info['url']
    file_name = task_info['file_name']
    full_path = os.path.join(output_dir, file_name)

    if os.path.exists(full_path) and os.path.getsize(full_path) > 0: # 檢查檔案是否存在且非空
        log_info(f"檔案已存在且非空，跳過: {file_name}")
        return 'exists'

    time.sleep(random.uniform(0.2, 1.0))
    for attempt in range(3): # 最多重試3次
        try:
            headers = {'User-Agent': random.choice(USER_AGENTS), 'Referer': task_info.get('referer', BASE_URL)}

            if task_info['type'] == 'POST':
                response = session.post(url, data=task_info.get('payload', {}), headers=headers, timeout=120, stream=True) # 使用 stream=True
            else:
                response = session.get(url, headers=headers, timeout=120, stream=True) # 使用 stream=True

            if response.status_code == 200:
                # 檢查 Content-Type 是否為 ZIP 或二進位流
                is_zip = 'application/zip' in response.headers.get('Content-Type', '').lower()
                is_octet_stream = 'application/octet-stream' in response.headers.get('Content-Type', '').lower()

                # 檢查是否有 Content-Disposition 且包含檔名，這通常表示是直接檔案下載
                is_direct_file = 'Content-Disposition' in response.headers

                if is_zip or is_octet_stream or is_direct_file:
                    with open(full_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192): # 分塊寫入
                            f.write(chunk)
                    # 再次檢查檔案大小，防止空檔案
                    if os.path.getsize(full_path) > 0:
                        log_success(f"下載成功: {file_name}")
                        return 'success'
                    else:
                        log_error(f"下載失敗 (檔案為空): {file_name}")
                        os.remove(full_path) # 刪除空檔案
                        return 'error_empty_file'

                # 如果不是直接的檔案類型，再檢查是否為「查無資料」的HTML頁面
                # 需要先讀取部分內容判斷，因為 response.text 會消耗整個流
                content_peek = response.raw.read(2048).decode(response.encoding or 'utf-8', errors='ignore')
                response.raw.unread(len(content_peek.encode(response.encoding or 'utf-8', errors='ignore'))) # 將讀取的內容放回流中

                if "查無資料" in content_peek or "檔案不存在" in content_peek:
                    log_error(f"下載失敗 (查無資料/檔案不存在): {file_name}")
                    return 'not_found'

                # 若以上條件都不滿足，且內容看起來不像壓縮檔 (例如HTML錯誤頁面)
                # 則也視為錯誤
                log_error(f"下載失敗 (未預期的響應內容類型 {response.headers.get('Content-Type')}): {file_name}")
                return 'error_unexpected_content'


            elif response.status_code == 404:
                log_error(f"下載失敗 (404 Not Found): {file_name}")
                return 'not_found'
            else:
                log_error(f"下載失敗 (Status: {response.status_code}): {file_name}")
                if attempt < 2:
                    log_info(f"準備重試 ({attempt+2}/3)...")
                    time.sleep(random.uniform(3,7)) # 重試前等待更長時間
                    continue
                return 'error_status_code'

        except requests.exceptions.Timeout:
            log_error(f"網路請求超時: {file_name}")
            if attempt < 2:
                log_info(f"準備重試 ({attempt+2}/3)...")
                time.sleep(random.uniform(5,10)) # 超時後等待更長時間
                continue
            return 'error_timeout'
        except requests.exceptions.RequestException as e:
            log_error(f"網路請求失敗: {file_name} - {e}")
            if attempt < 2:
                log_info(f"準備重試 ({attempt+2}/3)...")
                time.sleep(random.uniform(3,7))
                continue
            return 'error_request_exception'
    return 'error_max_retries'


def main(start_date_str, end_date_str, year, output_dir):
    log_info("--- TAIFEX 數據採集官 v2.0 (創世紀閃擊戰版) 啟動 ---")
    os.makedirs(output_dir, exist_ok=True)
    log_info(f"所有下載檔案將存放於: {output_dir}")

    tasks = []
    # --- 生成近期每日數據任務 (GET請求) ---
    # 根據「創世紀閃擊戰」計畫，此部分不再執行實際下載，但保留任務生成邏輯以供 prospector 使用
    # start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
    # end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')
    # current_dt = start_dt
    # while current_dt <= end_dt:
    #     date_fmt = current_dt.strftime('%Y_%m_%d')
    #     # 期貨逐筆
    #     tasks.append({
    #         'type': 'GET',
    #         'url': f"{BASE_URL}/file/taifex/Dailydownload/DailydownloadCSV/Daily_{date_fmt}.zip",
    #         'file_name': f"Daily_Futures_Tick_{date_fmt}.zip"
    #     })
    #     # 選擇權逐筆
    #     tasks.append({
    #         'type': 'GET',
    #         'url': f"{BASE_URL}/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_{date_fmt}.zip",
    #         'file_name': f"Daily_Options_Tick_{date_fmt}.zip"
    #     })
    #     current_dt += timedelta(days=1)

    # --- 生成久遠年度數據任務 (POST請求) ---
    # 根據「創世紀閃擊戰」計畫，此部分不再執行實際下載，但保留任務生成邏輯以供 prospector 使用
    # if year:
    #     year_str = str(year)
    #     # 年度期貨行情
    #     tasks.append({
    #         'type': 'POST',
    #         'url': f"{BASE_URL}/cht/3/futDataDown",
    #         'referer': f"{BASE_URL}/cht/3/dlFutDailyMarketView",
    #         'payload': {'down_type': '2', 'his_year': year_str, 'queryStartDate': f'{year_str}/01/01', 'queryEndDate': f'{year_str}/12/31'},
    #         'file_name': f"Annual_Futures_{year_str}.zip"
    #     })
    #     # 年度選擇權行情
    #     tasks.append({
    #         'type': 'POST',
    #         'url': f"{BASE_URL}/cht/3/optDataDown",
    #         'referer': f"{BASE_URL}/cht/3/dlOptDailyMarketView",
    #         'payload': {'down_type': '2', 'his_year': year_str, 'queryStartDate': f'{year_str}/01/01', 'queryEndDate': f'{year_str}/12/31'},
    #         'file_name': f"Annual_Options_{year_str}.zip"
    #     })

    # log_info(f"已生成 {len(tasks)} 項下載任務。")
    log_info("此模組在「創世紀閃擊戰」計畫中僅作為URL定義庫，實際下載/探勘由 `taifex_data_prospector` 執行。")
    log_info("若要獨立執行下載，請取消註解 main 函數中的任務生成和執行器部分。")

    # with ThreadPoolExecutor(max_workers=4) as executor: # 降低並發數以減輕目標伺服器壓力
    #     with requests.Session() as session:
    #         futures = [executor.submit(execute_download_task, session, task, output_dir) for task in tasks]
    #         for future in tqdm(as_completed(futures), total=len(futures), desc="數據採集進度"):
    #             future.result()

    log_info("--- 數據採集官任務定義完畢 ---")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TAIFEX 數據採集官 v2.0 (創世紀閃擊戰 URL定義庫)")
    parser.add_argument("--start_date", default="2025-06-01", help="近期數據開始日期 (YYYY-MM-DD) - 僅供 prospector 參考")
    parser.add_argument("--end_date", default="2025-07-04", help="近期數據結束日期 (YYYY-MM-DD) - 僅供 prospector 參考")
    parser.add_argument("--year", type=int, default=2015, help="要下載的單一年度歷史數據 (YYYY) - 僅供 prospector 參考")
    parser.add_argument("--output_dir", default="./input", help="下載檔案的輸出目錄 - 僅供 prospector 參考")
    args = parser.parse_args()

    # 在「創世紀閃擊戰」中，此腳本不應直接執行下載
    # main(args.start_date, args.end_date, args.year, args.output_dir)
    log_info(f"TAIFEX 數據採集官 (URL定義庫) 已載入。定義了開始日期: {args.start_date}, 結束日期: {args.end_date}, 年度: {args.year}。")
    log_info("請執行 `taifex_data_prospector` 來進行數據偵察。")
