# 版本: v1.0 (Genesis Blitz)
# 職責: TAIFEX數據閃電偵察兵。僅獲取少量樣本，調用 pipeline 的配方判斷函數。
import requests
import time
import os
import random
import argparse
import json
import hashlib
import duckdb
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 從相鄰模組導入 determine_parsing_recipe (如果結構允許)
# 假設 prospector, pipeline, downloader 在 project_genesis_sandbox 下同級
import sys
# 為了能正確導入 downloader 和 pipeline 中的函數/變數，需要將 project_genesis_sandbox 加入 sys.path
# 這假設此腳本是從 project_genesis_sandbox 目錄外部執行的，或者PYTHONPATH已設定
# 或者，更穩健的方式是將共享函數放到一個 common utils 模組
try:
    # 試圖找到 downloader 和 pipeline 的路徑
    # 這裡的路徑是相對於 project_genesis_sandbox
    # 如果此腳本在 project_genesis_sandbox/taifex_data_prospector/ 中執行
    current_script_path = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_path) # project_genesis_sandbox

    downloader_path = os.path.join(project_root, "taifex_data_downloader")
    pipeline_path = os.path.join(project_root, "taifex_data_pipeline")

    if downloader_path not in sys.path:
        sys.path.insert(0, downloader_path)
    if pipeline_path not in sys.path:
        sys.path.insert(0, pipeline_path)
    if project_root not in sys.path: # 確保 project_genesis_sandbox 在路徑中
        sys.path.insert(0, project_root)

    from taifex_data_downloader.run import USER_AGENTS, BASE_URL # 共享的變數
    from taifex_data_downloader.run import execute_download_task as fetch_sample_content # 借用其下載邏輯獲取樣本
                                                                                        # 注意：我們需要修改它以只獲取樣本
    from taifex_data_pipeline.run import determine_parsing_recipe, calculate_sha256
    from taifex_data_pipeline.run import log_info as pipeline_log_info # 避免與本地日誌衝突
    IMPORTED_SUCCESSFULLY = True
except ImportError as e:
    print(f"[ERROR] [PROSPECTOR] 導入相依模組失敗: {e}")
    print(f"[ERROR] [PROSPECTOR] 請確保 taifex_data_downloader 和 taifex_data_pipeline 模組與此模組在同一 project_genesis_sandbox 目錄下，且結構正確。")
    IMPORTED_SUCCESSFULLY = False
    # 定義一些預設值或函數，以使腳本在導入失敗時至少可以解析參數
    USER_AGENTS = ['Mozilla/5.0']
    BASE_URL = "https://www.taifex.com.tw"
    def determine_parsing_recipe(content_bytes, filename_hint=""): return None
    def calculate_sha256(content_bytes): return ""
    def pipeline_log_info(msg,component="PIPELINE_STUB"): print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{component}] [INFO] {msg}")


# --- 日誌記錄函數 ---
PROSPECTOR_DB_PATH = "./workspace/prospector_cache.duckdb" # 偵察兵的快取

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def log_info(message, component="PROSPECTOR"):
    print(f"[{get_timestamp()}] [{component}] [INFO] {message}")

def log_success(message, component="PROSPECTOR"):
    print(f"[{get_timestamp()}] [{component}] [SUCCESS] ✅ {message}")

def log_warning(message, component="PROSPECTOR"):
    print(f"[{get_timestamp()}] [{component}] [WARNING] ⚠️ {message}")

def log_error(message, component="PROSPECTOR"):
    print(f"[{get_timestamp()}] [{component}] [ERROR] ❌ {message}")
# --- 日誌記錄函數結束 ---

class ProspectorDBManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.component = "PROSPECTOR_DB"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            self._initialize_schema()
            log_info(f"成功連接到偵察快取資料庫: {self.db_path}", self.component)
        except Exception as e:
            log_error(f"連接或初始化偵察快取資料庫失敗: {e}", self.component)
            self.conn = None

    def _initialize_schema(self):
        if not self.conn: return
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prospected_urls (
                url_hash VARCHAR PRIMARY KEY, -- URL的哈希值
                url VARCHAR,
                filename_hint VARCHAR,
                sample_sha256 VARCHAR, -- 樣本內容的哈希值
                parsing_recipe_json TEXT, -- JSON格式的解析配方
                status VARCHAR, -- e.g., SUCCESS, NOT_FOUND, SAMPLE_TOO_SMALL, RECIPE_FAILED
                error_message TEXT,
                last_prospected_at TIMESTAMP DEFAULT current_timestamp,
                content_length INTEGER, -- 實際內容長度 (如果可得)
                http_status_code INTEGER
            );
        """)
        log_info("偵察快取 Schema 初始化/驗證完畢。", self.component)

    def get_prospected_info(self, url):
        if not self.conn: return None
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest() # 用MD5作為URL的鍵，因SHA256可能太長
        try:
            result = self.conn.execute("SELECT parsing_recipe_json, status, sample_sha256, last_prospected_at FROM prospected_urls WHERE url_hash = ?", [url_hash]).fetchone()
            if result:
                return {"recipe_json": result[0], "status": result[1], "sample_sha256": result[2], "timestamp": result[3]}
            return None
        except Exception as e:
            log_error(f"查詢偵察快取失敗 for URL hash {url_hash}: {e}", self.component)
            return None

    def store_prospected_info(self, url, filename_hint, sample_sha256, recipe, status, error_msg=None, content_length=None, http_status_code=None):
        if not self.conn: return
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        recipe_json = json.dumps(recipe) if recipe else None
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO prospected_urls
                (url_hash, url, filename_hint, sample_sha256, parsing_recipe_json, status, error_message, content_length, http_status_code, last_prospected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [url_hash, url, filename_hint, sample_sha256, recipe_json, status, error_msg, content_length, http_status_code, datetime.now()])
            log_info(f"URL '{url[:50]}...' 的偵察結果已存入快取。狀態: {status}", self.component) # log_debug to log_info
        except Exception as e:
            log_error(f"存儲偵察結果到快取失敗 for URL {url_hash}: {e}", self.component)

    def close(self):
        if self.conn: self.conn.close()


def fetch_sample_from_url(session, url, task_type='GET', payload=None, referer=None, sample_size_kb=20):
    """獲取URL內容的前N KB作為樣本"""
    log_info(f"嘗試獲取樣本 ({sample_size_kb}KB) 從 URL: {url[:100]}...") # log_debug to log_info
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    if referer: headers['Referer'] = referer

    max_bytes = sample_size_kb * 1024
    sample_content = b""
    http_status = None
    content_length_header = None

    try:
        if task_type == 'POST':
            response = session.post(url, data=payload, headers=headers, timeout=30, stream=True)
        else:
            response = session.get(url, headers=headers, timeout=30, stream=True)

        http_status = response.status_code
        content_length_header = response.headers.get('Content-Length')

        if response.status_code == 200:
            for chunk in response.iter_content(chunk_size=1024): # 每次讀1KB
                sample_content += chunk
                if len(sample_content) >= max_bytes:
                    break
            log_info(f"成功獲取 {len(sample_content)} bytes 樣本從 {url[:70]}... (HTTP {http_status})", component="PROSPECTOR_FETCHER") # log_debug to log_info, added component
            return sample_content, http_status, content_length_header, None # content, status, content_length, error
        else:
            error_msg = f"HTTP狀態碼 {response.status_code}。"
            # 嘗試讀取少量錯誤響應內容
            try:
                error_body_sample = response.content[:512].decode(response.encoding or 'utf-8', errors='ignore')
                error_msg += f" 響應預覽: {error_body_sample}"
            except: pass
            log_warning(f"獲取樣本失敗: {url[:70]}... {error_msg}")
            return None, http_status, content_length_header, error_msg

    except requests.exceptions.RequestException as e:
        error_msg = f"網路請求錯誤: {e}"
        log_error(f"獲取樣本時發生網路錯誤: {url[:70]}... - {e}")
        return None, http_status, content_length_header, error_msg


def prospect_task(session, task_info, db_manager):
    """執行單個URL的偵察任務"""
    url = task_info['url']
    filename_hint = task_info['file_name'] # 用於輔助配方判斷
    task_type = task_info.get('type', 'GET')
    payload = task_info.get('payload')
    referer = task_info.get('referer')

    # 1. 檢查快取
    cached_info = db_manager.get_prospected_info(url)
    if cached_info:
        # 這裡可以加入邏輯：如果快取太舊，可以考慮重新偵察
        # time_since_last_prospect = datetime.now() - cached_info.get('timestamp', datetime.min)
        # if time_since_last_prospect.days < 7: # 例如，7天內的快取有效
        log_success(f"CACHE HIT: URL '{url[:70]}...' 已於 {cached_info.get('timestamp')} 偵察過。狀態: {cached_info.get('status')}")
        return cached_info.get('status')
        # else:
        # log_info(f"CACHE STALE: URL '{url[:70]}...' 快取過舊，重新偵察。")

    # 2. 獲取樣本
    sample_bytes, http_status, content_len_header, fetch_error = fetch_sample_from_url(session, url, task_type, payload, referer, sample_size_kb=20)

    if fetch_error or sample_bytes is None:
        status = "FETCH_FAILED"
        if http_status == 404: status = "NOT_FOUND"
        elif http_status and http_status != 200 : status = f"HTTP_ERROR_{http_status}"

        db_manager.store_prospected_info(url, filename_hint, None, None, status, error_msg=fetch_error, content_length=content_len_header, http_status_code=http_status)
        log_error(f"偵察失敗 (獲取樣本): {url[:70]}... Reason: {fetch_error or '未知獲取錯誤'}")
        return status

    sample_sha256 = calculate_sha256(sample_bytes)

    # 3. 判斷配方
    # 注意：determine_parsing_recipe 是從 pipeline 模組導入的
    recipe = determine_parsing_recipe(sample_bytes, filename_hint=filename_hint)

    if recipe:
        log_success(f"配方識別成功 for {filename_hint} (URL: {url[:50]}...). 配方: {json.dumps(recipe)}")
        db_manager.store_prospected_info(url, filename_hint, sample_sha256, recipe, "RECIPE_SUCCESS", content_length=content_len_header, http_status_code=http_status)
        return "RECIPE_SUCCESS"
    else:
        # 檢查是否因為樣本太小 (determine_parsing_recipe 內部應有日誌)
        # 這裡的 status 可以更細化
        log_warning(f"配方識別失敗 for {filename_hint} (URL: {url[:50]}...). 樣本SHA256: {sample_sha256[:8]}")
        db_manager.store_prospected_info(url, filename_hint, sample_sha256, None, "RECIPE_FAILED", error_msg="determine_parsing_recipe returned None", content_length=content_len_header, http_status_code=http_status)
        return "RECIPE_FAILED"


def generate_taifex_tasks(start_date_str, end_date_str, year_str):
    """
    生成TAIFEX數據的URL任務列表。
    與 taifex_data_downloader/run.py 中的任務生成邏輯保持一致。
    """
    tasks = []
    # --- 近期每日數據任務 (GET請求) ---
    try:
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')
        current_dt = start_dt
        while current_dt <= end_dt:
            date_fmt_for_url = current_dt.strftime('%Y_%m_%d') # Daily_YYYY_MM_DD.zip
            date_fmt_for_name = current_dt.strftime('%Y%m%d') # 檔名中的日期格式

            # 每日行情下載-CSV (DailydownloadCSV) - 這個是舊的，但有些歷史資料可能還用這個格式
            # tasks.append({
            #     'type': 'GET',
            #     'url': f"{BASE_URL}/file/taifex/Dailydownload/DailydownloadCSV/Daily_{date_fmt_for_url}.zip",
            #     'file_name': f"Daily_Market_Data_{date_fmt_for_name}.zip" # 通用市場數據
            # })

            # 盤後資訊 > 每日行情下載 > 大宗交易時段後每日行情下載-CSV (DailyDownloadCSV)
            # (這個似乎是最新的日資料CSV下載點 for 期貨+選擇權合併)
            # https://www.taifex.com.tw/cht/3/dailyTradingReportView --> "下載CSV檔案"按鈕
            # 連結到 /file/taifex/DailyDownload/DailyDownloadCSV/Daily_契約年月.csv (這是單一契約的)
            # 我們需要的是總表，通常是ZIP

            # 根據 downloader v2.0 的邏輯，它下載的是:
            # Daily_Futures_Tick_YYYY_MM_DD.zip from /Dailydownload/DailydownloadCSV/Daily_{date_fmt}.zip
            # Daily_Options_Tick_YYYY_MM_DD.zip from /Dailydownload/OptionsDailydownloadCSV/OptionsDaily_{date_fmt}.zip

            # 1. 期貨每日交易資料 (通常包含 TXF, EXF, FXF, GDF, XIF, TGF, ZEFin等)
            tasks.append({
                'type': 'GET',
                'url': f"{BASE_URL}/file/taifex/Dailydownload/DailydownloadCSV/Daily_{date_fmt_for_url}.zip",
                'file_name': f"Taifex_Daily_Futures_{date_fmt_for_name}.zip"
            })

            # 2. 選擇權每日交易資料 (通常包含 TXO, EXO, FXO, GDO, XIO等)
            tasks.append({
                'type': 'GET',
                'url': f"{BASE_URL}/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_{date_fmt_for_url}.zip",
                'file_name': f"Taifex_Daily_Options_{date_fmt_for_name}.zip"
            })
            current_dt += timedelta(days=1)
    except ValueError as e:
        log_error(f"解析日期字串時出錯: {e}。近期數據任務可能未完全生成。")


    # --- 久遠年度數據任務 (POST請求) ---
    if year_str:
        # 年度期貨行情 (依商品)
        tasks.append({
            'type': 'POST',
            'url': f"{BASE_URL}/cht/3/futDataDown", # 期貨類歷史資料 (依商品)
            'referer': f"{BASE_URL}/cht/3/dlFutDailyMarketView", # 模擬從此頁面發起
            'payload': {'down_type': '2', # 2=年度報表
                        'commodity_id': 'TXF', # 商品代碼，例如 TXF (臺股期貨)。若要全市場，此處可能需遍歷或有特殊值
                        'his_year': year_str,
                        'datestart': f'{year_str}-01-01', # 網站似乎需要 YYYY-MM-DD
                        'dateend': f'{year_str}-12-31'},
            'file_name': f"Taifex_Annual_Futures_TXF_{year_str}.zip" # 檔名示例
        })
        # 年度選擇權行情 (依商品)
        tasks.append({
            'type': 'POST',
            'url': f"{BASE_URL}/cht/3/optDataDown", # 選擇權類歷史資料 (依商品)
            'referer': f"{BASE_URL}/cht/3/dlOptDailyMarketView",
            'payload': {'down_type': '2',
                        'commodity_id': 'TXO', # 商品代碼，例如 TXO (臺指選擇權)
                        'his_year': year_str,
                        'datestart': f'{year_str}-01-01',
                        'dateend': f'{year_str}-12-31'},
            'file_name': f"Taifex_Annual_Options_TXO_{year_str}.zip" # 檔名示例
        })
        # 補充：期交所網站上 "依日期" 的年度下載，其POST內容可能不同
        # 例如 https://www.taifex.com.tw/cht/3/futDataDown (依日期)
        # payload: down_type=1, queryStartDate=2015/01/01, queryEndDate=2015/12/31
        # 這會下載一個 Market_Data_YYYYMMDD_YYYYMMDD.zip 的檔案
        # 為了與 downloader v2.0 的 POST 邏輯對應 (它用 down_type=2, his_year=YYYY)
        # 我們這裡也用 down_type=2。但 downloader v2.0 中的 payload 是 {'down_type': '2', 'his_year': year_str}
        # 沒有 commodity_id, queryStartDate, queryEndDate。這可能意味著 downloader v2.0 的 POST 年度下載
        # 是下載該年度所有商品的打包檔，或者其 POST endpoint /cht/3/futDataDown 和 /cht/3/optDataDown
        # 在 down_type=2 時不需要其他參數。
        # **重要**：根據 downloader v2.0 的 payload {'down_type': '2', 'his_year': year_str},
        # 我們這裡的 payload 應該簡化。
        tasks.append({
            'type': 'POST',
            'url': f"{BASE_URL}/cht/3/futDataDown", # 期貨類歷史資料 (全市場年度)
            'referer': f"{BASE_URL}/cht/3/dlFutDailyMarketView",
            'payload': {'down_type': '2', 'his_year': year_str},
            'file_name': f"Taifex_Annual_Futures_Market_{year_str}.zip"
        })
        tasks.append({
            'type': 'POST',
            'url': f"{BASE_URL}/cht/3/optDataDown", # 選擇權類歷史資料 (全市場年度)
            'referer': f"{BASE_URL}/cht/3/dlOptDailyMarketView",
            'payload': {'down_type': '2', 'his_year': year_str},
            'file_name': f"Taifex_Annual_Options_Market_{year_str}.zip"
        })


    log_info(f"已生成 {len(tasks)} 項 TAIFEX 數據偵察任務。")
    return tasks


def main(start_date_str, end_date_str, year_to_prospect, force_reprospect=False):
    if not IMPORTED_SUCCESSFULLY:
        log_error("由於相依模組未能成功導入，偵察兵無法執行。請檢查錯誤訊息並修正環境。")
        return

    log_info(f"--- TAIFEX 數據閃電偵察兵 v1.0 (創世紀閃擊戰) 啟動 ---")
    log_info(f"參數: StartDate='{start_date_str}', EndDate='{end_date_str}', Year='{year_to_prospect}', ForceReprospect={force_reprospect}")

    db_manager = ProspectorDBManager(PROSPECTOR_DB_PATH)
    if not db_manager.conn:
        log_error("偵察兵資料庫初始化失敗，無法執行。")
        return

    # 清理快取 (如果需要)
    if force_reprospect:
        log_warning("強制重新偵察已啟用，將清空現有偵察快取...")
        try:
            db_manager.conn.execute("DELETE FROM prospected_urls;")
            log_success("偵察快取已清空。")
        except Exception as e_clear:
            log_error(f"清空偵察快取失敗: {e_clear}")
            # 即使清空失敗，也繼續嘗試，但可能會使用到舊快取


    tasks_to_run = generate_taifex_tasks(start_date_str, end_date_str, str(year_to_prospect) if year_to_prospect else None)
    if not tasks_to_run:
        log_warning("沒有生成任何偵察任務。請檢查日期和年份參數。")
        db_manager.close()
        return

    log_info(f"將對 {len(tasks_to_run)} 個URL執行偵察...")

    # 使用 ThreadPoolExecutor 執行並發偵察
    # 降低並發數，避免對目標伺服器造成過大壓力
    # TAIFEX 網站可能對頻繁請求敏感
    max_workers = 1 # 再次修改為1以調試 Segfault
    successful_recipes = 0
    failed_fetch = 0
    failed_recipe = 0
    cache_hits = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with requests.Session() as session: # 共享 Session 以利用連接池
            # 將任務提交給執行緒池
            futures = [executor.submit(prospect_task, session, task, db_manager) for task in tasks_to_run]

            # 使用 tqdm 顯示進度條
            for future in tqdm(as_completed(futures), total=len(futures), desc="偵察進度"):
                try:
                    result_status = future.result() # 獲取任務結果
                    if result_status == "RECIPE_SUCCESS":
                        successful_recipes += 1
                    elif result_status == "RECIPE_FAILED":
                        failed_recipe += 1
                    elif result_status in ["FETCH_FAILED", "NOT_FOUND"] or "HTTP_ERROR" in result_status :
                        failed_fetch +=1
                    elif result_status is not None and "CACHE HIT" in result_status.upper() : # 簡易判斷是否來自快取
                        cache_hits +=1 # 這個計數方式不準確，prospect_task 返回的是 status
                                       # 需要改進 prospect_task 的返回值以區分快取命中
                except Exception as e_future:
                    log_error(f"執行偵察任務時發生異常: {e_future}")
                    failed_fetch +=1 # 將執行緒內部的異常也算作獲取失敗

    log_info("--- 偵察任務總結 ---")
    log_info(f"總任務數: {len(tasks_to_run)}")
    # log_info(f"快取命中數: {cache_hits}") # 此計數不準確，需改進
    log_info(f"成功識別配方數: {successful_recipes}")
    log_info(f"樣本獲取失敗數 (含HTTP錯誤): {failed_fetch}")
    log_info(f"配方識別失敗數: {failed_recipe}")

    # KPI 驗證點 (通用性與健壯性)
    # 通用性：對於2025年的數據URL，determine_parsing_recipe 成功從其數據樣本中識別出CSV結構並打印出「解析配方」。
    # -> 檢查 successful_recipes 的數量是否符合預期 (例如，近期數據URL都成功)
    # 健壯性：對於2015年無效或查無資料的URL，腳本必須記錄「查無資料」或「樣本無法識別」，並且絕不能崩潰。
    # -> 檢查 failed_fetch (NOT_FOUND) 的數量是否符合預期 (例如，年度數據URL部分失敗是正常的)
    # -> 並且整個過程沒有Python異常導致崩潰 (這由腳本是否能執行到這裡判斷)

    # 這裡可以加入更明確的KPI檢查邏輯，例如：
    # num_recent_tasks = sum(1 for t in tasks_to_run if str(start_date_str[:4]) in t['file_name'] or str(end_date_str[:4]) in t['file_name'])
    # num_annual_tasks = sum(1 for t in tasks_to_run if str(year_to_prospect) in t['file_name'])
    # if successful_recipes >= num_recent_tasks * 0.8: # 假設近期數據80%能成功識別
    #    log_success("KPI [通用性]: 近期數據URL的配方識別率達標。")
    # else:
    #    log_warning("KPI [通用性]: 近期數據URL的配方識別率未達標。")

    if failed_fetch > 0 and any(str(year_to_prospect) in t['file_name'] for t in tasks_to_run):
        log_info("KPI [健壯性]: 對於年度數據 (可能部分不存在)，已記錄獲取失敗或未找到，符合預期。")

    if successful_recipes + failed_fetch + failed_recipe == len(tasks_to_run) - cache_hits: # 減去快取命中 (需準確)
         log_success("KPI [完整性]: 所有非快取任務都已嘗試處理 (成功或失敗均已記錄)。")
    else:
         log_warning("KPI [完整性]: 部分任務可能未被處理或狀態未記錄。")


    db_manager.close()
    log_info("--- TAIFEX 數據閃電偵察兵任務完畢 ---")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TAIFEX 數據閃電偵察兵 (創世紀閃擊戰)")
    # 參數與「創世紀閃擊戰」計畫書中的行動 1.1 保持一致
    # 但計畫書中是從 downloader 獲取這些參數，這裡讓 prospector 自己管理
    parser.add_argument("--start_date", default="2025-06-01", help="近期數據開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end_date", default="2025-07-04", help="近期數據結束日期 (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, default=2015, help="要偵察的單一年度歷史數據 (YYYY)")
    parser.add_argument("--force_reprospect", action='store_true', help="是否強制重新偵察所有URL (清空快取)")

    args = parser.parse_args()
    main(args.start_date, args.end_date, args.year, args.force_reprospect)
