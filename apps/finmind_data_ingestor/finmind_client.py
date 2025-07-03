import requests
import pandas as pd
import time
import json

BASE_URL = "https://api.finmindtrade.com/api/v4"

def fetch_finmind_api(endpoint: str, params: dict) -> pd.DataFrame:
    """
    一個通用的函數，用於請求 FinMind API 的特定端點。
    """
    url = f"{BASE_URL}/{endpoint}"
    print(f"📡 正在請求: {url} | 參數: {params}")
    try:
        response = requests.get(url, params=params)
        # 檢查是否觸發速率限制
        if response.status_code == 402:
            print(f"⚠️ 觸發速率限制！API 回應: {response.text}")
            return pd.DataFrame() # 返回空的 DataFrame
        # 檢查其他 HTTP 錯誤
        response.raise_for_status()

        data = response.json()
        if data.get("data"):
            print("✅ 請求成功，已獲取數據。")
            return pd.DataFrame(data["data"])
        else:
            print(f"🟡 請求成功，但未返回 `data` 欄位。API 回應: {response.text}")
            return pd.DataFrame()

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP 錯誤發生: {http_err}")
        print(f"    回應內容: {http_err.response.text if http_err.response else 'N/A'}")
    except requests.exceptions.RequestException as req_err:
        print(f"❌ 請求例外發生: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"❌ JSON 解碼錯誤: {json_err}")
        if 'response' in locals() and response is not None:
            print(f"    無法解析的回應內容: {response.text}")
        else:
            print("    無法解析的回應內容 (回應物件不存在)。")
    return pd.DataFrame()

def get_stock_info() -> pd.DataFrame:
    """
    獲取台灣所有上市公司基本資訊 (TaiwanStockInfo)。
    """
    params = {"dataset": "TaiwanStockInfo"}
    df = fetch_finmind_api("data", params=params)
    time.sleep(1) # 速率限制處理
    return df

def get_stock_price(stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    獲取指定股票在特定日期範圍內的日成交資訊 (TaiwanStockPrice)。
    """
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date
    }
    df = fetch_finmind_api("data", params=params)
    time.sleep(1) # 速率限制處理
    return df

def get_financial_statements(stock_id: str, start_date: str) -> pd.DataFrame:
    """
    獲取指定股票特定日期之後的財務報表 (TaiwanStockFinancialStatements)。
    """
    params = {
        "dataset": "TaiwanStockFinancialStatements",
        "data_id": stock_id,
        "start_date": start_date
    }
    df = fetch_finmind_api("data", params=params)
    time.sleep(1) # 速率限制處理
    return df

def get_total_institutional_investors(start_date: str, end_date: str) -> pd.DataFrame:
    """
    獲取台灣市場整體法人在特定日期範圍內的買賣超 (TaiwanStockTotalInstitutionalInvestors)。
    """
    params = {
        "dataset": "TaiwanStockTotalInstitutionalInvestors",
        "start_date": start_date,
        "end_date": end_date
    }
    df = fetch_finmind_api("data", params=params)
    time.sleep(1) # 速率限制處理
    return df

if __name__ == '__main__':
    # 簡單測試 (實際執行時應透過 run.py)
    print("--- 測試 get_stock_info ---")
    df_info = get_stock_info()
    if not df_info.empty:
        print(df_info.head())
    else:
        print("未獲取到 TaiwanStockInfo 數據。")

    print("\n--- 測試 get_stock_price (2330, 2024-01-01 to 2024-01-05) ---")
    df_price = get_stock_price(stock_id="2330", start_date="2024-01-01", end_date="2024-01-05")
    if not df_price.empty:
        print(df_price.head())
    else:
        print("未獲取到 TaiwanStockPrice 數據。")

    print("\n--- 測試 get_financial_statements (2330, 2023-01-01) ---")
    df_financials = get_financial_statements(stock_id="2330", start_date="2023-01-01")
    if not df_financials.empty:
        # 篩選 'Revenue' 和 'EPS' 以便查看
        # 實際欄位名可能需要根據 API 回應調整，此處為示意
        # relevant_financials = df_financials[df_financials['type'].isin(['Revenue', 'EPS', 'NetIncome', 'GrossProfit'])]
        # print(relevant_financials.head())
        print(df_financials.head()) # 顯示原始前幾行
    else:
        print("未獲取到 TaiwanStockFinancialStatements 數據。")

    print("\n--- 測試 get_total_institutional_investors (2024-01-01 to 2024-01-05) ---")
    df_institutional = get_total_institutional_investors(start_date="2024-01-01", end_date="2024-01-05")
    if not df_institutional.empty:
        print(df_institutional.head())
    else:
        print("未獲取到 TaiwanStockTotalInstitutionalInvestors 數據。")
