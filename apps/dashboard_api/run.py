# apps/dashboard_api/run.py
import argparse
import sys
import json
import os # 預期會用到
import sqlite3 # 引入 sqlite3
from datetime import datetime, timezone

def main():
    parser = argparse.ArgumentParser(description="鳳凰計畫 - 儀表板數據 API v70.0")
    parser.add_argument("--db-path-logs", required=True, help="日誌資料庫 (logs.sqlite) 的完整路徑。")
    parser.add_argument("--log-history-limit", type=int, default=5, help="要返回的最新日誌條目數量。")

    args = parser.parse_args()

    dashboard_data = {
        "system_status": {
            "latest_event": "無最新事件", # 預設值
            "latest_event_timestamp": None, # 新增時間戳
            # "start_time": None, # start_time 可能需要從特定的 BATTLE 日誌獲取
        },
        "hardware_monitor": {
            "cpu_percent": None,
            "ram_percent": None,
            "timestamp": None
        },
        "log_history": [],
        "query_timestamp": datetime.now(timezone.utc).isoformat(),
        "error": None
    }

    try:
        if not os.path.exists(args.db_path_logs):
            dashboard_data["error"] = f"日誌資料庫檔案不存在: {args.db_path_logs}"
            # 這種情況下，我們仍然打印 JSON，但包含錯誤信息
            json_output = json.dumps(dashboard_data, indent=2, ensure_ascii=False)
            print(json_output)
            sys.exit(0) # API 本身執行成功，只是數據源可能有問題

        conn = sqlite3.connect(f"file:{args.db_path_logs}?mode=ro", uri=True, timeout=5) # 以唯讀模式打開
        cursor = conn.cursor()

        # 1. 查詢 system_status: 最新事件 (BATTLE 或 INFO 包含 "成功執行完畢" 或 "啟動")
        # 我們假設 'message' 欄位包含事件描述，'timestamp' (TEXT) 或 'raw_timestamp' (REAL) 可用於排序
        # 這裡使用 'raw_timestamp' (REAL) 進行排序，更可靠
        cursor.execute("""
            SELECT message, timestamp
            FROM logs
            WHERE level_name = 'INFO' AND (message LIKE '%成功執行完畢%' OR message LIKE '%啟動%')
            ORDER BY raw_timestamp DESC
            LIMIT 1
        """)
        latest_event_row = cursor.fetchone()
        if latest_event_row:
            dashboard_data["system_status"]["latest_event"] = latest_event_row[0]
            dashboard_data["system_status"]["latest_event_timestamp"] = latest_event_row[1]

        # 2. 查詢 hardware_monitor
        cursor.execute("""
            SELECT cpu_usage, memory_usage, timestamp
            FROM hardware_logs
            ORDER BY raw_timestamp DESC
            LIMIT 1
        """)
        hw_row = cursor.fetchone()
        if hw_row:
            dashboard_data["hardware_monitor"]["cpu_percent"] = hw_row[0]
            dashboard_data["hardware_monitor"]["ram_percent"] = hw_row[1]
            dashboard_data["hardware_monitor"]["timestamp"] = hw_row[2]

        # 3. 查詢 log_history (獲取原始 message，因為 formatter 可能已應用於存儲的 message)
        # 或者，如果 setup_logging 將 formatter 後的訊息存儲到 message 欄位，則直接用 message
        # 假設 logs 表中的 message 欄位已包含時間戳和級別等信息（由 formatter 處理）
        cursor.execute("""
            SELECT message
            FROM logs
            ORDER BY raw_timestamp DESC
            LIMIT ?
        """, (args.log_history_limit,))
        log_rows = cursor.fetchall()
        dashboard_data["log_history"] = [row[0] for row in log_rows]

        conn.close()

    except sqlite3.Error as e:
        dashboard_data["error"] = f"查詢日誌資料庫時發生 SQLite 錯誤: {str(e)}"
    except Exception as e:
        dashboard_data["error"] = f"處理儀表板數據時發生未知錯誤: {str(e)}"

    # 將數據以 JSON 格式打印到 stdout
    try:
        json_output = json.dumps(dashboard_data, indent=2, ensure_ascii=False)
        print(json_output) # 確保 stdout 是純淨的 JSON
        sys.exit(0)
    except TypeError as e:
        # 處理潛在的 JSON 序列化問題
        error_json = json.dumps({"error": f"無法將儀表板數據序列化為 JSON: {str(e)}", "raw_data_preview": str(dashboard_data)[:200]})
        print(error_json)
        sys.exit(1)


if __name__ == "__main__":
    main()
