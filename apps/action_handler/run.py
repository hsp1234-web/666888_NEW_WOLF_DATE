
# apps/action_handler/run.py (虛假腳本 FOR E2E TESTING)
import argparse
import sys
import os
import datetime
import sqlite3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假 Action Handler FOR E2E TESTING")
    parser.add_argument("--action", required=True, choices=['create_log_snapshot'])
    parser.add_argument("--log-db-path", help="日誌資料庫路徑 (用於 create_log_snapshot)")
    parser.add_argument("--output-dir", help="快照輸出目錄 (用於 create_log_snapshot)")
    # 可以添加更多 action 和它們的參數

    args = parser.parse_args()

    if args.action == 'create_log_snapshot':
        if not args.log_db_path or not args.output_dir:
            print("[FAKE ACTION HANDLER ERROR] create_log_snapshot 需要 --log-db-path 和 --output-dir", file=sys.stderr)
            sys.exit(1)

        snapshot_filename = f"log_snapshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        snapshot_filepath = os.path.join(args.output_dir, snapshot_filename)

        try:
            # 模擬從日誌數據庫讀取日誌
            # 為了簡單起見，如果日誌DB是SQLite，我們嘗試連接並讀取；否則，就寫入一些虛擬日誌。
            log_content = "Fake log snapshot content:\n"
            if os.path.exists(args.log_db_path):
                try:
                    # 這裡假設 logs.sqlite 包含一個名為 'logs' 的表，且有 'message' 欄位
                    # 實際的 E2E 測試中，這個 logs.sqlite 是由其他虛假腳本填充的
                    # 為了讓這個虛假 action_handler 能工作，我們不需要真的去讀取 SQLite，
                    # 只需要模擬寫入快照檔案即可。
                    # 但如果 test_action_handler_create_snapshot 要驗證內容，
                    # 則這裡的虛假腳本和填充日誌的虛假腳本需要協同。
                    # 簡單起見，我們這裡寫入固定的幾行 + args.log_db_path 的內容（如果是文字檔）
                    log_content += f"Simulated read from: {args.log_db_path}\n"
                    if os.path.isfile(args.log_db_path): # 如果日誌DB是個文字檔（例如之前虛假腳本寫的）
                         with open(args.log_db_path, "r", encoding="utf-8") as f_db_content:
                            log_content += f_db_content.read()

                except Exception as e:
                    log_content += f"Error reading log db (this is a fake handler, so this is ok): {e}\n"
            else:
                log_content += "Log database not found, using placeholder content.\n"

            with open(snapshot_filepath, "w", encoding="utf-8") as f_snapshot:
                f_snapshot.write(log_content)
            print(f"[FAKE ACTION HANDLER] Created log snapshot: {snapshot_filepath}")
            print("[FAKE_ACTION_HANDLER_EXECUTION_COMPLETE]", flush=True)
            sys.exit(0)
        except Exception as e:
            print(f"[FAKE ACTION HANDLER ERROR] Failed to create snapshot: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"[FAKE ACTION HANDLER ERROR] Unknown action: {args.action}", file=sys.stderr)
        sys.exit(1)
