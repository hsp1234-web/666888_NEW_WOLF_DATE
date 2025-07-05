
# apps/fake_streaming_script/run.py (虛假腳本 FOR STREAMING TEST)
import sys
import time
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假串流腳本 FOR TESTING")
    parser.add_argument("--exit-code", type=int, default=0, help="腳本的退出碼")
    parser.add_argument("--message-count", type=int, default=3, help="要打印的訊息數量")
    parser.add_argument("--db-path-logs", help="日誌資料庫路徑 (用於模擬硬體日誌)")
    parser.add_argument("--num-hw-logs", type=int, default=0, help="要生成的模擬硬體日誌數量")
    parser.add_argument("--hw-log-interval", type=float, default=0.05, help="模擬硬體日誌之間的間隔（秒）")
    args = parser.parse_args()

    for i in range(1, args.message_count + 1):
        print(f"串流訊息 #{i}")
        sys.stdout.flush() # 確保立即刷新緩衝區

        # 在打印串流訊息之間模擬寫入硬體日誌
        if args.db_path_logs and args.num_hw_logs > 0 :
            # 假設每次打印串流訊息後，都嘗試寫入一部分硬體日誌
            # 為了簡化，這裡就在每次主循環時寫入一次（如果 num_hw_logs 允許）
            # 或者可以設計成更均勻分佈
            if i <= args.num_hw_logs: # 簡單控制寫入次數
                 with open(args.db_path_logs, "a", encoding="utf-8") as f_log:
                    timestamp = time.time()
                    # 模擬 hardware_logs 表的格式 (timestamp, cpu_usage, memory_usage)
                    # 這裡用簡化的文本格式
                    f_log.write(f"HARDWARE_LOG_ENTRY: {timestamp}, cpu_usage_fake_{(i*10)%100}, memory_usage_fake_{(i*5)%80}\n")
                 print(f"[FAKE STREAMING SCRIPT] Wrote HW log entry to {args.db_path_logs}")
                 if args.hw_log_interval > 0:
                    time.sleep(args.hw_log_interval)


        if i < args.message_count: #不在最後一次打印後睡眠
            time.sleep(0.1) # 模擬主要工作的耗時操作

    if args.exit_code != 0:
        print(f"串流腳本將以錯誤碼 {args.exit_code} 退出。", file=sys.stderr)
        sys.stderr.flush()

    sys.exit(args.exit_code)
