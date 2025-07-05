
# apps/taifex_data_pipeline/run.py (虛假腳本 FOR TESTING)
import argparse
import sys
import json
import os # <<<< ADDED IMPORT OS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假台指期數據管線 FOR TESTING")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據")
    parser.add_argument("--db-path-logs", help="日誌資料庫路徑")
    parser.add_argument("--db-path-raw-taifex", help="原始 Taifex 資料庫路徑")
    parser.add_argument("--db-path-taifex-historical", help="歷史 Taifex 資料庫路徑")
    parser.add_argument("--input-file-path", help="ELT 載入步驟的輸入檔案路徑")
    parser.add_argument("--pipeline-step", choices=['load', 'transform'], help="ELT 管線步驟 (load 或 transform)")
    # 模擬接收任意其他參數
    parser.add_argument('--extra-args', nargs='*', help='其他可能的參數')

    args, unknown = parser.parse_known_args()

    if args.force_data_refresh:
        print("[FAKE TAIFEX PIPELINE] 強制刷新數據已啟用 (FORCE_DATA_REFRESH=True)")

    if args.db_path_logs:
        with open(args.db_path_logs, "a", encoding="utf-8") as f_log:
            f_log.write(f"TAIFEX_PIPELINE_LOG: Step {args.pipeline_step}. Log DB: {args.db_path_logs}.\n")
        print(f"[FAKE TAIFEX PIPELINE] Logged to {args.db_path_logs}")

    if args.pipeline_step == 'load':
        if args.input_file_path and args.db_path_raw_taifex:
            # 模擬從 input_file_path 讀取並寫入 db_path_raw_taifex
            with open(args.db_path_raw_taifex, "a", encoding="utf-8") as f_db:
                f_db.write(f"RAW_TAIFEX_DATA: Loaded from {args.input_file_path}.\n")
            print(f"[FAKE TAIFEX PIPELINE] Loaded data from {args.input_file_path} to {args.db_path_raw_taifex}")
        else:
            print("[FAKE TAIFEX PIPELINE ERROR] Load step missing input_file_path or db_path_raw_taifex.", file=sys.stderr)
            sys.exit(1)
    elif args.pipeline_step == 'transform':
        if args.db_path_raw_taifex and args.db_path_taifex_historical:
            # 模擬從 db_path_raw_taifex 讀取並寫入 db_path_taifex_historical
            with open(args.db_path_taifex_historical, "a", encoding="utf-8") as f_db:
                f_db.write(f"HISTORICAL_TAIFEX_DATA: Transformed from {args.db_path_raw_taifex}.\n")
            print(f"[FAKE TAIFEX PIPELINE] Transformed data from {args.db_path_raw_taifex} to {args.db_path_taifex_historical}")
        else:
            print("[FAKE TAIFEX PIPELINE ERROR] Transform step missing db_path_raw_taifex or db_path_taifex_historical.", file=sys.stderr)
            sys.exit(1)

    output_data = {"script": "taifex_data_pipeline", "args": vars(args), "cwd": os.getcwd()}
    print(json.dumps(output_data)) # 主要用於 _test_run.py 自身的斷言
    if args.pipeline_step == 'load':
        print("[FAKE_PIPELINE_EXECUTION_COMPLETE_LOAD]", flush=True)
    elif args.pipeline_step == 'transform':
        print("[FAKE_PIPELINE_EXECUTION_COMPLETE_TRANSFORM]", flush=True)
    else: # 如果沒有 pipeline_step (雖然不太可能，因為 argparse 有 choices) 或其他情況
        print("[FAKE_PIPELINE_EXECUTION_COMPLETE_UNKNOWN_STEP]", flush=True)
    sys.exit(0)
