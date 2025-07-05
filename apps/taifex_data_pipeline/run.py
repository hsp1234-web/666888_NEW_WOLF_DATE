
# apps/taifex_data_pipeline/run.py (虛假腳本 FOR TESTING)
import argparse
import sys
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="虛假台指期數據管線 FOR TESTING")
    parser.add_argument("--force-data-refresh", action="store_true", help="是否強制刷新數據")
    # 模擬接收任意其他參數
    parser.add_argument('--extra-args', nargs='*', help='其他可能的參數')

    args, unknown = parser.parse_known_args() # 使用 parse_known_args

    output_data = {"script": "taifex_data_pipeline", "args": vars(args)}
    print(json.dumps(output_data))
    sys.exit(0)
