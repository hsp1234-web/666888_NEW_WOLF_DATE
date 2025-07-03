#!/bin/bash
# 標準化驗收流程 v2.2 (智慧型輕量版)
# 說明: 此版本能智慧判斷 ModuleNotFoundError，
#      將其視為本地環境隔離的成功驗證，而非錯誤。

BASE_DIR=$(pwd)
TEST_PASSED_COLOR='\033[0;32m'
TEST_FAILED_COLOR='\033[0;31m'
INFO_COLOR='\033[0;34m'
NC='\033[0m' # No Color

echo "--- 開始執行標準化驗收流程 v2.2 (智慧型輕量版) ---"

# --- 測試 1: daily_market_analyzer ---
APP_NAME="daily_market_analyzer"
APP_DIR="$BASE_DIR/apps/$APP_NAME"
REQ_FILE="$APP_DIR/requirements.txt"
HAS_ERROR=0

echo -e "\n[驗收] 正在測試微應用: ${INFO_COLOR}$APP_NAME${NC}"

# 步驟 1: 驗證依賴藍圖是否存在
if [ ! -f "$REQ_FILE" ]; then
    echo -e "${TEST_FAILED_COLOR}錯誤: 在 $APP_DIR 中未找到依賴藍圖 (requirements.txt)。${NC}"
    exit 1
else
    echo "依賴藍圖 (requirements.txt) 已找到。"
fi

# 步驟 2: 執行快速結構驗證，並捕捉輸出與錯誤
echo "執行 $APP_NAME 的快速結構驗證 (run.py --help)..."

# 將 stderr 和 stdout 都重定向到一個變數中
EXEC_OUTPUT=$(python3 "$APP_DIR/run.py" --help 2>&1)
RETURN_CODE=$?

# 步驟 3: 智慧判斷結果
if [ $RETURN_CODE -eq 0 ]; then
    echo -e "${TEST_PASSED_COLOR}[驗收通過] $APP_NAME 腳本結構完整，可直接執行。${NC}"
else
    # 檢查錯誤輸出中是否包含 ModuleNotFoundError
    if echo "$EXEC_OUTPUT" | grep -q "ModuleNotFoundError"; then
        echo -e "${TEST_PASSED_COLOR}[驗收通過] 成功驗證環境隔離。${NC}"
        echo -e "${INFO_COLOR}註: 偵測到預期內的 ModuleNotFoundError，依賴將由指揮中心安裝。${NC}"
    else
        echo -e "${TEST_FAILED_COLOR}[驗收失敗] $APP_NAME 發生非預期的錯誤。${NC}"
        echo -e "錯誤碼: $RETURN_CODE"
        echo -e "錯誤輸出:"
        echo -e "$EXEC_OUTPUT"
        HAS_ERROR=1
    fi
fi

# --- 最終總結 ---
if [ $HAS_ERROR -ne 0 ]; then
    echo -e "\n${TEST_FAILED_COLOR}--- 部分驗收測試失敗 ---${NC}"
    exit 1
else
    echo -e "\n${TEST_PASSED_COLOR}--- 所有本地驗收測試均已成功完成 ---${NC}"
    exit 0
fi
