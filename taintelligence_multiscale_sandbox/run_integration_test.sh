#!/bin/bash
# 作戰演習：多重時間尺度TAINTELLIGENCE - 整合測試

echo "=== [TAINTELLIGENCE整合測試] 演習開始 ==="

# --- 清理環境：確保每次演習都在乾淨的狀態下開始 ---
echo "\n--- [準備階段] 清理舊的資料庫與報告... ---"
rm -f ./workspace/db/*.duckdb
rm -f ./workspace/reports/*.html
echo "環境清理完畢。"

# --- 測試案例 1: 全週期執行與報告一致性 ---
echo "\n--- [測試案例 1.1] 基準測試 (1d - 日級週期) ---"
python main_controller.py --ticker "TSLA" --interval "1d" --start "2025-06-01" --end "2025-07-04"

echo "\n--- [測試案例 1.2] 基準測試 (1h - 小時級週期) ---"
python main_controller.py --ticker "NVDA" --interval "1h" --start "2025-07-01" --end "2025-07-04"

echo "\n--- [測試案例 1.3] 基準測試 (1m - 分鐘級週期) ---"
python main_controller.py --ticker "MSFT" --interval "1m" --start "2025-07-03" --end "2025-07-04"


# --- 測試案例 2: 快取驗證 ---
echo "\n--- [測試案例 2.1] 快取驗證 (重複請求 1d 日級週期) ---"
python main_controller.py --ticker "TSLA" --interval "1d" --start "2025-06-01" --end "2025-07-04"


# --- 測試案例 3: 錯誤韌性與智能降級 ---
echo "\n--- [測試案例 3.1] 智能降級測試 (請求過時的 1m 數據) ---"
python main_controller.py --ticker "AAPL" --interval "1m" --start "2024-01-01" --end "2024-01-05"


echo "\n=== 演習完畢 ==="
