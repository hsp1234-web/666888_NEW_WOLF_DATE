#!/bin/bash
# 作戰演習：普羅米修斯計畫 - 第一階段

echo "=== [普羅米修斯計畫] 演習開始 ==="

# --- 首次執行：學習與處理 ---
echo "\n--- [回合一] 啟動：系統應學習所有新檔案格式 ---"
# 執行高速載入器 (應能探勘並載入4個真實檔案，跳過3個毒丸)
python ./apps/taifex_data_pipeline/run.py --source-dir ./input --db-path ./workspace/databases/raw_taifex.duckdb --metadata-path ./workspace/databases/pipeline_metadata.duckdb

# 執行數據轉換器 (應能利用新學到的配方完成轉換)
python ./apps/taifex_data_transformer/run.py --raw-db-path ./workspace/databases/raw_taifex.duckdb --target-db-path ./workspace/databases/taifex_historical.duckdb

echo "\n--- [回合一] 結束 ---"
echo "----------------------------------------------------"
sleep 5

# --- 二次執行：驗證快取效率 ---
echo "\n--- [回合二] 啟動：系統應利用快取，高速跳過已知檔案 ---"
# 再次執行高速載入器 (應能透過指紋和配方快取，極速完成)
python ./apps/taifex_data_pipeline/run.py --source-dir ./input --db-path ./workspace/databases/raw_taifex.duckdb --metadata-path ./workspace/databases/pipeline_metadata.duckdb

# 再次執行數據轉換器 (驗證重複執行下的穩定性)
python ./apps/taifex_data_transformer/run.py --raw-db-path ./workspace/databases/raw_taifex.duckdb --target-db-path ./workspace/databases/taifex_historical.duckdb

echo "\n--- [回合二] 結束 ---"
echo "\n=== 演習完畢 ==="
