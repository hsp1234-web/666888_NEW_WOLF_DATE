# -*- coding: utf-8 -*-
"""
設定檔：作戰日誌與指紋驗證模組 (`pipeline_metadata_manager`)

此檔案用於存放 `pipeline_metadata_manager` 微應用的相關設定。
"""

# 資料庫檔案的名稱
# 在實際部署中，可以考慮使用環境變數或更複雜的設定管理機制來覆寫此預設值。
DATABASE_FILENAME = "pipeline_metadata.duckdb"

# 主要儲存已處理檔案元數據的資料表名稱
PROCESSED_FILES_TABLE_NAME = "processed_files"

# 預設的 ETL/管線版本標籤
# 當記錄檔案處理時，若未特別指定，則使用此版本號。
DEFAULT_ETL_VERSION = "v35.0"

# 其他可能的設定：
# LOG_LEVEL = "INFO"
# MAX_RETRIES_DB_CONNECTION = 3
# ...
