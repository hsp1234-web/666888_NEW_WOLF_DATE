# 創世紀計畫 (Project: Genesis) - 後端系統作戰手冊

## 1. 專案宗旨與核心能力

「創世紀計畫」旨在構建一套高效、穩健的金融數據處理與分析後端系統。其核心能力包括：

*   **TAIFEX數據處理子系統**：具備對台灣期貨交易所 (TAIFEX) 歷史數據進行獲取、智能格式探勘、解析與結構化存儲的能力。
    *   **數據偵察 (`taifex_data_prospector`)**: 能夠對目標URL進行輕量級樣本獲取和格式預判，驗證數據可用性，並將偵察結果（成功、失敗、配方元數據）存入快取資料庫 (`prospector_cache.db`)。
    *   **數據管線 (`taifex_data_pipeline`)**: 包含核心的 `determine_parsing_recipe` 函數，用於智能分析原始數據樣本並生成解析配方。設計目標是實現對多種TAIFEX歷史數據格式的兼容處理。*(在「創世紀閃擊戰」演習中，此函數的完整解析邏輯被簡化以驗證框架穩定性，完整解析能力為後續優化目標。)*
    *   **數據下載器 (`taifex_data_downloader`)**: 作為TAIFEX數據URL定義的來源參考，並具備下載完整數據檔案的能力（在閃擊戰中未完整執行下載）。
    *   **數據轉換器 (`taifex_data_transformer`)**: 為未來將解析後的原始數據進一步轉換並載入到分析型資料庫預留的模組（在閃擊戰中未執行）。
*   **yfinance即時數據分析子系統 (`daily_market_analyzer`)**: 負責從 Yahoo Finance (yfinance) 獲取股票市場數據，並提供即時分析能力。
    *   **數據獲取與快取**: 從yfinance API獲取指定股票代碼和時間範圍的市場數據（如開高低收量），並將結果高效存入本地DuckDB快取 (`yfinance_cache.db`)，以提升後續請求的響應速度。
    *   **智能降級**: 當請求的數據顆粒度（如1分鐘線）因限制（如日期過舊）無法獲取時，系統能自動嘗試降級到更粗的時間顆粒度（如日線數據），確保在可能的情況下仍能返回有效數據。
    *   **錯誤處理**: 能優雅處理無效股票代碼、網路問題等異常情況，並在報告中清晰反饋。
    *   **請求日誌**: 詳細記錄每一次數據請求的參數、執行狀態、是否命中快取、錯誤訊息等，存入 `request_log` 表，便於追蹤與分析。

「創世紀閃擊戰」演習已成功驗證了上述能力的基礎框架、端到端數據流、快取效率和錯誤處理韌性。

## 2. 核心架構圖 (數據流)

```mermaid
graph TD
    A[外部數據源: TAIFEX網站] --> B(taifex_data_prospector);
    B -- 樣本獲取 & URL --> C{determine_parsing_recipe\n(in taifex_data_pipeline)};
    C -- 解析配方/None --> B;
    B -- 偵察日誌/狀態 --> D[DuckDB: prospector_cache.db];

    E[外部數據源: yfinance API] --> F(daily_market_analyzer);
    F -- 數據請求 --> G{yfinance_cache.db};
    G -- 快取命中 --> F;
    G -- 快取未命中 --> E;
    F -- 降級決策 --> E;
    F -- 分析報告/日誌 --> H[用戶/控制台];
    F -- 請求記錄 --> I[DuckDB: request_log @ yfinance_cache.db];
    F -- 數據存儲 --> G;

    subgraph TAIFEX數據偵察與處理框架
        B; C; D;
        X(taifex_data_downloader) -.-> B;
        Y(taifex_data_transformer) -.-> D;
    end

    subgraph yfinance數據分析與快取
        F; G; I;
    end
```

*   **TAIFEX數據流 (閃擊戰驗證部分)**：
    1.  `taifex_data_prospector` 根據預定義的URL列表（參考 `taifex_data_downloader` 的URL生成邏輯）向TAIFEX網站發起請求。
    2.  僅獲取數據樣本，並調用 `taifex_data_pipeline` 中的 `determine_parsing_recipe` 函數進行格式預判。
    3.  `determine_parsing_recipe` 返回預判結果（在閃擊戰中簡化為返回`None`）。
    4.  `taifex_data_prospector` 將偵察結果（URL、樣本SHA256、狀態、時間戳等）存入 `prospector_cache.db`。
*   **yfinance數據流**：
    1.  `daily_market_analyzer` 接收用戶請求（股票代碼、時間範圍、顆粒度）。
    2.  首先查詢 `yfinance_cache.db` 中的 `stock_data` 表。
    3.  若快取命中且數據有效，則直接使用快取數據。
    4.  若快取未命中或數據無效，則通過yfinance API從網路獲取。
        *   如果請求的顆粒度（如1分鐘）因限制無法獲取，會嘗試降級到日線數據。
    5.  獲取到的新數據會存入 `stock_data` 表。
    6.  每一次請求的詳細情況（參數、狀態、是否命中快取等）記錄到 `request_log` 表。
    7.  最終生成分析報告（文本格式）到控制台。

## 3. 作戰模組詳解 (各模組職責)

*   **`project_genesis_sandbox/taifex_data_prospector/run.py`**:
    *   **職責**: TAIFEX數據閃擊偵察兵。負責遍歷預定義的TAIFEX數據URL，獲取小量數據樣本，調用 `taifex_data_pipeline` 的核心配方判斷函數，並將偵察結果（包括成功、失敗、HTTP錯誤等狀態）記錄到其本地快取資料庫 (`./workspace/prospector_cache.duckdb`)。
    *   **核心能力**: URL可用性驗證、樣本數據獲取、與配方判斷模組的解耦集成、偵察結果快取。
*   **`project_genesis_sandbox/taifex_data_pipeline/run.py`**:
    *   **職責**: （在「創世紀閃擊戰」中）提供核心的 `determine_parsing_recipe` 函數，用於根據輸入的二進制數據樣本和檔名提示，智能判斷數據的編碼和CSV結構（如表頭位置、數據起始行）。
    *   **核心能力**: 數據編碼檢測（依賴`chardet`）、CSV結構啟發式分析。（完整版的檔案內容解析與載入能力為後續優化項）。
*   **`project_genesis_sandbox/daily_market_analyzer/run.py`**:
    *   **職責**: yfinance股票數據分析器。負責根據用戶請求獲取、處理、快取yfinance數據，並生成分析報告。
    *   **核心能力**: 通過yfinance API獲取數據、本地DuckDB快取管理 (`./workspace/yfinance_cache.duckdb`)、針對數據獲取限制的智能降級邏輯（例如從分鐘級到日級）、詳細的請求日誌記錄、基本的文本報告生成。
*   **`project_genesis_sandbox/taifex_data_downloader/run.py`**:
    *   **職責**: （在「創世紀閃擊戰」中）作為TAIFEX數據URL定義的來源參考，其內部的URL生成邏輯被 `taifex_data_prospector` 借鑒。本身具備完整的TAIFEX歷史數據（近期和年度）下載能力，包括處理GET和POST請求。
    *   **核心能力**: TAIFEX各種數據產品URL的構造、HTTP請求發送與響應處理、檔案保存。
*   **`project_genesis_sandbox/taifex_data_transformer/run.py`**:
    *   **職責**: (在「創世紀閃擊戰」中未執行) 預留模組，設計目標是讀取由 `taifex_data_pipeline` 解析並存儲的原始數據（例如在 `raw_taifex.db` 中），進行數據清洗、轉換、並將其歸檔到結構化的分析型資料庫（例如 `taifex_historical.db`）中的對應表格。
    *   **核心能力**: (設計中) 數據清洗、欄位映射、類型轉換、動態路由到目標表。

## 4. 標準作業流程 (新開發者如何上手與執行)

1.  **環境準備**:
    *   確保Python 3.x 環境。
    *   在 `project_genesis_sandbox/` 目錄下，分別進入各模組目錄 (`taifex_data_prospector`, `daily_market_analyzer`, `taifex_data_pipeline`, `taifex_data_downloader`)，執行 `pip install -r requirements.txt` 安裝各自的依賴。
2.  **執行TAIFEX數據偵察**:
    *   指令: `python project_genesis_sandbox/taifex_data_prospector/run.py [參數]`
    *   常用參數:
        *   `--start_date YYYY-MM-DD`: 近期數據開始日期 (預設: 2025-06-01)
        *   `--end_date YYYY-MM-DD`: 近期數據結束日期 (預設: 2025-07-04)
        *   `--year YYYY`: 要偵察的單一年度歷史數據 (預設: 2015)
        *   `--force_reprospect`: 強制重新偵察所有URL，忽略現有快取。
    *   日誌會輸出到控制台。偵察快取數據庫位於 `./project_genesis_sandbox/workspace/prospector_cache.duckdb`。
3.  **執行yfinance數據分析**:
    *   指令: `python project_genesis_sandbox/daily_market_analyzer/run.py [參數]`
    *   常用參數:
        *   `--ticker <股票代碼>`: (必要) 例如 TSLA, AAPL
        *   `--start <YYYY-MM-DD>`: (必要) 開始日期
        *   `--end <YYYY-MM-DD>`: (必要) 結束日期
        *   `--interval <時間顆粒度>`: (必要) 例如 1m, 1h, 1d, 1wk
        *   `--log_level <INFO|DEBUG|WARNING|ERROR>`: 設定日誌級別 (預設: INFO)
    *   報告會輸出到控制台。yfinance數據快取與請求日誌位於 `./project_genesis_sandbox/workspace/yfinance_cache.duckdb`。
4.  **查看快取資料庫**:
    *   可以使用DuckDB客戶端 (CLI或 відповідні Python庫) 打開位於 `./project_genesis_sandbox/workspace/` 目錄下的 `.duckdb` 檔案進行查詢和分析。

## 5. 資料庫綱要 (Schema)

### 5.1. `prospector_cache.db` (TAIFEX偵察快取)

*   **表名：`prospected_urls`**
    *   `url_hash VARCHAR PRIMARY KEY`: URL的MD5哈希值。
    *   `url VARCHAR`: 原始偵察URL。
    *   `filename_hint VARCHAR`: 根據URL推斷的原始檔名提示。
    *   `sample_sha256 VARCHAR`: 獲取到的數據樣本的SHA256哈希值。
    *   `parsing_recipe_json TEXT`: JSON格式的解析配方（如果成功）。
    *   `status VARCHAR`: 偵察狀態 (例如 `RECIPE_SUCCESS`, `RECIPE_FAILED`, `NOT_FOUND`, `HTTP_ERROR_XXX`, `FETCH_FAILED`)。
    *   `error_message TEXT`: 錯誤訊息（如果失敗）。
    *   `last_prospected_at TIMESTAMP`: 上次偵察此URL的時間戳。
    *   `content_length INTEGER`: 從HTTP Header獲取的Content-Length（如果可得）。
    *   `http_status_code INTEGER`: HTTP請求的狀態碼。

### 5.2. `yfinance_cache.db` (yfinance數據快取與日誌)

*   **表名：`stock_data`** (存儲yfinance市場數據)
    *   `ticker VARCHAR`: 股票代碼。
    *   `datetime TIMESTAMP`: 數據時間戳 (通常為UTC)。
    *   `interval VARCHAR`: 數據的時間顆粒度 (例如 '1d', '1m')。
    *   `open DOUBLE`: 開盤價。
    *   `high DOUBLE`: 最高價。
    *   `low DOUBLE`: 最低價。
    *   `close DOUBLE`: 收盤價（可能已調整）。
    *   `volume BIGINT`: 成交量。
    *   `dividends DOUBLE`: 股息 (如果 `auto_adjust=False`)。
    *   `stock_splits DOUBLE`: 股票分割比例 (如果 `auto_adjust=False`)。
    *   `fetched_at TIMESTAMP`: 此條記錄從API獲取並存入快取的時間戳。
    *   `PRIMARY KEY (ticker, interval, datetime)`

*   **表名：`request_log`** (記錄yfinance數據請求)
    *   `request_id UUID DEFAULT uuid() PRIMARY KEY`: 請求的唯一ID。
    *   `ticker VARCHAR`: 請求的股票代碼。
    *   `start_date TIMESTAMP`: 請求的開始日期。
    *   `end_date TIMESTAMP`: 請求的結束日期。
    *   `requested_interval VARCHAR`: 用戶請求的時間顆粒度。
    *   `actual_interval VARCHAR`: 實際獲取數據的時間顆粒度（考慮到降級）。
    *   `status VARCHAR`: 請求的最終狀態 (例如 `success`, `success_cache`, `downgraded_to_1d`, `error_no_such_ticker`, `error_network_or_api`, 等)。
    *   `cache_hit BOOLEAN`: 是否命中快取。
    *   `error_message TEXT`: 錯誤訊息（如果請求失敗）。
    *   `rows_fetched INTEGER`: 從網路或快取獲取的數據行數。
    *   `request_time TIMESTAMP`: 請求發生的時間戳。
