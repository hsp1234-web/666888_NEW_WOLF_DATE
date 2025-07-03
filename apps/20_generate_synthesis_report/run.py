import os
import json
import argparse
import logging
from datetime import datetime

try:
    import google.generativeai as genai
except ImportError:
    logging.warning("google.generativeai library not found. Please install it: pip install google-generativeai")
    genai = None

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration ---
DEFAULT_SYNTHESIS_REPORTS_DIR = "data/reports" # General reports directory
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DEFAULT_GEMINI_MODEL = "gemini-1.5-pro-latest" # Use a more capable model for synthesis

# This prompt is a starting point and will likely need significant refinement
# based on the actual content of the weekly reports and desired insights.
CROSS_REPORT_SYNTHESIS_PROMPT_TEMPLATE = """\
您是一位資深的宏觀策略分析師，擁有從多份獨立分析報告中洞察深層模式與趨勢的卓越能力。

您現在收到了 {num_reports} 份針對不同【目標週】生成的金融市場分析報告。每份報告都包含了該週的宏觀背景、詳細的市場與社群動態複盤，以及當時的策略洞察。

您的任務是基於以下【多週分析報告合集】，提煉出一份具有更高層次視野的【跨週模式與趨勢綜合報告】。

---
### 【多週分析報告合集】

{concatenated_reports}

---

### 【跨週模式與趨勢綜合報告】

請專注於以下幾個分析維度，並盡可能從提供的報告中提取證據支持您的觀點：

**1. 市場敘事與情緒的演變 (Evolution of Narratives & Sentiment):**
   - 在這些報告覆蓋的時間段內，市場關注的核心主題或敘事（例如，通膨、利率、特定行業動態、地緣政治等）是如何隨時間演變的？
   - 整體市場情緒（如樂觀、悲觀、恐慌、貪婪）呈現出怎樣的變化趨勢？是否存在明顯的轉折點？

**2. 重複出現的市場行為模式 (Recurring Market Behavior Patterns):**
   - 是否觀察到某些特定的市場條件（例如，特定經濟數據發布前後、財報密集公佈期、特定技術指標形態）與市場後續走勢之間存在重複出現的模式？
   - 社群輿情在這些模式中扮演了什麼角色？（例如，輿情是否通常領先/滯後於市場轉折，或者在特定模式下呈現一致的反應？）

**3. 策略有效性分析 (Effectiveness of Highlighted Strategies):**
   - 各週報告中提及的交易策略或市場洞察，在後續的市場發展中（如果可從其他報告推斷）是否得到了驗證？哪些類型的策略似乎更為成功或失敗？

**4. 關鍵差異與共性 (Key Differences & Commonalities):**
   - 比較不同報告所分析的週次，它們在市場驅動因素、主要矛盾、或最終結論上有哪些顯著的差異？又有哪些共通的觀察或教訓？

**5. 宏觀策略建議 (Overarching Strategic Recommendations):**
   - 基於以上所有跨週期的分析，如果讓您為未來一段時間的市場參與者提供3-5條宏觀層面的策略性建議或需要警惕的風險點，那會是什麼？

請確保您的分析是客觀的，並且緊密圍繞所提供的報告內容。如果報告內容不足以支持某個維度的深入分析，請明確指出。
"""

def load_report_content(report_filepath):
    """Loads the content of a single report file."""
    try:
        with open(report_filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logging.warning(f"Report file not found: {report_filepath}. Skipping.")
        return None
    except Exception as e:
        logging.error(f"Error reading report file {report_filepath}: {e}")
        return None

def call_gemini_api_for_synthesis(prompt_text, api_key, model_name):
    """Calls the Gemini API for synthesis."""
    if not genai:
        logging.error("Gemini library (google.generativeai) is not available.")
        raise ImportError("Gemini library not installed.")
    if not api_key:
        logging.error("Gemini API key not provided or found in environment.")
        raise ValueError("Missing Gemini API Key.")

    genai.configure(api_key=api_key)

    generation_config = {
        "temperature": 0.6, # Slightly higher for more creative synthesis
        "top_p": 1.0,
        "top_k": 32,
        "max_output_tokens": 8192, # Synthesis can be very long
    }
    safety_settings = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    ]

    model = genai.GenerativeModel(model_name=model_name,
                                  generation_config=generation_config,
                                  safety_settings=safety_settings)
    try:
        logging.info(f"Calling Gemini API for synthesis with model {model_name}...")
        response = model.generate_content(prompt_text)
        logging.info("Gemini API call for synthesis successful.")
        full_response_text = "".join(part.text for part in response.parts) if response.parts else ""

        if not full_response_text and response.prompt_feedback and response.prompt_feedback.block_reason:
            logging.error(f"Gemini API call blocked. Reason: {response.prompt_feedback.block_reason}")
            return f"Error: Gemini API call blocked. Reason: {response.prompt_feedback.block_reason}"

        return full_response_text
    except Exception as e:
        logging.error(f"Error calling Gemini API for synthesis: {e}")
        return f"Error: Exception during Gemini API call for synthesis - {str(e)}"


def main():
    parser = argparse.ArgumentParser(description="從多份每週分析報告生成綜合報告。") # 中文化
    # 修改 --report-filepaths 參數以符合指令要求
    parser.add_argument("--report-filepaths", type=str, required=True, nargs='+', # nargs='+' 表示允許多個值
                        help="要進行綜合分析的報告檔案路徑列表 (以空格分隔)。") # 中文化
    parser.add_argument("--synthesis-reports-dir", type=str, default=DEFAULT_SYNTHESIS_REPORTS_DIR,
                        help=f"儲存生成的綜合報告的目錄。預設值: {DEFAULT_SYNTHESIS_REPORTS_DIR}") # 中文化
    parser.add_argument("--gemini-model", type=str, default=DEFAULT_GEMINI_MODEL,
                        help=f"用於綜合分析的 Gemini 模型。預設值: {DEFAULT_GEMINI_MODEL}") # 中文化

    args = parser.parse_args()

    logging.info(f"開始從 {len(args.report_filepaths)} 份報告進行跨週報告綜合分析。") # 中文化

    if not GEMINI_API_KEY:
        logging.error("未設定 GEMINI_API_KEY 環境變數。無法繼續。") # 中文化
        return

    concatenated_reports_content = []
    valid_reports_count = 0

    # 直接使用 args.report_filepaths，因為它已經是一個列表
    if not args.report_filepaths:
        logging.error("錯誤：未提供任何報告檔案路徑 (--report-filepaths)。") # 中文化
        return

    for report_path in args.report_filepaths:
        report_basename = os.path.basename(report_path)
        # 嘗試從檔案名提取更友好的標識符，例如移除副檔名和常見後綴
        report_identifier = os.path.splitext(report_basename)[0]
        report_identifier = report_identifier.replace("_AnalysisReport", "").replace("_Report", "")

        content = load_report_content(report_path)
        if content:
            concatenated_reports_content.append(f"\n--- 報告開始: {report_identifier} ({report_basename}) ---\n") # 中文化並加入原始檔名
            concatenated_reports_content.append(content)
            concatenated_reports_content.append(f"\n--- 報告結束: {report_identifier} ---\n") # 中文化
            valid_reports_count += 1
        else:
            logging.warning(f"警告：無法加載報告 '{report_path}' 或其內容為空。將在綜合分析中跳過此報告。") # 中文化

    if not concatenated_reports_content:
        logging.error("未找到有效的報告內容進行綜合分析。正在結束。") # 中文化
        return

    logging.info(f"成功加載 {valid_reports_count} 份報告進行綜合。") # 中文化

    full_concatenated_text = "\n".join(concatenated_reports_content)

    prompt_fill_data = {
        "num_reports": valid_reports_count,
        "concatenated_reports": full_concatenated_text
    }
    final_prompt = CROSS_REPORT_SYNTHESIS_PROMPT_TEMPLATE.format(**prompt_fill_data)

    # 調試長提示訊息的選項 (可選)
    # debug_prompt_dir = os.path.join(args.synthesis_reports_dir, "debug_prompts")
    # os.makedirs(debug_prompt_dir, exist_ok=True)
    # debug_prompt_filename = f"synthesis_prompt_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
    # debug_prompt_path = os.path.join(debug_prompt_dir, debug_prompt_filename)
    # try:
    #     with open(debug_prompt_path, 'w', encoding='utf-8') as f:
    #        f.write(final_prompt)
    #     logging.info(f"已將用於調試的綜合提示儲存至 {debug_prompt_path}") # 中文化
    # except Exception as e:
    #     logging.error(f"儲存調試提示失敗: {e}")


    synthesis_report_content = call_gemini_api_for_synthesis(final_prompt, GEMINI_API_KEY, args.gemini_model)

    os.makedirs(args.synthesis_reports_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    # 檔案名可以包含報告數量信息
    synthesis_filename = f"CrossReportSynthesis_{valid_reports_count}files_{timestamp}.md" # 改為 .md 格式
    synthesis_filepath = os.path.join(args.synthesis_reports_dir, synthesis_filename)

    try:
        with open(synthesis_filepath, 'w', encoding='utf-8') as f:
            f.write(synthesis_report_content)
        logging.info(f"已成功將跨週綜合報告儲存至：{synthesis_filepath}") # 中文化
    except Exception as e:
        logging.error(f"儲存綜合報告至 {synthesis_filepath} 時發生錯誤：{e}") # 中文化

    logging.info("跨週報告綜合分析完成。") # 中文化


if __name__ == "__main__":
    # 示例命令:
    # python apps/20_generate_synthesis_report/run.py \
    #   --report-filepaths data_workspace/reports/market_analysis_FULL_AAPL_2024-07-01_to_2024-07-02_20240715103000.md data_workspace/reports/market_analysis_FULL_MSFT_2024-07-01_to_2024-07-02_20240715103500.md \
    #   --synthesis-reports-dir data_workspace/synthesis_reports
    # (確保 GEMINI_API_KEY 已設定)
    if not genai:
        print("錯誤：未安裝 google.generativeai 套件。請執行 'pip install google-generativeai'") # 中文化
    elif not GEMINI_API_KEY:
        print("錯誤：未設定 GEMINI_API_KEY 環境變數。") # 中文化
    else:
        main()
