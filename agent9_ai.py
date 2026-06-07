import os
import time
import logging
from dotenv import load_dotenv
from google import genai

# 1. Load .env once
load_dotenv()

# 2. Extract keys securely (splitting by space to ignore any inline comments in the .env file)
key_std = os.getenv("GEMINI_API_KEY_STD", "").split(" ")[0]
key_omni = os.getenv("GEMINI_API_KEY_OMNI", "").split(" ")[0]

if not key_std or not key_omni:
    raise EnvironmentError("CRITICAL: GEMINI_API_KEY_STD and GEMINI_API_KEY_OMNI must be set in your .env file.")

# 3. Initialize clients once
client_std = genai.Client(api_key=key_std)
client_omni = genai.Client(api_key=key_omni)

def _generate_with_retry(client, prompt, agent_name, format_tag):
    """DRY Helper function to handle Gemini API calls, retries, and formatting."""
    max_retries = 3 
    retry_delay = 60

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-3.5-flash", 
                contents=prompt
            )
            # Clean output based on requested format (json vs html)
            return response.text.replace(f'```{format_tag}', '').replace('```', '').strip()
            
        except Exception as e:
            error_msg = str(e)
            if ("503" in error_msg or "429" in error_msg) and attempt < max_retries - 1:
                logging.warning(f"{agent_name}: Server busy (429/503). Retrying in {retry_delay}s... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                continue
            else:
                logging.error(f"{agent_name} failed after {attempt+1} attempts: {error_msg}")
                return f"{agent_name} Error: {error_msg}"

def agent9_analyze_business(ticker, eps, roe):
    logging.info(f"Standard AI Analysis started for {ticker}...")
    is_crypto = ticker.endswith("-USD")
    
    if is_crypto:
        prompt = f"""Act as a Senior Crypto Asset Analyst. Please perform a deep-dive analysis on {ticker}.

Analysis Requirements:
1. Tokenomics & Network: Analyze utility, supply dynamics (inflation/burn), and network adoption.
2. Market Momentum: Evaluate liquidity, volume trends, and cycle positioning.
3. Sentiment & Catalysts: Analyze recent news, narrative strength (e.g., DeFi, AI, Layer 1), and upcoming catalysts.

Output Format (STRICT JSON):
- Output ONLY valid JSON.
- DO NOT use markdown code blocks (e.g., ```json).
- DO NOT add introductory or concluding sentences.
- Investment thesis must be a single string with 3 professional bullet points separated by "\\n".

Template:
{{
  "ticker": "{ticker}",
  "quality_score": "1-10",
  "valuation_status": "Strong/Neutral/Weak Momentum",
  "management_sentiment": "Positive/Negative/Neutral",
  "investment_thesis": "Point 1\\nPoint 2\\nPoint 3"
}}"""
    else:
        prompt = f"""Act as a Senior Investment Analyst. Please perform a deep-dive analysis on {ticker} using these inputs:
- EPS Growth: {eps}%
- ROE: {roe}%

Analysis Requirements:
1. Financial Quality: Calculate FCF Yield and interpret ROE using Dupont Analysis. Focus on operational efficiency versus financial leverage.
2. Valuation: Compare current P/E with 5-year historical averages. Classify as Undervalued, Fair, or Overvalued.
3. Sentiment: Analyze recent earnings and news. Identify bullish/bearish guidance for the next 12 months.

Output Format (STRICT JSON):
- Output ONLY valid JSON.
- DO NOT use markdown code blocks (e.g., ```json).
- DO NOT add introductory or concluding sentences.
- Investment thesis must be a single string with 3 professional bullet points separated by "\\n".

Template:
{{
  "ticker": "{ticker}",
  "quality_score": "1-10",
  "valuation_status": "Undervalued/Fair/Overvalued",
  "management_sentiment": "Positive/Negative/Neutral",
  "investment_thesis": "Point 1\\nPoint 2\\nPoint 3"
}}"""

    return _generate_with_retry(client_std, prompt, "Standard AI", "json")

def omni_trader_analyze(ticker, market, strategy, price_info):
    logging.info(f"OmniTrader Analysis started for {ticker}...")
    prompt = f"""คุณคือ "OmniTrader" สุดยอดระบบ AI วิเคราะห์การลงทุนขั้นสูง ที่เชี่ยวชาญทั้งตลาด "หุ้น (Equities)" และ "คริปโทเคอร์เรนซี (Cryptocurrency)" รวมถึงมีความเชี่ยวชาญเชิงลึกในกลยุทธ์ทั้ง "Day Trade (เก็งกำไรรายวัน)" และ "DCA (สะสมระยะยาว)"

ข้อมูลปัจจุบัน:
- สินทรัพย์ (Ticker): {ticker}
- ตลาด (Market): {market}
- กลยุทธ์ (Strategy): {strategy}
- ข้อมูลราคา/สถานะพอร์ต: {price_info}

กรุณาวิเคราะห์ตามโครงสร้างด้านล่าง โดยให้ **ปรับเปลี่ยนวิธีคิด (Adapt)** ตาม "ตลาด" และ "กลยุทธ์" ที่ฉันระบุ ดังนี้:

## Step 1: Asset Core Assessment (ประเมินสภาวะของสินทรัพย์ตามตลาด)
[หากเป็นตลาดหุ้น]: โฟกัสที่ผลประกอบการ, โมเดลธุรกิจ, ข่าวสารบริษัท, P/E Ratio และ Fund Flow ของสถาบัน
[หากเป็นตลาดคริปโต]: โฟกัสที่ Tokenomics (การเฟ้อ/เผาเหรียญ), On-chain Data, Use Case/Narrative ของเหรียญ, และความสัมพันธ์กับ Bitcoin Dominance
สภาพแวดล้อมมหภาค (Macro): สภาพคล่องโลก (Liquidity), อัตราดอกเบี้ย, เงินเฟ้อ ส่งผลต่อสินทรัพย์ตัวนี้อย่างไรในตอนนี้?

## Step 2: Strategy-Specific Analysis (วิเคราะห์เจาะลึกตามกลยุทธ์)
[หากฉันระบุว่า "Day Trade" หรือ "SWING"]: ให้ข้ามปัจจัยพื้นฐานระยะยาวไปเลย แล้วโฟกัสที่ Price Action, Momentum และ Volatility
[หากฉันระบุว่า "DCA"]: ให้ข้ามความผันผวนระยะสั้น แล้วโฟกัสที่ "โอกาสรอดของโปรเจกต์/บริษัทในอีก 3-5 ปีข้างหน้า" (Survivability)

## Step 3: Actionable Trading Plan & Risk Management (แผนปฏิบัติการ)
[แผนสำหรับ Day Trade/SWING]: Trade Bias, Entry Price, Stop Loss (SL), Take Profit (TP), Risk/Reward (RR)
[แผนสำหรับ DCA]: Accumulation Zones, Invalidation Point, Allocation Weight

⚠️ กฎเหล็กของ OmniTrader:
1. หากสินทรัพย์เป็นคริปโตกลุ่ม "Meme Coin" หรือหุ้นปั่นที่มีสภาพคล่องต่ำ ให้เตือนความเสี่ยงตัวโตๆ และห้ามแนะนำให้ DCA เด็ดขาด
2. ตอบให้ตรงประเด็น เป็นรูปธรรม ไม่ใช้น้ำเยอะ 
3. หากข้อมูลราคาที่ให้มาขัดแย้งกับเทรนด์ ให้ยึด "ราคาล่าสุดและพฤติกรรมราคา" เป็นตัวตัดสินหลักสำหรับ Day Trade

สรุปกลยุทธ์สำหรับ {ticker} ใน 1 ประโยค:

(กรุณาตอบกลับเป็นรูปแบบ HTML Tags สำหรับแสดงผลบนเว็บไซต์โดยตรง ห้ามใช้ Markdown)"""

    return _generate_with_retry(client_omni, prompt, "OmniTrader AI", "html")