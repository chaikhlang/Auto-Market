"""
╔══════════════════════════════════════════════════════════════════════╗
║         AI TRADING AGENCY HQ  —  main.py  (Backend Pipeline)         ║
║         8-Agent System: Scanner → Factor → Risk → Setup →            ║
║                         Trail → Notifier → Logger → Updater          ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import concurrent.futures
import hashlib
import logging
import datetime
import schedule
import threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from agent9_ai import agent9_analyze_business, omni_trader_analyze

import yfinance as yf
import pandas as pd
import ta
import requests

from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore

# ─────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("HQ")

# ─────────────────────────────────────────────────────────────────────
# CONFIG — loaded from .env
# ─────────────────────────────────────────────────────────────────────
load_dotenv()

CREDS_PATH       = os.getenv("FIREBASE_CREDENTIALS_PATH", "serviceAccountKey.json")
PROJECT_ID       = os.getenv("FIREBASE_PROJECT_ID", "")
USER_UID         = os.getenv("USER_UID", "")
TOTAL_CAPITAL    = float(os.getenv("TOTAL_CAPITAL", 100_000))
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE_PCT", 1.5)) / 100   # 0.015
MAX_POSITIONS    = int(os.getenv("MAX_POSITIONS", 10))
TRADE_MODE       = os.getenv("TRADE_MODE", "SWING").upper()
CURRENT_MARKET_MODE = "STOCKS"

# ─────────────────────────────────────────────────────────────────────
# FIREBASE ADMIN INIT
# ─────────────────────────────────────────────────────────────────────
def init_firebase() -> firestore.Client:
    """Initialise Firebase Admin SDK and return a Firestore client."""
    if not firebase_admin._apps:
        if os.path.exists(CREDS_PATH):
            cred = credentials.Certificate(CREDS_PATH)
            firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
        else:
            firebase_admin.initialize_app(options={"projectId": PROJECT_ID})
        log.info("Firebase Admin initialised (project: %s)", PROJECT_ID)
    return firestore.client()

DB: firestore.Client = init_firebase()

# ─────────────────────────────────────────────────────────────────────
# FLASK API (For On-Demand AI Analysis)
# ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

@app.route('/analyze', methods=['POST'])
def analyze_endpoint():
    try:
        data = request.json
        if not data or 'ticker' not in data:
            return jsonify({"error": "Missing ticker parameter"}), 400
            
        ticker = data.get('ticker')
        eps = data.get('eps', 0)
        roe = data.get('roe', 0)
        
        result = agent9_analyze_business(ticker, eps, roe)
        return jsonify({"analysis": result})
    except Exception as e:
        log.error(f"Error in analyze_endpoint: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/omni_analyze', methods=['POST'])
def omni_analyze_endpoint():
    try:
        data = request.json
        if not data or 'ticker' not in data:
            log.warning("omni_analyze: Missing ticker parameter in request")
            return jsonify({"error": "Missing ticker parameter"}), 400
            
        ticker = data.get('ticker')
        market = data.get('market', 'STOCKS')
        strategy = data.get('strategy', 'SWING')
        price_info = data.get('price_info', 'No data')
        
        log.info(f"API Request: /omni_analyze for {ticker} ({market} / {strategy})")
        result = omni_trader_analyze(ticker, market, strategy, price_info)
        return jsonify({"analysis": result})
    except Exception as e:
        log.error(f"Error in omni_analyze_endpoint: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/run_pipeline', methods=['POST'])
def run_pipeline_endpoint():
    global CURRENT_MARKET_MODE
    try:
        data = request.json or {}
        CURRENT_MARKET_MODE = data.get("mode", "STOCKS")
        threading.Thread(target=run_pipeline).start()
        return jsonify({"status": "success", "message": f"Pipeline scan initiated in background (Mode: {CURRENT_MARKET_MODE})."})
    except Exception as e:
        log.error(f"Error in run_pipeline_endpoint: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

def run_flask():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# ─────────────────────────────────────────────────────────────────────
# UNIVERSE (Dynamic Rising Stars Fetcher)
# ─────────────────────────────────────────────────────────────────────
def get_rising_star_tickers():
    from io import StringIO
    
    log.info("Fetching Mid-Cap and Small-Cap tickers for Rising Stars scan...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        mid_url = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
        mid_html = requests.get(mid_url, headers=headers).text
        mid_df = pd.read_html(StringIO(mid_html))[0]
        mid_col = 'Symbol' if 'Symbol' in mid_df.columns else 'Ticker' if 'Ticker' in mid_df.columns else mid_df.columns[0]
        mid_tickers = mid_df[mid_col].tolist()
        
        small_url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
        small_html = requests.get(small_url, headers=headers).text
        small_df = pd.read_html(StringIO(small_html))[0]
        small_col = 'Symbol' if 'Symbol' in small_df.columns else 'Ticker' if 'Ticker' in small_df.columns else small_df.columns[0]
        small_tickers = small_df[small_col].tolist()
        
        all_tickers = mid_tickers + small_tickers
        clean_tickers = [str(t).replace('.', '-') for t in all_tickers]
        
        log.info(f"Successfully loaded {len(clean_tickers)} tickers.")
        return clean_tickers
        
    except Exception as e:
        log.error(f"Failed to fetch tickers: Pandas/Network issue. Using backup list.")
        return ["PLTR", "SMCI", "ARM", "HIMS", "CRWD", "DDOG", "NET"]

UNIVERSE = get_rising_star_tickers()

def get_crypto_tickers():
    log.info("Fetching USDT crypto pairs from Binance...")
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        resp = requests.get(url).json()
        stablecoins = {"USDT", "USDC", "FDUSD", "TUSD", "DAI", "EUR"}
        tickers = []
        for s in resp.get("symbols", []):
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
                base = s.get("baseAsset")
                if base not in stablecoins:
                    tickers.append(f"{base}-USD")
        log.info(f"Loaded {len(tickers)} crypto tickers.")
        return tickers
    except Exception as e:
        log.error(f"Failed to fetch crypto tickers: {e}")
        return ["BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD"]

# ─────────────────────────────────────────────────────────────────────
#  HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"

def signal_id(ticker: str) -> str:
    day = datetime.date.today().isoformat()
    raw = f"{ticker}_{day}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

def log_to_firestore(collection_path: str, doc_id: str, data: dict):
    ref = DB.collection("users").document(USER_UID).collection(collection_path).document(doc_id)
    ref.set(data, merge=True)

def agent_log(agent_num: int, agent_name: str, message: str, level: str = "INFO"):
    entry = {
        "agent":     f"A{agent_num}:{agent_name}",
        "message":   message,
        "level":     level,
        "timestamp": utc_now()
    }
    method = getattr(log, level.lower(), log.info)
    method("[A%d %-18s] %s", agent_num, agent_name, message)
    try:
        log_id = hashlib.sha256((entry["agent"] + message + entry["timestamp"]).encode()).hexdigest()[:12]
        log_to_firestore("pipeline_logs", log_id, entry)
    except Exception:
        pass 

# ══════════════════════════════════════════════════════════════════════
#  AGENT 1 — FUNDAMENTAL SCANNER
# ══════════════════════════════════════════════════════════════════════
def agent1_scanner(universe: list[str]) -> list[dict]:
    agent_log(1, "SCANNER", f"Starting scan on {len(universe)} symbols (Mode: {CURRENT_MARKET_MODE}).")
    passed = []
    completed = 0
    lock = threading.Lock()

    def check_asset(ticker):
        try:
            stock = yf.Ticker(ticker)
            info  = stock.info

            hist = stock.history(period="25d", auto_adjust=True)
            if hist.empty or len(hist) < 21:
                return None

            avg_vol     = hist["Volume"].iloc[:-1].rolling(20).mean().iloc[-1]
            current_vol = hist["Volume"].iloc[-1]
            vol_spike   = current_vol / avg_vol if avg_vol > 0 else 0
            price       = info.get("currentPrice") or info.get("regularMarketPrice", 0)
            if price <= 0:
                price = hist["Close"].iloc[-1]

            if CURRENT_MARKET_MODE == "STOCKS":
                eps_growth = info.get("earningsGrowth", 0) or 0       
                rev_growth = info.get("revenueGrowth", 0) or 0
                roe        = info.get("returnOnEquity", 0) or 0        
                peg        = info.get("pegRatio", 999) or 999
                de_ratio   = info.get("debtToEquity", 999) or 999      
                de_norm    = de_ratio / 100 if de_ratio > 10 else de_ratio  

                if eps_growth < 0.25 or rev_growth < 0.20 or roe < 0.15 or peg > 2.0 or de_norm > 2.0:
                    return None

                if avg_vol < 300000 or vol_spike <= 0:
                    return None

                return {
                    "ticker":      ticker,
                    "eps_growth":  round(eps_growth * 100, 2),
                    "rev_growth":  round(rev_growth * 100, 2),
                    "roe":         round(roe * 100, 2),
                    "peg":         round(peg, 2),
                    "de_ratio":    round(de_norm, 2),
                    "vol_spike_x": round(vol_spike, 2),
                    "price":       price,
                    "market_cap":  info.get("marketCap", 0),
                    "sector":      info.get("sector", "Unknown"),
                }
            else:
                if avg_vol * price <= 20000000:
                    return None
                
                return {
                    "ticker":      ticker,
                    "eps_growth":  0,
                    "rev_growth":  0,
                    "roe":         0,
                    "peg":         0,
                    "de_ratio":    0,
                    "vol_spike_x": round(vol_spike, 2),
                    "price":       price,
                    "market_cap":  info.get("marketCap", 0),
                    "sector":      "Crypto",
                }

        except Exception as exc:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(check_asset, t): t for t in universe}
        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            try:
                res = future.result()
                if res:
                    passed.append(res)
                    agent_log(1, "SCANNER", f"PASS {ticker}: vol_spike={res['vol_spike_x']}x")
            except Exception:
                pass
            
            with lock:
                completed += 1
                if completed % 50 == 0:
                    agent_log(1, "SCANNER", f"Progress: Scanned {completed}/{len(universe)}...")

    agent_log(1, "SCANNER", f"Scan complete. {len(passed)}/{len(universe)} symbols passed.")
    return passed

# ══════════════════════════════════════════════════════════════════════
#  AGENT 2 — FACTOR / TECHNICAL ANALYST
# ══════════════════════════════════════════════════════════════════════
def agent2_factor_analyst(candidates: list[dict]) -> list[dict]:
    agent_log(2, "FACTOR", f"Running technical filter on {len(candidates)} candidates.")
    confirmed = []

    for c in candidates:
        ticker = c["ticker"]
        try:
            hist = yf.download(ticker, period="90d", interval="1d", auto_adjust=True, progress=False)
            if hist is None or len(hist) < 30:
                continue

            close = hist["Close"].squeeze()
            ema9  = ta.trend.ema_indicator(close, window=9)
            ema21 = ta.trend.ema_indicator(close, window=21)
            if ema9 is None or ema21 is None:
                continue

            last_ema9  = float(ema9.iloc[-1])
            last_ema21 = float(ema21.iloc[-1])
            prev_ema9  = float(ema9.iloc[-2])
            prev_ema21 = float(ema21.iloc[-2])

            bullish_cross = (last_ema9 > last_ema21)           
            fresh_cross   = (prev_ema9 <= prev_ema21)          

            rsi_series = ta.momentum.rsi(close, window=14)
            if rsi_series is None:
                continue
            last_rsi = float(rsi_series.iloc[-1])

            if not bullish_cross or not (40 <= last_rsi <= 65):
                continue

            c.update({
                "ema9":        round(last_ema9, 4),
                "ema21":       round(last_ema21, 4),
                "fresh_cross": fresh_cross,
                "rsi":         round(last_rsi, 2),
                "tech_score":  round((65 - last_rsi) / 25 + (last_ema9 - last_ema21) / last_ema21 * 100, 4),
            })
            confirmed.append(c)
            agent_log(2, "FACTOR", f"PASS {ticker}: EMA9={last_ema9:.2f} EMA21={last_ema21:.2f} RSI={last_rsi:.1f}")
            time.sleep(0.3)

        except Exception as exc:
            pass

    agent_log(2, "FACTOR", f"Technical filter complete. {len(confirmed)} confirmed setups.")
    return confirmed

# ══════════════════════════════════════════════════════════════════════
#  AGENT 3 — RISK MANAGER
# ══════════════════════════════════════════════════════════════════════
def agent3_risk_manager(confirmed: list[dict], existing_positions: int = 0) -> list[dict]:
    agent_log(3, "RISK_MGR", f"Sizing {len(confirmed)} positions. Capital=${TOTAL_CAPITAL:,.0f}")
    sized = []
    available_slots = MAX_POSITIONS - existing_positions

    if available_slots <= 0:
        agent_log(3, "RISK_MGR", "Max positions reached. No new trades.")
        return []

    dollar_risk = TOTAL_CAPITAL * RISK_PER_TRADE

    for c in confirmed[:available_slots]:
        price = c.get("price", 0)
        if not price or price <= 0:
            continue

        approx_sl_dist = price * 0.03
        shares         = int(dollar_risk / approx_sl_dist) if approx_sl_dist > 0 else 0

        if shares <= 0:
            continue

        position_value = shares * price
        c.update({
            "dollar_risk":     round(dollar_risk, 2),
            "approx_sl_dist":  round(approx_sl_dist, 4),
            "shares":          shares,
            "position_value":  round(position_value, 2),
        })
        sized.append(c)

    agent_log(3, "RISK_MGR", f"Risk sizing done. {len(sized)} positions.")
    return sized

# ══════════════════════════════════════════════════════════════════════
#  AGENT 4 — SETUP PLANNER
# ══════════════════════════════════════════════════════════════════════
def agent4_setup_planner(sized: list[dict]) -> list[dict]:
    agent_log(4, "SETUP", f"Planning entry/SL/TP for {len(sized)} setups.")
    planned = []

    for s in sized:
        ticker = s["ticker"]
        try:
            hist = yf.download(ticker, period="30d", interval="1d", auto_adjust=True, progress=False)
            if hist is None or len(hist) < 15:
                continue

            high  = hist["High"].squeeze()
            low   = hist["Low"].squeeze()
            close = hist["Close"].squeeze()
            atr_series = ta.volatility.average_true_range(high, low, close, window=14)
            if atr_series is None or atr_series.empty:
                continue

            atr   = float(atr_series.iloc[-1])
            price = float(close.iloc[-1])

            entry = round(price, 4)
            
            if TRADE_MODE == "DCA":
                sl, tp, tp1, tp15 = 0.0, 0.0, 0.0, 0.0
                risk = entry
            else:
                sl    = round(entry - 1.5 * atr, 4)
                risk  = entry - sl
                tp    = round(entry + 2.0 * risk, 4)
                tp1   = round(entry + 1.0 * risk, 4)   
                tp15  = round(entry + 1.5 * risk, 4)

            dollar_risk = s.get("dollar_risk", TOTAL_CAPITAL * RISK_PER_TRADE)
            shares      = int(dollar_risk / risk) if risk > 0 else s.get("shares", 0)
            position_value = round(shares * entry, 2)

            s.update({
                "entry":          entry,
                "sl":             sl,
                "tp":             tp,
                "tp1":            tp1,
                "tp15":           tp15,
                "atr":            round(atr, 4),
                "risk_per_share": round(risk, 4),
                "shares":         shares,
                "position_value": position_value,
                "rr_ratio":       2.0,
                "side":           "LONG",
            })
            planned.append(s)
            time.sleep(0.3)

        except Exception as exc:
            pass

    agent_log(4, "SETUP", f"Setup planning done. {len(planned)} setups ready.")
    return planned

# ══════════════════════════════════════════════════════════════════════
#  AGENT 5 — TRAILING STOP MONITOR
# ══════════════════════════════════════════════════════════════════════
def agent5_trailing_monitor() -> dict:
    if TRADE_MODE == "DCA":
        return {"adjusted": 0, "checked": 0, "errors": 0}

    agent_log(5, "TRAIL", "Checking trailing stops for all open positions…")
    results = {"adjusted": 0, "checked": 0, "errors": 0}

    try:
        port_ref = DB.collection("users").document(USER_UID).collection("portfolio")
        docs     = port_ref.where("status", "in", ["OPEN", "TRAIL"]).stream()

        for doc in docs:
            pos = doc.to_dict()
            ticker = pos.get("ticker", "")
            results["checked"] += 1
            try:
                stock      = yf.Ticker(ticker)
                price_info = stock.fast_info
                current    = float(price_info.get("last_price", 0) or price_info.get("regularMarketPrice", 0))

                if current <= 0: continue

                entry          = float(pos.get("entry", current))
                sl             = float(pos.get("sl", entry * 0.97))
                risk           = entry - sl
                new_sl         = sl
                new_status     = pos.get("status", "OPEN")

                if current >= entry + risk and sl < entry:
                    new_sl     = entry
                    new_status = "TRAIL"

                if current >= entry + 2 * risk and sl < entry + risk:
                    new_sl     = round(entry + 0.5 * risk, 4)
                    new_status = "TRAIL"

                if new_sl != sl:
                    doc.reference.update({
                        "sl":          new_sl,
                        "status":      new_status,
                        "last_updated": utc_now(),
                    })
                    results["adjusted"] += 1

                time.sleep(0.2)

            except Exception:
                results["errors"] += 1

    except Exception:
        results["errors"] += 1

    agent_log(5, "TRAIL", f"Trail check done. adjusted={results['adjusted']}")
    return results

# ══════════════════════════════════════════════════════════════════════
#  AGENT 6 — NOTIFIER / FORMATTER
# ══════════════════════════════════════════════════════════════════════
def agent6_notifier(planned: list[dict]) -> list[dict]:
    agent_log(6, "NOTIFIER", f"Formatting {len(planned)} signal payloads.")
    signals  = []
    deduped  = 0

    existing_ids: set = set()
    try:
        ref    = DB.collection("users").document(USER_UID).collection("signals")
        today  = datetime.date.today().isoformat()
        snaps  = ref.where("date", "==", today).stream()
        for snap in snaps: existing_ids.add(snap.id)
    except Exception:
        pass

    for p in planned:
        sig_id = signal_id(p["ticker"])
        if sig_id in existing_ids:
            deduped += 1
            continue

        payload = {
            "signal_id":       sig_id,
            "ticker":          p["ticker"],
            "side":            p.get("side", "LONG"),
            "entry":           p.get("entry", 0),
            "sl":              p.get("sl", 0),
            "tp":              p.get("tp", 0),
            "tp1":             p.get("tp1", 0),
            "tp15":            p.get("tp15", 0),
            "atr":             p.get("atr", 0),
            "rr_ratio":        p.get("rr_ratio", 2.0),
            "shares":          p.get("shares", 0),
            "position_value":  p.get("position_value", 0),
            "risk_per_share":  p.get("risk_per_share", 0),
            "dollar_risk":     p.get("dollar_risk", 0),
            "eps_growth":      p.get("eps_growth", 0),
            "roe":             p.get("roe", 0),
            "peg":             p.get("peg", 0),
            "rsi":             p.get("rsi", 0),
            "ema9":            p.get("ema9", 0),
            "ema21":           p.get("ema21", 0),
            "fresh_cross":     p.get("fresh_cross", False),
            "vol_spike_x":     p.get("vol_spike_x", 0),
            "sector":          p.get("sector", "Unknown"),
            "date":            datetime.date.today().isoformat(),
            "timestamp":       utc_now(),
            "status":          "NEW",
            "current_price":   p.get("entry", 0),
            "pnl_pct":         0.0,
        }
        signals.append(payload)

    agent_log(6, "NOTIFIER", f"Formatting done. {len(signals)} ready, {deduped} deduped.")
    return signals

# ══════════════════════════════════════════════════════════════════════
#  AGENT 7 — FIRESTORE LOGGER (Manual Add to Port Mode)
# ══════════════════════════════════════════════════════════════════════
def agent7_logger(signals: list[dict]) -> dict:
    agent_log(7, "LOGGER", f"Writing {len(signals)} signals to Firestore.")
    results = {"written": 0, "errors": 0}

    for sig in signals:
        ticker    = sig["ticker"]
        signal_id_val = sig["signal_id"]

        # ── Write to signals collection ONLY ──────────────────────────
        try:
            log_to_firestore("signals", signal_id_val, sig)
            agent_log(7, "LOGGER", f"SIGNAL WRITE {ticker} → signals/{signal_id_val}")
            results["written"] += 1
        except Exception as exc:
            agent_log(7, "LOGGER", f"WRITE ERROR (signal) {ticker}: {exc}", "ERROR")
            results["errors"] += 1

    agent_log(7, "LOGGER", f"Logger done. written={results['written']} errors={results['errors']}")
    return results

# ══════════════════════════════════════════════════════════════════════
#  AGENT 8 — PORTFOLIO UPDATER
# ══════════════════════════════════════════════════════════════════════
def agent8_portfolio_updater():
    agent_log(8, "UPDATER", "Portfolio update cycle starting…")

    try:
        port_ref = DB.collection("users").document(USER_UID).collection("portfolio")
        docs     = list(port_ref.where("status", "in", ["NEW", "OPEN", "TRAIL"]).stream())
    except Exception as exc:
        agent_log(8, "UPDATER", f"Firestore read failed: {exc}", "ERROR")
        return

    if not docs:
        agent_log(8, "UPDATER", "No active positions to update.")
        return

    updated_count = 0
    cumulative_pnl_list = []

    for doc in docs:
        ticker = doc.id
        pos    = doc.to_dict()
        entry  = float(pos.get("entry", 0))

        if entry <= 0: continue

        current = 0.0
        stock = yf.Ticker(ticker)
        try:
            fi = stock.fast_info
            # รองรับทั้ง yfinance เวอร์ชันเก่า(dict) และใหม่(FastInfo object)
            if hasattr(fi, 'get'):
                current = float(fi.get("last_price") or fi.get("regularMarketPrice") or 0)
            else:
                current = float(getattr(fi, "last_price", getattr(fi, "regularMarketPrice", 0)))
        except Exception:
            pass

        if current <= 0:
            try:
                hist = stock.history(period="1d")
                if not hist.empty:
                    current = float(hist['Close'].iloc[-1])
            except Exception:
                pass

        if current <= 0: 
            time.sleep(1)
            continue

        pnl_pct = round((current - entry) / entry * 100, 4)
        cumulative_pnl_list.append(pnl_pct)

        sl = float(pos.get("sl", 0))
        tp = float(pos.get("tp", 0))
        current_status = pos.get("status", "OPEN")

        if sl > 0 and current <= sl: new_status = "STOPPED"
        elif tp > 0 and current >= tp: new_status = "TP_HIT"
        elif current_status == "NEW": new_status = "OPEN"
        else: new_status = current_status

        update_payload = {
            "current_price": round(current, 4),
            "pnl_pct":       pnl_pct,
            "status":        new_status,
            "last_updated":  utc_now(),
        }

        try:
            doc.reference.update(update_payload)
            updated_count += 1
        except Exception:
            pass
            
        time.sleep(1)  # ป้องกันการโดน Yahoo บล็อก (Error 401)

    avg_pnl = round(sum(cumulative_pnl_list) / len(cumulative_pnl_list), 4) if cumulative_pnl_list else 0.0

    try:
        summary = {
            "total_positions":  len(docs),
            "updated_positions": updated_count,
            "cumulative_pnl_pct": avg_pnl,
            "last_run": utc_now(),
        }
        DB.collection("users").document(USER_UID).set({"portfolio_summary": summary}, merge=True)
    except Exception:
        pass

    agent_log(8, "UPDATER", f"Update cycle complete. Cumulative PnL={avg_pnl:+.2f}%")

# ══════════════════════════════════════════════════════════════════════
#  FULL PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════
def run_pipeline():
    log.info("=" * 70)
    log.info("  AI TRADING AGENCY HQ  —  PIPELINE START  —  %s", utc_now())
    log.info("=" * 70)

    existing_count = 0
    try:
        port_ref = DB.collection("users").document(USER_UID).collection("portfolio")
        open_docs = list(port_ref.where("status", "in", ["NEW","OPEN","TRAIL"]).stream())
        existing_count = len(open_docs)
    except Exception:
        pass

    if existing_count >= MAX_POSITIONS:
        log.info("Max positions reached (%d). Skipping new signal generation.", MAX_POSITIONS)
    else:
        universe = get_rising_star_tickers() if CURRENT_MARKET_MODE == "STOCKS" else get_crypto_tickers()
        candidates = agent1_scanner(universe)
        if candidates:
            confirmed = agent2_factor_analyst(candidates)
            if confirmed:
                sized = agent3_risk_manager(confirmed, existing_count)
                if sized:
                    planned = agent4_setup_planner(sized)
                    if planned:
                        if TRADE_MODE == "SWING":
                            agent5_trailing_monitor()
                        signals = agent6_notifier(planned)
                        if signals:
                            agent7_logger(signals)

    agent8_portfolio_updater()

    log.info("=" * 70)
    log.info("  PIPELINE COMPLETE  —  %s", utc_now())
    log.info("=" * 70)

# ══════════════════════════════════════════════════════════════════════
#  SCHEDULER 
# ══════════════════════════════════════════════════════════════════════
def schedule_jobs():
    schedule.every(4).hours.do(run_pipeline)
    schedule.every(1).minutes.do(agent8_portfolio_updater)
    schedule.every(5).minutes.do(agent5_trailing_monitor)
    log.info("Scheduler configured: Pipeline (4h), Portfolio Update (1m), Trail (5m)")

# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("AI Trading Agency HQ — Backend starting")
    
    if not USER_UID:
        log.error("USER_UID not set in .env! Firestore writes will fail. Exiting.")
        raise SystemExit(1)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    log.info("Flask API started on http://0.0.0.0:5000")

    run_pipeline()
    schedule_jobs()

    log.info("Entering scheduler loop (Ctrl+C to stop)…")
    while True:
        schedule.run_pending()
        time.sleep(30)