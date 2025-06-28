#!/usr/bin/env python3
"""
Minimal NIFTY 50 Real Data Scraper & Telegram Bot
Scrapes open, high, low, close, prev high, prev close, OI change% for NIFTY 50 from NSE and sends to Telegram.
"""
import os
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import bs4
import time
import yfinance as yf
from agents.filter_agent import FilterAgent
import schedule

# --- CONFIG ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
NSE_URL = "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# --- SCRAPER ---
def fetch_nifty50_data() -> list:
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050",
        "Connection": "keep-alive",
        "DNT": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Des": "empty",
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=10)
    time.sleep(1)
    r = session.get(url, headers=headers, timeout=10)
    stocks = []
    if r.status_code == 200:
        try:
            data = r.json()
            for stock in data.get("data", []):
                def safe_round(val):
                    try:
                        return round(float(val), 2)
                    except Exception:
                        return val
                symbol = stock.get("symbol", "")
                open_ = safe_round(stock.get("open", ""))
                high = safe_round(stock.get("dayHigh", ""))
                low = safe_round(stock.get("dayLow", ""))
                ltp = safe_round(stock.get("lastPrice", ""))
                prev_high = safe_round(stock.get("previousHigh", ""))
                prev_low = safe_round(stock.get("previousLow", ""))
                prev_close = safe_round(stock.get("previousClose", ""))
                oi = stock.get("openInterest")
                oi_change = stock.get("changeinOpenInterest")
                oi_change_pct = stock.get("pchangeinOpenInterest")
                # Always fetch from option chain for best OI data
                oi_opt, oi_change_opt, oi_change_pct_opt = fetch_oi_full_from_option_chain(symbol)
                # Prefer option chain values if available, else fallback to stockIndices
                oi_final = safe_round(oi_opt) if oi_opt is not None else safe_round(oi) if oi is not None else "N/A"
                oi_change_final = safe_round(oi_change_opt) if oi_change_opt is not None else safe_round(oi_change) if oi_change is not None else "N/A"
                oi_change_pct_final = safe_round(oi_change_pct_opt) if oi_change_pct_opt is not None else safe_round(oi_change_pct) if oi_change_pct is not None else "N/A"
                stocks.append({
                    "symbol": symbol,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "ltp": ltp,
                    "close": ltp,  # For compatibility
                    "prev_high": prev_high,
                    "prev_low": prev_low,
                    "prev_close": prev_close,
                    "oi": oi_final,
                    "oi_change": oi_change_final,
                    "oi_change_pct": oi_change_pct_final,
                })
        except Exception as e:
            logger.error(f"NSE API JSON decode error: {e}")
    if stocks:
        logger.info(f"Fetched {len(stocks)} NIFTY 50 stocks from NSE API.")
        return stocks
    logger.warning("Falling back to Yahoo Finance for NIFTY 50 data.")
    return []

def fetch_oi_full_from_option_chain(symbol):
    url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.nseindia.com/option-chain?symbol={symbol}",
        "Connection": "keep-alive",
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=10)
    time.sleep(1)
    r = session.get(url, headers=headers, timeout=10)
    if r.status_code != 200:
        return None, None, None
    try:
        data = r.json()
        total_oi = 0
        total_oi_change = 0
        for record in data.get("records", {}).get("data", []):
            ce = record.get("CE")
            pe = record.get("PE")
            if ce:
                total_oi += ce.get("openInterest", 0)
                total_oi_change += ce.get("changeinOpenInterest", 0)
            if pe:
                total_oi += pe.get("openInterest", 0)
                total_oi_change += pe.get("changeinOpenInterest", 0)
        oi_change_pct = None
        if total_oi != 0:
            oi_change_pct = round((total_oi_change / total_oi) * 100, 2)
        return total_oi, total_oi_change, oi_change_pct
    except Exception:
        return None, None, None

# --- TELEGRAM ---
def send_telegram_message(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not set.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_length = 4096
    success = True
    # Split message into chunks
    for i in range(0, len(message), max_length):
        chunk = message[i:i+max_length]
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': chunk,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200 and r.json().get('ok'):
                logger.info("Telegram message chunk sent.")
            else:
                logger.error(f"Telegram error: {r.text}")
                success = False
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            success = False
    return success

# --- MAIN ---
def main():
    stocks_50 = fetch_nifty50_data()
    if not stocks_50:
        send_telegram_message("❌ Failed to fetch NIFTY 50 data from NSE.")
        return
    filter_agent = FilterAgent()
    filtered_stocks = filter_agent.filter_buy_sell_signals(stocks_50)
    if not filtered_stocks:
        send_telegram_message("No BUY or SELL signals for NIFTY 50 stocks today.")
        return
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for s in filtered_stocks:
        msg = (f"<b>{s['signal']} SIGNAL</b>\n{now}\n\n"
               f"<b>{s['symbol']}</b> | Open: {s['open']} | High: {s['high']} | Low: {s['low']} | "
               f"LTP: {s['ltp']} | Prev High: {s['prev_high']} | Prev Low: {s['prev_low']} | Prev Close: {s['prev_close']} | "
               f"OI: {s['oi']} | OI Δ: {s['oi_change']} | OI Δ%: {s['oi_change_pct']}")
        send_telegram_message(msg)

def run_every_5min():
    try:
        main()
    except Exception as e:
        print(f"Scheduled run error: {e}")

if __name__ == "__main__":
    schedule.every(5).minutes.do(run_every_5min)
    print("NIFTY 50 screener started. Running every 5 minutes...")
    run_every_5min()  # Run once at start
    while True:
        schedule.run_pending()
        time.sleep(1)