import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Fyers API Configuration
    FYERS_APP_ID = os.getenv('FYERS_APP_ID')
    FYERS_SECRET_KEY = os.getenv('FYERS_SECRET_KEY')
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN')
    
    # Telegram Configuration
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    
    # Database Configuration
    DATABASE_PATH = os.getenv('DATABASE_PATH', './database/stock_screener.db')
    
    # Strategy Configuration
    MIN_PERCENTAGE_INCREASE = float(os.getenv('MIN_PERCENTAGE_INCREASE', 7.0))
    SCAN_INTERVAL_MINUTES = int(os.getenv('SCAN_INTERVAL_MINUTES', 5))
    
    # NSE URLs
    NSE_BASE_URL = "https://www.nseindia.com"
    NSE_FO_URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20F%26O"
    NSE_SPROUTS_URL = "https://www.nseindia.com/api/market-data-pre-open?key=ALL"
    
    # Headers for NSE requests
    NSE_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', './logs/stock_screener.log')

settings = Settings()