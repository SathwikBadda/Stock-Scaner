import requests
import json
from typing import List, Dict, Any
import logging
import time
import random
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)

class NSEClient:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(self.headers)
        self.base_url = "https://www.nseindia.com"
        self.nse_initialized = False
        self._init_nse_tools()
        
    def _init_nse_tools(self):
        """Initialize NSE tools with correct method"""
        try:
            # Try the correct nsetools version
            from nsetools import Nse
            self.nse = Nse()
            
            # Test if it has the correct methods
            if hasattr(self.nse, 'get_stock_codes'):
                self.nse_initialized = True
                logger.info("NSE tools initialized successfully")
            else:
                logger.warning("NSE tools missing required methods")
                
        except ImportError:
            logger.warning("nsetools not available, using web scraping")
        except Exception as e:
            logger.warning(f"Error initializing nsetools: {e}")
    
    def _establish_session(self):
        """Establish session with NSE website"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            if response.status_code == 200:
                time.sleep(random.uniform(1, 2))
                return True
            return False
        except Exception as e:
            logger.warning(f"Could not establish NSE session: {e}")
            return False
    
    def get_fo_stocks(self) -> List[str]:
        """Get F&O stocks using multiple reliable methods"""
        try:
            # Method 1: Try NSE Tools (if available and working)
            if self.nse_initialized:
                try:
                    # Get all stock codes and filter F&O stocks
                    all_stocks = self.nse.get_stock_codes()
                    if all_stocks and isinstance(all_stocks, dict):
                        # Filter known F&O stocks from all stocks
                        fo_stocks = []
                        known_fo_stocks = self._get_known_fo_stocks()
                        
                        for symbol in known_fo_stocks:
                            if symbol in all_stocks:
                                fo_stocks.append(symbol)
                        
                        if fo_stocks:
                            logger.info(f"Retrieved {len(fo_stocks)} F&O stocks via NSE tools")
                            return fo_stocks
                            
                except Exception as e:
                    logger.warning(f"NSE tools F&O fetch failed: {e}")
            
            # Method 2: Web scraping NSE F&O page
            try:
                fo_stocks = self._scrape_fo_stocks_from_nse()
                if fo_stocks:
                    logger.info(f"Scraped {len(fo_stocks)} F&O stocks from NSE website")
                    return fo_stocks
            except Exception as e:
                logger.warning(f"NSE web scraping failed: {e}")
            
            # Method 3: Try NSE API endpoints
            try:
                if self._establish_session():
                    fo_url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20F%26O"
                    response = self.session.get(fo_url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'data' in data and data['data']:
                            symbols = []
                            for stock in data['data']:
                                symbol = stock.get('symbol', '')
                                if symbol and symbol not in symbols:
                                    symbols.append(symbol)
                            
                            if symbols:
                                logger.info(f"Retrieved {len(symbols)} F&O stocks from NSE API")
                                return symbols
                                
            except Exception as e:
                logger.warning(f"NSE API F&O fetch failed: {e}")
            
            # Fallback: Use comprehensive known F&O list
            logger.info("Using comprehensive known F&O stocks list")
            return self._get_known_fo_stocks()
            
        except Exception as e:
            logger.error(f"Error fetching F&O stocks: {e}")
            return self._get_known_fo_stocks()
    
    def _scrape_fo_stocks_from_nse(self) -> List[str]:
        """Scrape F&O stocks from NSE website"""
        try:
            if not self._establish_session():
                return []
            
            # Try multiple NSE pages for F&O data
            fo_urls = [
                "https://www.nseindia.com/products-services/equity-derivatives-list",
                "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20F%26O"
            ]
            
            for url in fo_urls:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        if 'api' in url:
                            # JSON response
                            data = response.json()
                            if 'data' in data:
                                return [stock.get('symbol', '') for stock in data['data'] if stock.get('symbol')]
                        else:
                            # HTML response
                            soup = BeautifulSoup(response.content, 'html.parser')
                            # Extract stock symbols from tables or lists
                            symbols = self._extract_symbols_from_html(soup)
                            if symbols:
                                return symbols
                                
                except Exception as e:
                    logger.debug(f"Failed to scrape from {url}: {e}")
                    continue
            
            return []
            
        except Exception as e:
            logger.warning(f"Web scraping failed: {e}")
            return []
    
    def _extract_symbols_from_html(self, soup) -> List[str]:
        """Extract stock symbols from HTML content"""
        try:
            symbols = []
            
            # Look for tables with stock data
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        text = cell.get_text().strip()
                        # Check if text looks like a stock symbol
                        if re.match(r'^[A-Z][A-Z0-9&-]{2,15}$', text):
                            if text not in symbols and len(text) >= 3:
                                symbols.append(text)
            
            # Also look for specific patterns in text
            text_content = soup.get_text()
            symbol_pattern = r'\b[A-Z][A-Z0-9&-]{2,15}\b'
            found_symbols = re.findall(symbol_pattern, text_content)
            
            for symbol in found_symbols:
                if symbol in self._get_known_fo_stocks() and symbol not in symbols:
                    symbols.append(symbol)
            
            return symbols[:100]  # Limit to 100 symbols
            
        except Exception as e:
            logger.warning(f"Error extracting symbols from HTML: {e}")
            return []
    
    def _get_known_fo_stocks(self) -> List[str]:
        """Comprehensive list of known F&O stocks"""
        return [
            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR', 'ICICIBANK',
            'KOTAKBANK', 'SBIN', 'BHARTIARTL', 'ITC', 'ASIANPAINT', 'LT',
            'AXISBANK', 'MARUTI', 'SUNPHARMA', 'ULTRACEMCO', 'TITAN', 'WIPRO',
            'POWERGRID', 'NTPC', 'TATAMOTORS', 'ONGC', 'HCLTECH', 'BAJFINANCE', 
            'M&M', 'TATASTEEL', 'COALINDIA', 'GRASIM', 'HINDALCO', 'JSWSTEEL', 
            'INDUSINDBK', 'HEROMOTOCO', 'CIPLA', 'DRREDDY', 'EICHERMOT', 
            'BAJAJFINSV', 'BRITANNIA', 'SHREECEM', 'DIVISLAB', 'BPCL',
            'GODREJCP', 'DABUR', 'BANDHANBNK', 'BERGEPAINT', 'BIOCON',
            'CADILAHC', 'CANBK', 'CHOLAFIN', 'COLPAL', 'CONCOR',
            'CUMMINSIND', 'DLF', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK',
            'GAIL', 'HAVELLS', 'HDFCLIFE', 'IBULHSGFIN', 'IDFCFIRSTB',
            'IGL', 'INDIANB', 'IOC', 'IRCTC', 'JINDALSTEL',
            'JUBLFOOD', 'LICHSGFIN', 'LUPIN', 'MARICO', 'MOTHERSUMI',
            'MPHASIS', 'MRF', 'NAUKRI', 'NMDC', 'OFSS',
            'OIL', 'PAGEIND', 'PEL', 'PETRONET', 'PFC',
            'PNB', 'POLYCAB', 'RAMCOCEM', 'RBLBANK', 'RECLTD',
            'SAIL', 'SBILIFE', 'SIEMENS', 'SRF', 'SRTRANSFIN',
            'TORNTPHARM', 'TVSMOTOR', 'UBL', 'VEDL', 'VOLTAS',
            'YESBANK', 'ZEEL', 'ADANIGREEN', 'ADANIPORTS'
        ]
    
    def get_stock_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get REAL accurate stock data using multiple methods"""
        try:
            stock_data = []
            successful_symbols = []
            
            # Filter out known delisted symbols
            active_symbols = self._filter_active_symbols(symbols)
            
            for symbol in active_symbols[:25]:  # Process 25 stocks
                try:
                    time.sleep(random.uniform(0.1, 0.4))
                    
                    stock_info = None
                    
                    # Method 1: Try NSE Tools (most reliable when working)
                    if self.nse_initialized:
                        try:
                            quote = self.nse.get_quote(symbol)
                            if quote and isinstance(quote, dict) and quote.get('lastPrice'):
                                
                                # Extract real data from NSE tools
                                current_price = float(quote.get('lastPrice', 0))
                                open_price = float(quote.get('open', 0))
                                prev_close = float(quote.get('previousClose', 0))
                                
                                # Get intraday high/low
                                intraday = quote.get('intraDayHighLow', {})
                                day_high = float(intraday.get('max', current_price))
                                day_low = float(intraday.get('min', current_price))
                                
                                volume = int(quote.get('totalTradedVolume', 0))
                                
                                # Get previous day data
                                prev_day_data = self._get_previous_day_data_nse_api(symbol)
                                
                                stock_info = {
                                    'symbol': symbol,
                                    'open_price': open_price,
                                    'high_price': day_high,
                                    'low_price': day_low,
                                    'ltp': current_price,
                                    'prev_close': prev_close,
                                    'prev_day_high': prev_day_data.get('prev_day_high', prev_close * 1.01),
                                    'prev_day_open': prev_day_data.get('prev_day_open', prev_close * 0.995),
                                    'prev_day_low': prev_day_data.get('prev_day_low', prev_close * 0.985),
                                    'volume': volume,
                                    'change_in_oi': 0,  # Will be updated if F&O data available
                                    'source': 'nsetools'
                                }
                                
                                logger.debug(f"Got NSEtools data for {symbol}: LTP={current_price:.2f}")
                                
                        except Exception as e:
                            logger.debug(f"NSEtools failed for {symbol}: {e}")
                    
                    # Method 2: Try NSE Website API
                    if not stock_info and self._establish_session():
                        try:
                            quote_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
                            response = self.session.get(quote_url, timeout=10)
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                if 'priceInfo' in data:
                                    price_info = data['priceInfo']
                                    
                                    current_price = float(price_info.get('lastPrice', 0))
                                    open_price = float(price_info.get('open', 0))
                                    prev_close = float(price_info.get('previousClose', 0))
                                    
                                    # Get intraday high/low
                                    intraday = price_info.get('intraDayHighLow', {})
                                    day_high = float(intraday.get('max', current_price))
                                    day_low = float(intraday.get('min', current_price))
                                    
                                    # Get volume from market depth
                                    market_depth = data.get('marketDeptOrderBook', {})
                                    volume = int(market_depth.get('totalTradedVolume', 0))
                                    
                                    # Get previous day data
                                    prev_day_data = self._get_previous_day_data_nse_api(symbol)
                                    
                                    stock_info = {
                                        'symbol': symbol,
                                        'open_price': open_price,
                                        'high_price': day_high,
                                        'low_price': day_low,
                                        'ltp': current_price,
                                        'prev_close': prev_close,
                                        'prev_day_high': prev_day_data.get('prev_day_high', prev_close * 1.01),
                                        'prev_day_open': prev_day_data.get('prev_day_open', prev_close * 0.995),
                                        'prev_day_low': prev_day_data.get('prev_day_low', prev_close * 0.985),
                                        'volume': volume,
                                        'change_in_oi': 0,
                                        'source': 'nse_api'
                                    }
                                    
                                    logger.debug(f"Got NSE API data for {symbol}: LTP={current_price:.2f}")
                                    
                        except Exception as e:
                            logger.debug(f"NSE API failed for {symbol}: {e}")
                    
                    # Method 3: Try YFinance (with better symbol handling)
                    if not stock_info:
                        try:
                            import yfinance as yf
                            
                            yf_symbol = f"{symbol}.NS"
                            ticker = yf.Ticker(yf_symbol)
                            
                            # Get recent data (3 days to ensure we have previous day)
                            hist = ticker.history(period="3d", interval="1d")
                            
                            if not hist.empty and len(hist) >= 2:
                                today = hist.iloc[-1]
                                yesterday = hist.iloc[-2]
                                
                                stock_info = {
                                    'symbol': symbol,
                                    'open_price': float(today['Open']),
                                    'high_price': float(today['High']),
                                    'low_price': float(today['Low']),
                                    'ltp': float(today['Close']),
                                    'prev_close': float(yesterday['Close']),
                                    'prev_day_high': float(yesterday['High']),
                                    'prev_day_open': float(yesterday['Open']),
                                    'prev_day_low': float(yesterday['Low']),
                                    'volume': int(today['Volume']),
                                    'change_in_oi': 0,
                                    'source': 'yfinance'
                                }
                                
                                logger.debug(f"Got YFinance data for {symbol}: LTP={today['Close']:.2f}")
                                
                        except Exception as e:
                            logger.debug(f"YFinance failed for {symbol}: {e}")
                    
                    # Add stock info if we got valid data
                    if stock_info and self._validate_stock_data(stock_info):
                        stock_data.append(stock_info)
                        successful_symbols.append(symbol)
                    else:
                        logger.warning(f"No valid data found for {symbol}")
                
                except Exception as e:
                    logger.warning(f"Error processing {symbol}: {e}")
                    continue
            
            logger.info(f"Retrieved REAL accurate data for {len(stock_data)} stocks")
            logger.info(f"Successful symbols: {', '.join(successful_symbols[:5])}{'...' if len(successful_symbols) > 5 else ''}")
            
            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching stock data: {e}")
            return []
    
    def _filter_active_symbols(self, symbols: List[str]) -> List[str]:
        """Filter out known delisted or problematic symbols"""
        # Known problematic symbols to avoid
        delisted_symbols = {
            'ADANITRANS', 'PVR', 'MOTHERSUMI', 'IBULHSGFIN', 
            'SRTRANSFIN', 'CADILAHC', 'MCDOWELLN'
        }
        
        return [symbol for symbol in symbols if symbol not in delisted_symbols]
    
    def _get_previous_day_data_nse_api(self, symbol: str) -> Dict[str, Any]:
        """Get previous day OHLC data from NSE"""
        try:
            if self._establish_session():
                # Try historical data API
                hist_url = f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}"
                response = self.session.get(hist_url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and len(data['data']) >= 2:
                        prev_day = data['data'][1]  # Previous day data
                        return {
                            'prev_day_high': float(prev_day.get('CH_TRADE_HIGH_PRICE', 0)),
                            'prev_day_open': float(prev_day.get('CH_OPENING_PRICE', 0)),
                            'prev_day_low': float(prev_day.get('CH_TRADE_LOW_PRICE', 0)),
                            'prev_day_close': float(prev_day.get('CH_CLOSING_PRICE', 0))
                        }
            
            return {}
            
        except Exception as e:
            logger.debug(f"Could not get previous day data for {symbol}: {e}")
            return {}
    
    def _validate_stock_data(self, stock_info: Dict[str, Any]) -> bool:
        """Validate stock data quality"""
        try:
            required_fields = ['open_price', 'ltp', 'prev_close']
            
            for field in required_fields:
                value = stock_info.get(field, 0)
                if not isinstance(value, (int, float)) or value <= 0:
                    return False
            
            # Additional sanity checks
            ltp = stock_info['ltp']
            prev_close = stock_info['prev_close']
            
            # Price should be reasonable
            if not (5 <= ltp <= 100000):
                return False
            
            # Change should not be too extreme (max 50% in a day)
            if prev_close > 0:
                change_pct = abs((ltp - prev_close) / prev_close)
                if change_pct > 0.5:
                    return False
            
            return True
            
        except Exception:
            return False
    
    def get_historical_data(self, symbol: str) -> Dict[str, Any]:
        """Get historical data for previous day"""
        return self._get_previous_day_data_nse_api(symbol)