import requests
import json
from typing import List, Dict, Any
import logging
import time
import random
from datetime import datetime, timedelta
from urllib.parse import quote
import warnings
import re

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

logger = logging.getLogger(__name__)

class RobustNSEScraper:
    """Ultra-robust NSE scraper that bypasses all security measures"""
    
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://www.nseindia.com"
        self.cookies_established = False
        self.last_session_time = 0
        
        # Rotating user agents for better stealth
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        # Enhanced headers that mimic real browser
        self.base_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        
        self._update_headers()
        logger.info("🔧 Robust NSE Scraper initialized")
    
    def _update_headers(self):
        """Update headers with random user agent"""
        self.session.headers.clear()
        self.session.headers.update(self.base_headers)
        self.session.headers['User-Agent'] = random.choice(self.user_agents)
    
    def _establish_robust_session(self) -> bool:
        """Establish robust NSE session with multiple fallback methods"""
        try:
            current_time = time.time()
            
            # Re-establish session every 5 minutes
            if self.cookies_established and (current_time - self.last_session_time) < 300:
                return True
            
            logger.debug("🔓 Establishing robust NSE session...")
            
            # Method 1: Standard homepage approach
            if self._try_homepage_session():
                self.cookies_established = True
                self.last_session_time = current_time
                return True
            
            # Method 2: Alternative entry points
            entry_points = [
                "/",
                "/market-data/live-equity-market",
                "/get-quotes/equity",
                "/companies-listing/corporate-filings-announcements"
            ]
            
            for entry_point in entry_points:
                try:
                    self._update_headers()  # Rotate user agent
                    url = f"{self.base_url}{entry_point}"
                    
                    response = self.session.get(url, timeout=15, allow_redirects=True)
                    
                    if response.status_code == 200:
                        # Test API access
                        if self._test_api_access():
                            logger.debug(f"✅ Session established via {entry_point}")
                            self.cookies_established = True
                            self.last_session_time = current_time
                            return True
                    
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.debug(f"⚠️ Entry point {entry_point} failed: {e}")
                    continue
            
            # Method 3: Direct API warmup
            return self._direct_api_warmup()
            
        except Exception as e:
            logger.debug(f"⚠️ Session establishment failed: {e}")
            return False
    
    def _try_homepage_session(self) -> bool:
        """Try standard homepage session establishment"""
        try:
            response = self.session.get(self.base_url, timeout=15)
            
            if response.status_code == 200:
                # Simulate human behavior
                time.sleep(random.uniform(2, 4))
                
                # Test API access
                return self._test_api_access()
            
            return False
            
        except Exception as e:
            logger.debug(f"⚠️ Homepage session failed: {e}")
            return False
    
    def _test_api_access(self) -> bool:
        """Test if API access is working"""
        test_endpoints = [
            "/api/allIndices",
            "/api/equity-stockIndices?index=NIFTY%2050"
        ]
        
        for endpoint in test_endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        logger.debug("✅ API access confirmed")
                        return True
                
            except Exception:
                continue
        
        return False
    
    def _direct_api_warmup(self) -> bool:
        """Direct API warmup without homepage"""
        try:
            # Update headers for API calls
            api_headers = self.base_headers.copy()
            api_headers.update({
                'Accept': 'application/json, text/plain, */*',
                'Referer': f'{self.base_url}/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin'
            })
            
            self.session.headers.update(api_headers)
            
            # Test with a simple API call
            response = self.session.get(f"{self.base_url}/api/allIndices", timeout=10)
            
            if response.status_code == 200:
                logger.debug("✅ Direct API access successful")
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"⚠️ Direct API warmup failed: {e}")
            return False
    
    def get_fo_stocks_robust(self) -> List[str]:
        """Get F&O stocks using multiple robust methods"""
        try:
            all_symbols = set()
            
            if not self._establish_robust_session():
                logger.warning("⚠️ Could not establish NSE session, using fallback list")
                return self._get_comprehensive_fo_list()
            
            # Method 1: F&O Index endpoint
            try:
                url = f"{self.base_url}/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        for item in data['data']:
                            symbol = item.get('symbol', '').strip()
                            if symbol and len(symbol) >= 2:
                                all_symbols.add(symbol)
                        logger.info(f"✅ F&O Index: {len(all_symbols)} symbols")
                
            except Exception as e:
                logger.debug(f"⚠️ F&O index failed: {e}")
            
            # Method 2: Market data pre-open
            try:
                time.sleep(1)
                url = f"{self.base_url}/api/market-data-pre-open?key=FO"
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        for item in data['data']:
                            if 'metadata' in item:
                                symbol = item['metadata'].get('symbol', '').strip()
                            else:
                                symbol = item.get('symbol', '').strip()
                            
                            if symbol and len(symbol) >= 2:
                                all_symbols.add(symbol)
                
            except Exception as e:
                logger.debug(f"⚠️ Pre-open F&O failed: {e}")
            
            # Method 3: Alternative indices
            indices = [
                "NIFTY%20F%26O",
                "NIFTY%20500", 
                "NIFTY%20200"
            ]
            
            for index in indices:
                try:
                    time.sleep(1)
                    url = f"{self.base_url}/api/equity-stockIndices?index={index}"
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'data' in data:
                            for item in data['data']:
                                symbol = item.get('symbol', '').strip()
                                if symbol and len(symbol) >= 2:
                                    # Filter only known F&O stocks
                                    if symbol in self._get_comprehensive_fo_list():
                                        all_symbols.add(symbol)
                    
                except Exception as e:
                    logger.debug(f"⚠️ Index {index} failed: {e}")
                    continue
            
            if all_symbols:
                symbols_list = list(all_symbols)
                logger.info(f"✅ Total F&O symbols collected: {len(symbols_list)}")
                return symbols_list
            else:
                logger.warning("⚠️ No symbols from NSE, using comprehensive list")
                return self._get_comprehensive_fo_list()
            
        except Exception as e:
            logger.error(f"❌ F&O stocks fetch failed: {e}")
            return self._get_comprehensive_fo_list()
    
    def get_stock_data_robust(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get real stock data using multiple robust methods"""
        try:
            if not self._establish_robust_session():
                logger.error("❌ Cannot establish NSE session for data fetch")
                return []
            
            real_stock_data = []
            success_count = 0
            
            # Process symbols in batches
            batch_size = 5
            for i in range(0, min(len(symbols), 30), batch_size):
                batch = symbols[i:i + batch_size]
                
                for symbol in batch:
                    try:
                        time.sleep(random.uniform(0.3, 0.8))  # Rate limiting
                        
                        stock_data = self._get_individual_stock_data(symbol)
                        
                        if stock_data and self._validate_real_stock_data(stock_data):
                            real_stock_data.append(stock_data)
                            success_count += 1
                            logger.debug(f"✅ {symbol}: ₹{stock_data['ltp']:.2f}")
                        
                    except Exception as e:
                        logger.debug(f"⚠️ {symbol} failed: {e}")
                        continue
                
                # Pause between batches
                if i + batch_size < len(symbols):
                    time.sleep(random.uniform(1, 2))
            
            logger.info(f"✅ Real data fetched for {success_count} stocks")
            return real_stock_data
            
        except Exception as e:
            logger.error(f"❌ Stock data fetch failed: {e}")
            return []
    
    def _get_individual_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Get individual stock data using multiple endpoints"""
        
        # Method 1: Quote equity endpoint
        try:
            url = f"{self.base_url}/api/quote-equity?symbol={quote(symbol)}"
            response = self.session.get(url, timeout=12)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'priceInfo' in data:
                    stock_data = self._parse_quote_equity_data(symbol, data)
                    if stock_data:
                        return stock_data
            
        except Exception as e:
            logger.debug(f"⚠️ Quote equity failed for {symbol}: {e}")
        
        # Method 2: Search and get quote
        try:
            search_data = self._search_symbol(symbol)
            if search_data:
                quote_data = self._get_quote_from_search(search_data)
                if quote_data:
                    return quote_data
            
        except Exception as e:
            logger.debug(f"⚠️ Search method failed for {symbol}: {e}")
        
        # Method 3: Alternative quote methods
        try:
            return self._get_alternative_quote(symbol)
            
        except Exception as e:
            logger.debug(f"⚠️ Alternative quote failed for {symbol}: {e}")
        
        return None
    
    def _parse_quote_equity_data(self, symbol: str, data: Dict) -> Dict[str, Any]:
        """Parse quote equity response"""
        try:
            price_info = data['priceInfo']
            
            # Current prices
            ltp = float(price_info.get('lastPrice', 0))
            open_price = float(price_info.get('open', 0))
            prev_close = float(price_info.get('previousClose', 0))
            
            # Intraday high/low
            intraday = price_info.get('intraDayHighLow', {})
            day_high = float(intraday.get('max', ltp))
            day_low = float(intraday.get('min', ltp))
            
            # Volume
            volume = 0
            if 'marketDeptOrderBook' in data:
                volume = int(data['marketDeptOrderBook'].get('totalTradedVolume', 0))
            
            # Get historical data for previous day
            hist_data = self._get_historical_data_robust(symbol)
            
            stock_data = {
                'symbol': symbol,
                'ltp': ltp,
                'open_price': open_price,
                'high_price': day_high,
                'low_price': day_low,
                'prev_close': prev_close,
                'prev_day_high': hist_data.get('prev_day_high', prev_close),
                'prev_day_open': hist_data.get('prev_day_open', prev_close),
                'prev_day_low': hist_data.get('prev_day_low', prev_close),
                'volume': volume,
                'change_in_oi': 0,
                'total_oi': 0,
                'source': 'nse_quote_equity_real'
            }
            
            # Try to get F&O data
            fo_data = self._get_fo_data_robust(symbol)
            if fo_data:
                stock_data.update(fo_data)
            
            return stock_data
            
        except Exception as e:
            logger.debug(f"⚠️ Parse quote equity failed for {symbol}: {e}")
            return None
    
    def _search_symbol(self, symbol: str) -> Dict:
        """Search for symbol to get exact match"""
        try:
            url = f"{self.base_url}/api/search/autocomplete?q={quote(symbol)}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'symbols' in data:
                    for item in data['symbols']:
                        if item.get('symbol', '').upper() == symbol.upper():
                            return item
                            
        except Exception as e:
            logger.debug(f"⚠️ Symbol search failed for {symbol}: {e}")
        
        return None
    
    def _get_quote_from_search(self, search_data: Dict) -> Dict[str, Any]:
        """Get quote data from search result"""
        try:
            symbol = search_data.get('symbol', '')
            if not symbol:
                return None
            
            # Try different quote endpoints
            endpoints = [
                f"/api/quote-equity?symbol={quote(symbol)}",
                f"/api/quote-equity?symbol={quote(symbol)}&section=trade_info"
            ]
            
            for endpoint in endpoints:
                try:
                    url = f"{self.base_url}{endpoint}"
                    response = self.session.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        return self._parse_quote_equity_data(symbol, data)
                
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"⚠️ Quote from search failed: {e}")
            return None
    
    def _get_alternative_quote(self, symbol: str) -> Dict[str, Any]:
        """Alternative quote methods"""
        try:
            # Method: Check if symbol is in market data
            url = f"{self.base_url}/api/equity-stockIndices?index=NIFTY%20500"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data:
                    for item in data['data']:
                        if item.get('symbol', '').upper() == symbol.upper():
                            return self._parse_market_index_data(symbol, item)
            
            return None
            
        except Exception as e:
            logger.debug(f"⚠️ Alternative quote failed for {symbol}: {e}")
            return None
    
    def _parse_market_index_data(self, symbol: str, data: Dict) -> Dict[str, Any]:
        """Parse market index data"""
        try:
            ltp = float(data.get('lastPrice', 0))
            open_price = float(data.get('open', 0))
            prev_close = float(data.get('previousClose', 0))
            day_high = float(data.get('dayHigh', ltp))
            day_low = float(data.get('dayLow', ltp))
            volume = int(data.get('totalTradedVolume', 0))
            
            # Get historical data
            hist_data = self._get_historical_data_robust(symbol)
            
            return {
                'symbol': symbol,
                'ltp': ltp,
                'open_price': open_price,
                'high_price': day_high,
                'low_price': day_low,
                'prev_close': prev_close,
                'prev_day_high': hist_data.get('prev_day_high', prev_close),
                'prev_day_open': hist_data.get('prev_day_open', prev_close),
                'prev_day_low': hist_data.get('prev_day_low', prev_close),
                'volume': volume,
                'change_in_oi': 0,
                'total_oi': 0,
                'source': 'nse_market_index_real'
            }
            
        except Exception as e:
            logger.debug(f"⚠️ Parse market index failed for {symbol}: {e}")
            return None
    
    def _get_historical_data_robust(self, symbol: str) -> Dict[str, Any]:
        """Get historical data with multiple fallback methods"""
        try:
            # Method 1: Historical CM equity endpoint
            end_date = datetime.now()
            start_date = end_date - timedelta(days=10)
            
            date_format = "%d-%m-%Y"
            from_date = start_date.strftime(date_format)
            to_date = end_date.strftime(date_format)
            
            url = f"{self.base_url}/api/historical/cm/equity?symbol={quote(symbol)}&series=[%22EQ%22]&from={from_date}&to={to_date}"
            
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and len(data['data']) >= 2:
                    # Get previous trading day data
                    prev_day = data['data'][-2]
                    
                    return {
                        'prev_day_high': float(prev_day.get('CH_TRADE_HIGH_PRICE', 0)),
                        'prev_day_open': float(prev_day.get('CH_OPENING_PRICE', 0)),
                        'prev_day_low': float(prev_day.get('CH_TRADE_LOW_PRICE', 0)),
                        'prev_day_close': float(prev_day.get('CH_CLOSING_PRICE', 0))
                    }
            
        except Exception as e:
            logger.debug(f"⚠️ Historical data failed for {symbol}: {e}")
        
        return {}
    
    def _get_fo_data_robust(self, symbol: str) -> Dict[str, Any]:
        """Get F&O data including OI with robust methods"""
        try:
            # F&O quote endpoint
            url = f"{self.base_url}/api/quote-derivative?symbol={quote(symbol)}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse F&O data from different structures
                if 'stocks' in data:
                    for stock_info in data['stocks']:
                        if 'metadata' in stock_info:
                            metadata = stock_info['metadata']
                            
                            if 'Futures' in metadata.get('instrumentType', ''):
                                oi = int(metadata.get('openInterest', 0))
                                prev_oi = int(metadata.get('prevOI', 0))
                                change_oi = oi - prev_oi if prev_oi > 0 else 0
                                
                                if oi > 0:
                                    return {
                                        'total_oi': oi,
                                        'change_in_oi': change_oi
                                    }
                
                # Alternative structure
                if 'data' in data:
                    for item in data['data']:
                        if 'FUT' in item.get('instrumentType', '').upper():
                            oi = int(item.get('openInterest', 0))
                            prev_oi = int(item.get('prevOI', 0))
                            change_oi = oi - prev_oi if prev_oi > 0 else 0
                            
                            if oi > 0:
                                return {
                                    'total_oi': oi,
                                    'change_in_oi': change_oi
                                }
            
        except Exception as e:
            logger.debug(f"⚠️ F&O data failed for {symbol}: {e}")
        
        return {'total_oi': 0, 'change_in_oi': 0}
    
    def _validate_real_stock_data(self, stock_data: Dict[str, Any]) -> bool:
        """Validate that we have real stock data"""
        try:
            required_fields = ['symbol', 'ltp', 'prev_close']
            
            for field in required_fields:
                value = stock_data.get(field, 0)
                if not isinstance(value, (int, float)) or value <= 0:
                    return False
            
            # Additional validations
            ltp = stock_data['ltp']
            prev_close = stock_data['prev_close']
            
            # Price range check
            if not (1 <= ltp <= 100000):
                return False
            
            # Change check
            if prev_close > 0:
                change = abs((ltp - prev_close) / prev_close)
                if change > 0.5:  # Max 50% change
                    return False
            
            return True
            
        except Exception:
            return False
    
    def _get_comprehensive_fo_list(self) -> List[str]:
        """Comprehensive F&O stocks list as fallback"""
        return [
            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR', 'ICICIBANK',
            'KOTAKBANK', 'SBIN', 'BHARTIARTL', 'ITC', 'ASIANPAINT', 'LT',
            'AXISBANK', 'MARUTI', 'SUNPHARMA', 'ULTRACEMCO', 'TITAN', 'WIPRO',
            'POWERGRID', 'NTPC', 'TATAMOTORS', 'ONGC', 'HCLTECH', 'BAJFINANCE', 
            'M&M', 'TATASTEEL', 'COALINDIA', 'GRASIM', 'HINDALCO', 'JSWSTEEL', 
            'INDUSINDBK', 'HEROMOTOCO', 'CIPLA', 'DRREDDY', 'EICHERMOT', 
            'BAJAJFINSV', 'BRITANNIA', 'SHREECEM', 'DIVISLAB', 'BPCL',
            'GODREJCP', 'DABUR', 'BANDHANBNK', 'BERGEPAINT', 'BIOCON',
            'CANBK', 'CHOLAFIN', 'COLPAL', 'CONCOR', 'CUMMINSIND', 
            'DLF', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'GAIL', 
            'HAVELLS', 'HDFCLIFE', 'IDFCFIRSTB', 'IGL', 'INDIANB', 
            'IOC', 'IRCTC', 'JINDALSTEL', 'JUBLFOOD', 'LICHSGFIN', 
            'LUPIN', 'MARICO', 'MOTHERSUMI', 'MPHASIS', 'MRF', 
            'NAUKRI', 'NMDC', 'OFSS', 'OIL', 'PAGEIND', 
            'PEL', 'PETRONET', 'PFC', 'PNB', 'POLYCAB', 
            'RAMCOCEM', 'RBLBANK', 'RECLTD', 'SAIL', 'SBILIFE', 
            'SIEMENS', 'SRF', 'SRTRANSFIN', 'TORNTPHARM', 'TVSMOTOR', 
            'UBL', 'VEDL', 'VOLTAS', 'YESBANK', 'ZEEL',
            'ADANIGREEN', 'ADANIPORTS', 'AMBUJACEM', 'APOLLOHOSP',
            'ASHOKLEY', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA',
            'BALKRISIND', 'BANKINDIA', 'BATAINDIA', 'BEL', 'BHARATFORG',
            'BHEL', 'BOSCHLTD', 'BSOFT', 'CANFINHOME', 'CHAMBLFERT', 
            'COFORGE', 'COROMANDEL', 'CROMPTON', 'CUB', 'DEEPAKNTR', 
            'DELTACORP', 'DMART', 'GLENMARK', 'GNFC', 'GRANULES', 
            'GUJGASLTD', 'HINDPETRO', 'HONAUT', 'IBULHSGFIN', 'IDEA', 
            'IDFC', 'INDIGO', 'INDIACEM', 'INDUSTOWER', 'INTELLECT', 
            'IPCALAB', 'ISEC', 'JKCEMENT', 'JSWENERGY', 'JUSTDIAL', 
            'KPITTECH', 'LAURUSLABS', 'LICI', 'LTTS', 'MANAPPURAM', 
            'MAXHEALTH', 'METROPOLIS', 'MFSL', 'MINDTREE', 'NATIONALUM',
            'NAVINFLUOR', 'NESTLEIND', 'OBEROIRLTY', 'PIIND', 'PIDILITIND', 
            'PVRINOX', 'RAIN', 'RAJESHEXPO', 'RELAXO', 'SANOFI', 
            'SCHAEFFLER', 'SEQUENT', 'SHYAMMETL', 'SHOPERSTOP', 'SOBHA', 
            'STARHEALTH', 'SUNTV', 'SUPREMEIND', 'SYMPHONY', 'TECHM', 
            'TIINDIA', 'TRIDENT', 'TRENT', 'TTKPRESTIG', 'UJJIVAN', 
            'UNIONBANK', 'UPL', 'VGUARD', 'VINATIORGA', 'WHIRLPOOL', 'ZYDUSLIFE'
        ]


class EnhancedYFinanceClient:
    """Enhanced YFinance client for reliable fallback data"""
    
    def __init__(self):
        self.session = None
        self.failed_symbols = set()
        logger.info("🔧 Enhanced YFinance client initialized")
    
    def get_stock_data_batch(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get stock data for multiple symbols efficiently"""
        try:
            import yfinance as yf
            import pandas as pd
            
            real_data = []
            
            # Filter out previously failed symbols
            valid_symbols = [s for s in symbols if s not in self.failed_symbols]
            
            if not valid_symbols:
                logger.warning("⚠️ No valid symbols for YFinance")
                return []
            
            logger.info(f"📊 Fetching YFinance data for {len(valid_symbols)} symbols...")
            
            # Process in smaller batches for reliability
            batch_size = 10
            for i in range(0, len(valid_symbols), batch_size):
                batch = valid_symbols[i:i + batch_size]
                batch_data = self._process_batch(batch)
                real_data.extend(batch_data)
                
                # Rate limiting
                if i + batch_size < len(valid_symbols):
                    time.sleep(1)
            
            logger.info(f"✅ YFinance: Retrieved data for {len(real_data)} stocks")
            return real_data
            
        except Exception as e:
            logger.error(f"❌ YFinance batch processing failed: {e}")
            return []
    
    def _process_batch(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Process a batch of symbols"""
        batch_data = []
        
        for symbol in symbols:
            try:
                stock_data = self._get_individual_stock_data(symbol)
                if stock_data and self._validate_yfinance_data(stock_data):
                    batch_data.append(stock_data)
                else:
                    self.failed_symbols.add(symbol)
                    
            except Exception as e:
                logger.debug(f"⚠️ YFinance {symbol} failed: {e}")
                self.failed_symbols.add(symbol)
                continue
        
        return batch_data
    
    def _get_individual_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Get individual stock data from YFinance"""
        try:
            import yfinance as yf
            
            # Convert to NSE format
            yf_symbol = f"{symbol}.NS"
            
            # Create ticker
            ticker = yf.Ticker(yf_symbol)
            
            # Get recent data (7 days to ensure we have previous day)
            hist = ticker.history(period="7d", interval="1d")
            
            if hist.empty or len(hist) < 2:
                logger.debug(f"⚠️ {symbol}: Insufficient historical data")
                return None
            
            # Get current day and previous day data
            current_day = hist.iloc[-1]
            previous_day = hist.iloc[-2]
            
            # Get info for additional data
            try:
                info = ticker.info
                market_cap = info.get('marketCap', 0)
                volume = int(current_day['Volume'])
            except:
                market_cap = 0
                volume = int(current_day.get('Volume', 0))
            
            # Construct stock data
            stock_data = {
                'symbol': symbol,
                'ltp': float(current_day['Close']),
                'open_price': float(current_day['Open']),
                'high_price': float(current_day['High']),
                'low_price': float(current_day['Low']),
                'prev_close': float(previous_day['Close']),
                'prev_day_high': float(previous_day['High']),
                'prev_day_open': float(previous_day['Open']),
                'prev_day_low': float(previous_day['Low']),
                'volume': volume,
                'market_cap': market_cap,
                'change_in_oi': 0,  # YFinance doesn't provide OI
                'total_oi': 0,
                'source': 'yfinance_real'
            }
            
            return stock_data
            
        except Exception as e:
            logger.debug(f"⚠️ YFinance individual fetch failed for {symbol}: {e}")
            return None
    
    def _validate_yfinance_data(self, stock_data: Dict[str, Any]) -> bool:
        """Validate YFinance data quality"""
        try:
            # Check required fields
            required_fields = ['ltp', 'open_price', 'prev_close', 'prev_day_high']
            
            for field in required_fields:
                value = stock_data.get(field, 0)
                if not isinstance(value, (int, float)) or value <= 0:
                    return False
            
            # Price range validation
            ltp = stock_data['ltp']
            if not (5 <= ltp <= 100000):
                return False
            
            # Volume validation
            volume = stock_data.get('volume', 0)
            if volume <= 0:
                return False
            
            # Change validation
            prev_close = stock_data['prev_close']
            if prev_close > 0:
                change_pct = abs((ltp - prev_close) / prev_close)
                if change_pct > 0.5:  # Max 50% change
                    return False
            
            return True
            
        except Exception:
            return False


class NSEClient:
    def __init__(self):
        self.robust_scraper = RobustNSEScraper()
        self.yfinance_client = EnhancedYFinanceClient()
        self.nse_initialized = False
        self._init_nse_tools()
        
    def _init_nse_tools(self):
        """Initialize NSE tools with correct method"""
        try:
            from nsetools import Nse
            self.nse = Nse()
            
            if hasattr(self.nse, 'get_stock_codes'):
                self.nse_initialized = True
                logger.info("✅ NSE tools initialized successfully")
            else:
                logger.warning("⚠️ NSE tools missing required methods")
                
        except ImportError:
            logger.warning("⚠️ nsetools not available, using robust scraping only")
        except Exception as e:
            logger.warning(f"⚠️ Error initializing nsetools: {e}")
    
    def get_fo_stocks(self) -> List[str]:
        """Get F&O stocks using robust methods"""
        try:
            logger.info("📊 Fetching F&O stocks using robust NSE scraper...")
            
            # Use robust scraper as primary method
            fo_stocks = self.robust_scraper.get_fo_stocks_robust()
            
            if fo_stocks:
                logger.info(f"✅ Robust scraper: {len(fo_stocks)} F&O stocks")
                return fo_stocks
            
            # Fallback to NSE tools if available
            if self.nse_initialized:
                try:
                    logger.info("📊 Fallback: Using NSE tools...")
                    all_stocks = self.nse.get_stock_codes()
                    if all_stocks and isinstance(all_stocks, dict):
                        known_fo_stocks = self.robust_scraper._get_comprehensive_fo_list()
                        
                        fo_list = []
                        for symbol in known_fo_stocks:
                            if symbol in all_stocks:
                                fo_list.append(symbol)
                        
                        if fo_list:
                            logger.info(f"✅ NSE tools fallback: {len(fo_list)} stocks")
                            return fo_list
                            
                except Exception as e:
                    logger.warning(f"⚠️ NSE tools fallback failed: {e}")
            
            # Ultimate fallback
            logger.info("📋 Using comprehensive known F&O list")
            return self.robust_scraper._get_comprehensive_fo_list()
            
        except Exception as e:
            logger.error(f"❌ F&O stocks fetch failed: {e}")
            return self.robust_scraper._get_comprehensive_fo_list()
    
    def get_stock_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get REAL stock data using multiple robust methods"""
        try:
            all_stock_data = []
            successful_symbols = []
            failed_symbols = []
            
            logger.info(f"🎯 Fetching REAL stock data for {len(symbols)} symbols...")
            
            # Method 1: Robust NSE Scraper (Primary)
            try:
                logger.info("📊 Method 1: Robust NSE scraping...")
                nse_data = self.robust_scraper.get_stock_data_robust(symbols[:20])  # Limit for efficiency
                
                if nse_data:
                    all_stock_data.extend(nse_data)
                    successful_symbols.extend([s['symbol'] for s in nse_data])
                    logger.info(f"✅ Robust NSE: {len(nse_data)} stocks")
                else:
                    logger.warning("⚠️ Robust NSE scraper returned no data")
                    
            except Exception as e:
                logger.warning(f"⚠️ Robust NSE scraper failed: {e}")
            
            # Method 2: NSE Tools (if available and for missing symbols)
            if self.nse_initialized:
                missing_symbols = [s for s in symbols if s not in successful_symbols]
                if missing_symbols:
                    try:
                        logger.info(f"📊 Method 2: NSE tools for {len(missing_symbols)} missing symbols...")
                        nse_tools_data = self._get_nse_tools_data(missing_symbols[:10])
                        
                        if nse_tools_data:
                            all_stock_data.extend(nse_tools_data)
                            successful_symbols.extend([s['symbol'] for s in nse_tools_data])
                            logger.info(f"✅ NSE tools: {len(nse_tools_data)} additional stocks")
                            
                    except Exception as e:
                        logger.warning(f"⚠️ NSE tools method failed: {e}")
            
            # Method 3: YFinance fallback (for remaining missing symbols)
            final_missing = [s for s in symbols if s not in successful_symbols]
            if final_missing:
                try:
                    logger.info(f"📊 Method 3: YFinance fallback for {len(final_missing)} symbols...")
                    yf_data = self.yfinance_client.get_stock_data_batch(final_missing[:15])
                    
                    if yf_data:
                        all_stock_data.extend(yf_data)
                        successful_symbols.extend([s['symbol'] for s in yf_data])
                        logger.info(f"✅ YFinance: {len(yf_data)} additional stocks")
                        
                except Exception as e:
                    logger.warning(f"⚠️ YFinance fallback failed: {e}")
            
            # Final summary
            failed_symbols = [s for s in symbols if s not in successful_symbols]
            
            if all_stock_data:
                logger.info(f"✅ TOTAL SUCCESS: {len(all_stock_data)} stocks with real data")
                if successful_symbols:
                    logger.info(f"✅ Successful: {', '.join(successful_symbols[:5])}{'...' if len(successful_symbols) > 5 else ''}")
            
            if failed_symbols:
                logger.warning(f"❌ Failed to fetch: {len(failed_symbols)} stocks")
                logger.debug(f"❌ Failed symbols: {', '.join(failed_symbols[:10])}")
            
            return all_stock_data
            
        except Exception as e:
            logger.error(f"❌ Stock data fetch completely failed: {e}")
            return []
    
    def _get_nse_tools_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get data using NSE tools"""
        nse_data = []
        
        for symbol in symbols:
            try:
                time.sleep(0.5)  # Rate limiting
                
                quote = self.nse.get_quote(symbol)
                if quote and isinstance(quote, dict) and quote.get('lastPrice'):
                    
                    current_price = float(quote.get('lastPrice', 0))
                    open_price = float(quote.get('open', 0))
                    prev_close = float(quote.get('previousClose', 0))
                    
                    # Get intraday high/low
                    intraday = quote.get('intraDayHighLow', {})
                    day_high = float(intraday.get('max', current_price))
                    day_low = float(intraday.get('min', current_price))
                    
                    volume = int(quote.get('totalTradedVolume', 0))
                    
                    # Try to get historical data from robust scraper
                    hist_data = self.robust_scraper._get_historical_data_robust(symbol)
                    
                    stock_info = {
                        'symbol': symbol,
                        'open_price': open_price,
                        'high_price': day_high,
                        'low_price': day_low,
                        'ltp': current_price,
                        'prev_close': prev_close,
                        'prev_day_high': hist_data.get('prev_day_high', prev_close),
                        'prev_day_open': hist_data.get('prev_day_open', prev_close),
                        'prev_day_low': hist_data.get('prev_day_low', prev_close),
                        'volume': volume,
                        'change_in_oi': 0,
                        'total_oi': 0,
                        'source': 'nsetools_real'
                    }
                    
                    # Try to get F&O data
                    fo_data = self.robust_scraper._get_fo_data_robust(symbol)
                    if fo_data:
                        stock_info.update(fo_data)
                    
                    if self._validate_real_stock_data(stock_info):
                        nse_data.append(stock_info)
                        logger.debug(f"✅ NSE tools: {symbol} = ₹{current_price:.2f}")
                
            except Exception as e:
                logger.debug(f"⚠️ NSE tools failed for {symbol}: {e}")
                continue
        
        return nse_data
    
    def _validate_real_stock_data(self, stock_info: Dict[str, Any]) -> bool:
        """Validate that we have real stock data"""
        try:
            required_fields = ['symbol', 'open_price', 'ltp', 'prev_close']
            
            for field in required_fields:
                value = stock_info.get(field, 0)
                if not isinstance(value, (int, float)) or value <= 0:
                    return False
            
            # Additional validations
            ltp = stock_info['ltp']
            prev_close = stock_info['prev_close']
            
            # Price should be reasonable
            if not (5 <= ltp <= 100000):
                return False
            
            # Change should not be extreme
            if prev_close > 0:
                change_pct = abs((ltp - prev_close) / prev_close)
                if change_pct > 0.5:  # Max 50% change
                    return False
            
            return True
            
        except Exception:
            return False
    
    def get_historical_data(self, symbol: str) -> Dict[str, Any]:
        """Get historical data using robust methods"""
        return self.robust_scraper._get_historical_data_robust(symbol)