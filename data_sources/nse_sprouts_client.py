import requests
import json
from typing import List, Dict, Any
import logging
import time
import random

logger = logging.getLogger(__name__)

class NSESproutsClient:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(self.headers)
        
    def get_pre_open_data(self) -> List[Dict[str, Any]]:
        """Get pre-open market data with complete OHLC and OI data"""
        try:
            logger.info("Attempting to fetch REAL comprehensive pre-open data from NSE")
            
            # Enhanced NSE endpoints for comprehensive data
            pre_open_urls = [
                "https://www.nseindia.com/api/market-data-pre-open?key=ALL",
                "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY",
                "https://www.nseindia.com/api/market-data-pre-open?key=FO",
                "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
            ]
            
            for url in pre_open_urls:
                try:
                    # Establish session first
                    home_response = self.session.get("https://www.nseindia.com", timeout=10)
                    if home_response.status_code != 200:
                        continue
                        
                    time.sleep(random.uniform(1, 2))  # Rate limiting
                    
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if 'data' in data and data['data']:
                            stock_data = []
                            
                            for item in data['data'][:40]:  # Increased limit
                                try:
                                    stock_info = self._parse_comprehensive_data(item)
                                    
                                    if stock_info and self._validate_real_data(stock_info):
                                        # Get additional data for each stock
                                        enhanced_data = self._get_enhanced_stock_data(stock_info['symbol'])
                                        if enhanced_data:
                                            stock_info.update(enhanced_data)
                                        
                                        stock_data.append(stock_info)
                                        
                                except Exception as e:
                                    logger.debug(f"Error parsing pre-open item: {e}")
                                    continue
                            
                            if stock_data:
                                logger.info(f"✅ Retrieved comprehensive REAL data for {len(stock_data)} stocks")
                                return stock_data
                                
                    else:
                        logger.debug(f"Pre-open URL returned {response.status_code}")
                        
                except Exception as e:
                    logger.debug(f"Error fetching from {url}: {e}")
                    continue
            
            # If no real data available, return empty list (NO SYNTHETIC DATA)
            logger.warning("❌ No REAL pre-open data available from NSE - returning empty list")
            return []
            
        except Exception as e:
            logger.error(f"Error fetching pre-open data: {e}")
            return []
    
    def _parse_comprehensive_data(self, item: Dict) -> Dict[str, Any]:
        """Parse comprehensive data from NSE response"""
        try:
            if 'metadata' in item and 'lastPrice' in item:
                metadata = item['metadata']
                
                stock_info = {
                    'symbol': metadata.get('symbol', ''),
                    'open_price': float(item.get('lastPrice', 0)),
                    'ltp': float(item.get('lastPrice', 0)),
                    'prev_close': float(metadata.get('prevClose', 0)),
                    'high_price': float(metadata.get('dayHigh', 0)),
                    'low_price': float(metadata.get('dayLow', 0)),
                    'prev_day_high': float(metadata.get('prevClose', 0)) * 1.01,  # Will be updated
                    'prev_day_open': float(metadata.get('prevClose', 0)) * 0.995,
                    'prev_day_low': float(metadata.get('prevClose', 0)) * 0.985,
                    'volume': int(item.get('totalTradedVolume', 0)),
                    'percentage_change': float(item.get('perChange', 0)),
                    'change_in_oi': 0,  # Will be updated if available
                    'total_oi': 0,
                    'source': 'nse_preopen_comprehensive'
                }
                
                return stock_info
            
            # Alternative parsing for different data structures
            elif 'symbol' in item:
                stock_info = {
                    'symbol': item.get('symbol', ''),
                    'open_price': float(item.get('open', 0)),
                    'ltp': float(item.get('lastPrice', 0)),
                    'prev_close': float(item.get('previousClose', 0)),
                    'high_price': float(item.get('dayHigh', 0)),
                    'low_price': float(item.get('dayLow', 0)),
                    'volume': int(item.get('totalTradedVolume', 0)),
                    'percentage_change': float(item.get('pChange', 0)),
                    'change_in_oi': 0,
                    'total_oi': 0,
                    'source': 'nse_market_data'
                }
                
                # Calculate previous day estimates
                prev_close = stock_info['prev_close']
                if prev_close > 0:
                    stock_info['prev_day_high'] = prev_close * 1.01
                    stock_info['prev_day_open'] = prev_close * 0.995
                    stock_info['prev_day_low'] = prev_close * 0.985
                
                return stock_info
            
            return None
            
        except Exception as e:
            logger.debug(f"Error parsing item data: {e}")
            return None
    
    def _get_enhanced_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Get enhanced data including previous day OHLC and OI"""
        try:
            enhanced_data = {}
            
            # Try to get historical OHLC data
            hist_data = self._get_historical_ohlc(symbol)
            if hist_data:
                enhanced_data.update(hist_data)
            
            # Try to get F&O data including OI
            fo_data = self._get_fo_data(symbol)
            if fo_data:
                enhanced_data.update(fo_data)
            
            return enhanced_data
            
        except Exception as e:
            logger.debug(f"Error getting enhanced data for {symbol}: {e}")
            return {}
    
    def _get_historical_ohlc(self, symbol: str) -> Dict[str, Any]:
        """Get previous day OHLC data"""
        try:
            # Historical data endpoint
            from datetime import datetime, timedelta
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)  # Get week data to ensure previous day
            
            hist_url = f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&series=[%22EQ%22]&from={start_date.strftime('%d-%m-%Y')}&to={end_date.strftime('%d-%m-%Y')}"
            
            response = self.session.get(hist_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and len(data['data']) >= 2:
                    # Get previous trading day data
                    prev_day = data['data'][-2] if len(data['data']) >= 2 else data['data'][-1]
                    
                    return {
                        'prev_day_high': float(prev_day.get('CH_TRADE_HIGH_PRICE', 0)),
                        'prev_day_open': float(prev_day.get('CH_OPENING_PRICE', 0)),
                        'prev_day_low': float(prev_day.get('CH_TRADE_LOW_PRICE', 0)),
                        'prev_day_close': float(prev_day.get('CH_CLOSING_PRICE', 0))
                    }
            
            return {}
            
        except Exception as e:
            logger.debug(f"Could not get historical OHLC for {symbol}: {e}")
            return {}
    
    def _get_fo_data(self, symbol: str) -> Dict[str, Any]:
        """Get F&O data including Open Interest"""
        try:
            # F&O quote endpoint
            fo_url = f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
            response = self.session.get(fo_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Look for futures data
                if 'stocks' in data:
                    for stock_data in data['stocks']:
                        if 'metadata' in stock_data:
                            metadata = stock_data['metadata']
                            if metadata.get('instrumentType') == 'Stock Futures':
                                return {
                                    'total_oi': int(metadata.get('openInterest', 0)),
                                    'change_in_oi': int(metadata.get('changeinOpenInterest', 0))
                                }
                
                # Alternative structure
                if 'data' in data and data['data']:
                    for item in data['data']:
                        if item.get('instrumentType') == 'FUTIDX' or item.get('instrumentType') == 'FUTSTK':
                            return {
                                'total_oi': int(item.get('openInterest', 0)),
                                'change_in_oi': int(item.get('changeinOpenInterest', 0))
                            }
            
            return {'total_oi': 0, 'change_in_oi': 0}
            
        except Exception as e:
            logger.debug(f"Could not get F&O data for {symbol}: {e}")
            return {'total_oi': 0, 'change_in_oi': 0}
    
    def _validate_real_data(self, stock_info: Dict[str, Any]) -> bool:
        """Validate that the data is real and not synthetic - STRICT VALIDATION"""
        try:
            # Check required fields
            required_fields = ['symbol', 'ltp', 'prev_close']
            for field in required_fields:
                if not stock_info.get(field):
                    return False
            
            # Check if prices are reasonable and REAL
            ltp = stock_info.get('ltp', 0)
            prev_close = stock_info.get('prev_close', 0)
            open_price = stock_info.get('open_price', 0)
            
            # All prices must be positive
            if ltp <= 0 or prev_close <= 0:
                return False
            
            # Price should be in reasonable range for Indian stocks
            if not (5 <= ltp <= 100000):
                return False
            
            # Symbol should look valid (Indian stock symbol format)
            symbol = stock_info.get('symbol', '')
            if len(symbol) < 2 or not symbol.replace('&', '').replace('-', '').isalnum():
                return False
            
            # Check for reasonable price relationships
            if open_price > 0:
                # Open shouldn't be too far from prev_close (max 20% gap)
                gap = abs(open_price - prev_close) / prev_close
                if gap > 0.2:
                    logger.debug(f"Extreme gap detected for {symbol}: {gap*100:.1f}%")
                    return False
            
            # Volume should be meaningful for most stocks
            volume = stock_info.get('volume', 0)
            if volume == 0 and ltp < 500:  # Low-priced stocks should have volume
                return False
                
            return True
            
        except Exception as e:
            logger.debug(f"Validation error: {e}")
            return False
    
    def get_gainers_losers(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get top gainers and losers with complete data"""
        try:
            logger.info("Attempting to fetch comprehensive gainers/losers data from NSE")
            
            gainers_data = []
            losers_data = []
            
            try:
                # Establish session
                home_response = self.session.get("https://www.nseindia.com", timeout=10)
                if home_response.status_code != 200:
                    raise Exception("Could not establish NSE session")
                
                time.sleep(1)
                
                # Get comprehensive market data
                market_urls = [
                    "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O",
                    "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"
                ]
                
                all_stocks_data = []
                
                for url in market_urls:
                    try:
                        response = self.session.get(url, timeout=15)
                        
                        if response.status_code == 200:
                            data = response.json()
                            if 'data' in data and data['data']:
                                all_stocks_data.extend(data['data'])
                        
                        time.sleep(1)  # Rate limiting
                        
                    except Exception as e:
                        logger.debug(f"Error fetching from {url}: {e}")
                        continue
                
                if all_stocks_data:
                    # Sort by percentage change to get gainers and losers
                    sorted_stocks = sorted(all_stocks_data, key=lambda x: float(x.get('pChange', 0)), reverse=True)
                    
                    # Top gainers (positive change)
                    gainers_count = 0
                    for stock in sorted_stocks:
                        if gainers_count >= 8:  # Limit to top 8 gainers
                            break
                            
                        pchange = float(stock.get('pChange', 0))
                        if pchange > 2:  # Only significant gainers
                            stock_info = self._create_comprehensive_stock_info(stock, 'nse_gainers_comprehensive')
                            
                            if stock_info and self._validate_real_data(stock_info):
                                # Get enhanced data
                                enhanced_data = self._get_enhanced_stock_data(stock_info['symbol'])
                                if enhanced_data:
                                    stock_info.update(enhanced_data)
                                
                                gainers_data.append(stock_info)
                                gainers_count += 1
                    
                    # Top losers (negative change)
                    losers_count = 0
                    losers_candidates = [s for s in sorted_stocks if float(s.get('pChange', 0)) < -2]
                    
                    for stock in losers_candidates[-8:]:  # Bottom 8 (most negative)
                        if losers_count >= 8:
                            break
                            
                        stock_info = self._create_comprehensive_stock_info(stock, 'nse_losers_comprehensive')
                        
                        if stock_info and self._validate_real_data(stock_info):
                            # Get enhanced data
                            enhanced_data = self._get_enhanced_stock_data(stock_info['symbol'])
                            if enhanced_data:
                                stock_info.update(enhanced_data)
                            
                            losers_data.append(stock_info)
                            losers_count += 1
                
                if gainers_data or losers_data:
                    logger.info(f"✅ Retrieved comprehensive data - {len(gainers_data)} gainers, {len(losers_data)} losers")
                    return {'gainers': gainers_data, 'losers': losers_data}
                    
            except Exception as e:
                logger.warning(f"NSE comprehensive gainers/losers API failed: {e}")
            
            # If no real data available, return empty lists
            logger.warning("❌ No REAL gainers/losers data available - returning empty lists")
            return {'gainers': [], 'losers': []}
            
        except Exception as e:
            logger.error(f"Error fetching gainers/losers data: {e}")
            return {'gainers': [], 'losers': []}
    
    def _create_comprehensive_stock_info(self, stock: Dict, source: str) -> Dict[str, Any]:
        """Create comprehensive stock info from NSE data"""
        try:
            stock_info = {
                'symbol': stock.get('symbol', ''),
                'ltp': float(stock.get('lastPrice', 0)),
                'open_price': float(stock.get('open', stock.get('lastPrice', 0))),
                'high_price': float(stock.get('dayHigh', stock.get('lastPrice', 0))),
                'low_price': float(stock.get('dayLow', stock.get('lastPrice', 0))),
                'prev_close': float(stock.get('previousClose', 0)),
                'percentage_change': float(stock.get('pChange', 0)),
                'volume': int(stock.get('totalTradedVolume', 0)),
                'turnover': float(stock.get('totalTradedValue', 0)) / 100000,  # Convert to lakhs
                'change_in_oi': 0,  # Will be updated if available
                'total_oi': 0,
                'source': source
            }
            
            # Calculate previous day estimates
            prev_close = stock_info['prev_close']
            if prev_close > 0:
                stock_info['prev_day_high'] = prev_close * 1.01
                stock_info['prev_day_open'] = prev_close * 0.995
                stock_info['prev_day_low'] = prev_close * 0.985
            
            return stock_info
            
        except Exception as e:
            logger.debug(f"Error creating stock info: {e}")
            return None
    
    def get_most_active_stocks(self) -> List[Dict[str, Any]]:
        """Get most active stocks by volume with comprehensive data"""
        try:
            logger.info("Attempting to fetch comprehensive most active stocks from NSE")
            
            active_stocks = []
            
            try:
                # Establish session
                home_response = self.session.get("https://www.nseindia.com", timeout=10)
                if home_response.status_code != 200:
                    raise Exception("Could not establish NSE session")
                
                time.sleep(1)
                
                # Get most active by volume
                active_urls = [
                    "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O",
                    "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"
                ]
                
                all_stocks_data = []
                
                for url in active_urls:
                    try:
                        response = self.session.get(url, timeout=15)
                        
                        if response.status_code == 200:
                            data = response.json()
                            if 'data' in data and data['data']:
                                all_stocks_data.extend(data['data'])
                        
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.debug(f"Error fetching from {url}: {e}")
                        continue
                
                if all_stocks_data:
                    # Sort by volume to get most active
                    sorted_by_volume = sorted(all_stocks_data, key=lambda x: int(x.get('totalTradedVolume', 0)), reverse=True)
                    
                    active_count = 0
                    for stock in sorted_by_volume:
                        if active_count >= 12:  # Top 12 by volume
                            break
                            
                        volume = int(stock.get('totalTradedVolume', 0))
                        if volume > 50000:  # Only stocks with significant volume
                            stock_info = self._create_comprehensive_stock_info(stock, 'nse_active_comprehensive')
                            
                            if stock_info and self._validate_real_data(stock_info):
                                # Get enhanced data
                                enhanced_data = self._get_enhanced_stock_data(stock_info['symbol'])
                                if enhanced_data:
                                    stock_info.update(enhanced_data)
                                
                                active_stocks.append(stock_info)
                                active_count += 1
                
                if active_stocks:
                    logger.info(f"✅ Retrieved comprehensive most active data for {len(active_stocks)} stocks")
                    return active_stocks
                    
            except Exception as e:
                logger.warning(f"NSE most active API failed: {e}")
            
            # If no real data available, return empty list
            logger.warning("❌ No REAL most active stocks data available - returning empty list")
            return []
            
        except Exception as e:
            logger.error(f"Error fetching most active stocks: {e}")
            return []
    
    def get_comprehensive_market_data(self) -> Dict[str, Any]:
        """Get comprehensive market data combining all sources"""
        try:
            logger.info("Fetching comprehensive market data from multiple NSE sources")
            
            # Get all data types
            pre_open_data = self.get_pre_open_data()
            gainers_losers = self.get_gainers_losers()
            most_active = self.get_most_active_stocks()
            
            # Combine all unique stocks
            all_stocks = {}
            
            # Add pre-open data
            for stock in pre_open_data:
                symbol = stock['symbol']
                all_stocks[symbol] = stock
            
            # Add gainers
            for stock in gainers_losers.get('gainers', []):
                symbol = stock['symbol']
                if symbol in all_stocks:
                    # Merge data, preferring more complete information
                    all_stocks[symbol] = self._merge_stock_data(all_stocks[symbol], stock)
                else:
                    all_stocks[symbol] = stock
            
            # Add losers
            for stock in gainers_losers.get('losers', []):
                symbol = stock['symbol']
                if symbol in all_stocks:
                    all_stocks[symbol] = self._merge_stock_data(all_stocks[symbol], stock)
                else:
                    all_stocks[symbol] = stock
            
            # Add most active
            for stock in most_active:
                symbol = stock['symbol']
                if symbol in all_stocks:
                    all_stocks[symbol] = self._merge_stock_data(all_stocks[symbol], stock)
                else:
                    all_stocks[symbol] = stock
            
            comprehensive_data = list(all_stocks.values())
            
            logger.info(f"✅ Compiled comprehensive data for {len(comprehensive_data)} unique stocks")
            
            return {
                'comprehensive_stocks': comprehensive_data,
                'pre_open_count': len(pre_open_data),
                'gainers_count': len(gainers_losers.get('gainers', [])),
                'losers_count': len(gainers_losers.get('losers', [])),
                'active_count': len(most_active),
                'total_unique_stocks': len(comprehensive_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting comprehensive market data: {e}")
            return {
                'comprehensive_stocks': [],
                'pre_open_count': 0,
                'gainers_count': 0,
                'losers_count': 0,
                'active_count': 0,
                'total_unique_stocks': 0
            }
    
    def _merge_stock_data(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """Merge stock data, preferring non-zero and more recent values"""
        try:
            merged = existing.copy()
            
            for key, value in new.items():
                if key == 'source':
                    # Combine sources
                    existing_sources = merged.get('source', '').split(',')
                    if value not in existing_sources:
                        merged['source'] = f"{merged.get('source', '')},{value}".strip(',')
                elif isinstance(value, (int, float)) and value != 0:
                    # Use non-zero values, or prefer newer data
                    if merged.get(key, 0) == 0 or value != 0:
                        merged[key] = value
                elif not merged.get(key) and value:
                    # Use non-empty values
                    merged[key] = value
            
            return merged
            
        except Exception as e:
            logger.debug(f"Error merging stock data: {e}")
            return existing