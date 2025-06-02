import requests
import json
from typing import List, Dict, Any
import logging
from config.settings import settings
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class FyersClient:
    def __init__(self):
        self.app_id = settings.FYERS_APP_ID
        self.access_token = settings.FYERS_ACCESS_TOKEN
        self.base_url = "https://api-t1.fyers.in/api/v3"
        
        if not self.access_token or not self.app_id:
            logger.warning("Fyers credentials not configured - will skip Fyers data")
        else:
            logger.info("Fyers client initialized with credentials")
    
    def get_fo_stocks(self) -> List[str]:
        """Get list of F&O stocks from Fyers"""
        try:
            if not self.access_token or not self.app_id:
                logger.info("Fyers not configured, using fallback F&O list")
                return self._get_fallback_fo_stocks()
            
            # Try to get symbols from Fyers symbols API
            try:
                url = f"{self.base_url}/data/symbols"
                headers = {
                    'Authorization': f'{self.app_id}:{self.access_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('s') == 'ok' and 'data' in data:
                        fo_stocks = []
                        for item in data['data']:
                            symbol_info = item.get('symbol', '')
                            if 'NSE:' in symbol_info and '-EQ' in symbol_info:
                                base_symbol = symbol_info.replace('NSE:', '').replace('-EQ', '')
                                if base_symbol not in fo_stocks and len(base_symbol) > 1:
                                    fo_stocks.append(base_symbol)
                        
                        if fo_stocks:
                            logger.info(f"Retrieved {len(fo_stocks)} symbols from Fyers")
                            return fo_stocks[:50]
                
            except Exception as e:
                logger.warning(f"Fyers symbols API failed: {e}")
            
            logger.info("Using fallback F&O stocks list")
            return self._get_fallback_fo_stocks()
            
        except Exception as e:
            logger.error(f"Error fetching F&O stocks from Fyers: {e}")
            return self._get_fallback_fo_stocks()
    
    def _get_fallback_fo_stocks(self) -> List[str]:
        """Fallback list of major F&O stocks"""
        return [
            'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR', 'ICICIBANK',
            'KOTAKBANK', 'SBIN', 'BHARTIARTL', 'ITC', 'ASIANPAINT', 'LT',
            'AXISBANK', 'MARUTI', 'SUNPHARMA', 'ULTRACEMCO', 'TITAN', 'WIPRO',
            'POWERGRID', 'NTPC', 'TATAMOTORS', 'ONGC', 'HCLTECH', 'BAJFINANCE', 
            'M&M', 'TATASTEEL', 'COALINDIA', 'GRASIM', 'HINDALCO', 'JSWSTEEL', 
            'INDUSINDBK', 'HEROMOTOCO', 'CIPLA', 'DRREDDY', 'EICHERMOT', 
            'BAJAJFINSV', 'BRITANNIA', 'SHREECEM', 'DIVISLAB', 'BPCL',
            'GODREJCP', 'DABUR', 'BANDHANBNK', 'BERGEPAINT', 'BIOCON'
        ]
    
    def get_stock_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get REAL accurate stock data from Fyers API with all required fields"""
        try:
            if not self.access_token or not self.app_id:
                logger.info("Fyers not configured, skipping Fyers data")
                return []
            
            stock_data = []
            
            # Process symbols in smaller batches
            for i in range(0, min(len(symbols), 20), 5):
                batch = symbols[i:i+5]
                
                for symbol in batch:
                    try:
                        time.sleep(0.2)  # Rate limiting
                        
                        # Get current market data
                        current_data = self._get_current_quote(symbol)
                        if not current_data:
                            continue
                        
                        # Get historical data for previous day OHLC
                        historical_data = self.get_detailed_historical_data(symbol)
                        
                        # Get F&O specific data (Open Interest)
                        fo_data = self._get_fo_data(symbol)
                        
                        # Combine all data
                        stock_info = {
                            'symbol': symbol,
                            'open_price': current_data.get('open_price', 0),
                            'high_price': current_data.get('high_price', 0),
                            'low_price': current_data.get('low_price', 0),
                            'ltp': current_data.get('ltp', 0),
                            'prev_close': current_data.get('prev_close', 0),
                            'prev_day_high': historical_data.get('prev_day_high', 0),
                            'prev_day_open': historical_data.get('prev_day_open', 0),
                            'prev_day_low': historical_data.get('prev_day_low', 0),
                            'volume': current_data.get('volume', 0),
                            'change_in_oi': fo_data.get('change_in_oi', 0),
                            'total_oi': fo_data.get('total_oi', 0),
                            'source': 'fyers'
                        }
                        
                        # Validate data quality
                        if self._validate_stock_data(stock_info):
                            stock_data.append(stock_info)
                            logger.debug(f"Got REAL Fyers data for {symbol}: Open={stock_info['open_price']:.2f}, LTP={stock_info['ltp']:.2f}")
                        else:
                            logger.warning(f"Invalid data for {symbol} from Fyers")
                            
                    except Exception as e:
                        logger.warning(f"Error getting Fyers data for {symbol}: {e}")
                        continue
                
                # Delay between batches
                if i + 5 < len(symbols):
                    time.sleep(1)
            
            logger.info(f"Retrieved REAL data for {len(stock_data)} stocks from Fyers")
            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching stock data from Fyers: {e}")
            return []
    
    def _get_current_quote(self, symbol: str) -> Dict[str, Any]:
        """Get current market quote for a symbol"""
        try:
            nse_symbol = f"NSE:{symbol}-EQ"
            
            url = f"{self.base_url}/data/quotes"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            params = {'symbols': nse_symbol}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('s') == 'ok' and 'd' in data and data['d']:
                    for quote_data in data['d']:
                        if 'v' in quote_data:
                            quote = quote_data['v']
                            
                            return {
                                'open_price': float(quote.get('o', 0)),
                                'high_price': float(quote.get('h', 0)),
                                'low_price': float(quote.get('l', 0)),
                                'ltp': float(quote.get('lp', 0)),
                                'prev_close': float(quote.get('prev_close_price', 0)),
                                'volume': int(quote.get('v', 0))
                            }
            elif response.status_code == 401:
                logger.error("Fyers authentication failed - check credentials")
            elif response.status_code == 404:
                logger.debug(f"Symbol {symbol} not found in Fyers")
            
            return {}
            
        except Exception as e:
            logger.debug(f"Error getting current quote for {symbol}: {e}")
            return {}
    
    def get_detailed_historical_data(self, symbol: str, days: int = 3) -> Dict[str, Any]:
        """Get detailed historical data including previous day OHLC"""
        try:
            nse_symbol = f"NSE:{symbol}-EQ"
            
            url = f"{self.base_url}/data/history"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            params = {
                'symbol': nse_symbol,
                'resolution': 'D',
                'date_format': '1',
                'range_from': start_date.strftime('%Y-%m-%d'),
                'range_to': end_date.strftime('%Y-%m-%d'),
                'cont_flag': '1'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('s') == 'ok' and 'candles' in data and data['candles']:
                    candles = data['candles']
                    
                    if len(candles) >= 2:
                        # Previous day data (second last candle)
                        prev_candle = candles[-2]
                        
                        return {
                            'prev_day_open': float(prev_candle[1]),   # Open
                            'prev_day_high': float(prev_candle[2]),   # High
                            'prev_day_low': float(prev_candle[3]),    # Low
                            'prev_day_close': float(prev_candle[4])   # Close
                        }
                    elif len(candles) == 1:
                        # Only current day data available
                        current_candle = candles[0]
                        return {
                            'prev_day_open': float(current_candle[1]),
                            'prev_day_high': float(current_candle[2]),
                            'prev_day_low': float(current_candle[3]),
                            'prev_day_close': float(current_candle[4])
                        }
            
            return {}
            
        except Exception as e:
            logger.debug(f"Error fetching historical data for {symbol}: {e}")
            return {}
    
    def _get_fo_data(self, symbol: str) -> Dict[str, Any]:
        """Get Futures & Options specific data like Open Interest"""
        try:
            # Try to get F&O data for the symbol
            fo_symbol = f"NSE:{symbol}"
            
            # Get futures data
            futures_url = f"{self.base_url}/data/quotes"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Try current month futures
            current_date = datetime.now()
            # Get last Thursday of current month for expiry
            expiry_date = self._get_expiry_date(current_date)
            
            fo_symbols = [
                f"NSE:{symbol}{expiry_date.strftime('%y%b').upper()}FUT",
                f"NSE:{symbol}-EQ"  # Fallback to equity
            ]
            
            for fo_sym in fo_symbols:
                try:
                    params = {'symbols': fo_sym}
                    response = requests.get(futures_url, headers=headers, params=params, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('s') == 'ok' and 'd' in data and data['d']:
                            for quote_data in data['d']:
                                if 'v' in quote_data:
                                    quote = quote_data['v']
                                    
                                    # Extract OI data if available
                                    oi = quote.get('oi', 0)
                                    prev_oi = quote.get('prev_oi', 0)
                                    change_oi = oi - prev_oi if prev_oi > 0 else 0
                                    
                                    return {
                                        'total_oi': int(oi),
                                        'change_in_oi': int(change_oi)
                                    }
                                    
                except Exception as e:
                    logger.debug(f"Error getting F&O data for {fo_sym}: {e}")
                    continue
            
            return {'total_oi': 0, 'change_in_oi': 0}
            
        except Exception as e:
            logger.debug(f"Error getting F&O data for {symbol}: {e}")
            return {'total_oi': 0, 'change_in_oi': 0}
    
    def _get_expiry_date(self, date: datetime) -> datetime:
        """Get the last Thursday of the month for F&O expiry"""
        try:
            # Get last day of month
            if date.month == 12:
                next_month = date.replace(year=date.year + 1, month=1, day=1)
            else:
                next_month = date.replace(month=date.month + 1, day=1)
            
            last_day = next_month - timedelta(days=1)
            
            # Find last Thursday
            days_after_thursday = (last_day.weekday() - 3) % 7
            last_thursday = last_day - timedelta(days=days_after_thursday)
            
            return last_thursday
            
        except Exception:
            # Fallback to end of month
            return date.replace(day=28)
    
    def _validate_stock_data(self, stock_info: Dict[str, Any]) -> bool:
        """Validate stock data quality"""
        try:
            required_fields = ['open_price', 'ltp', 'prev_close']
            
            for field in required_fields:
                if not isinstance(stock_info.get(field), (int, float)) or stock_info[field] <= 0:
                    return False
            
            # Additional validation
            if stock_info['ltp'] > stock_info['prev_close'] * 3:  # Sanity check
                return False
                
            return True
            
        except Exception:
            return False
    
    def test_connection(self) -> bool:
        """Test Fyers API connection"""
        try:
            if not self.access_token or not self.app_id:
                logger.warning("Fyers credentials not configured for testing")
                return False
            
            url = f"{self.base_url}/profile"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('s') == 'ok':
                    logger.info("✅ Fyers connection test successful")
                    return True
                else:
                    logger.error(f"❌ Fyers API error: {data.get('message', 'Unknown error')}")
                    return False
            else:
                logger.error(f"❌ Fyers connection test failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error testing Fyers connection: {e}")
            return False
    
    def get_funds(self) -> Dict[str, Any]:
        """Get account funds information"""
        try:
            if not self.access_token or not self.app_id:
                return {}
            
            url = f"{self.base_url}/funds"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('s') == 'ok':
                    return data.get('fund_limit', {})
            
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching funds: {e}")
            return {}
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """Get current positions"""
        try:
            if not self.access_token or not self.app_id:
                return []
            
            url = f"{self.base_url}/positions"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('s') == 'ok':
                    return data.get('netPositions', [])
            
            return []
            
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []
    
    def get_single_quote(self, symbol: str) -> Dict[str, Any]:
        """Get quote for a single symbol"""
        try:
            if not self.access_token or not self.app_id:
                return {}
            
            nse_symbol = f"NSE:{symbol}-EQ"
            
            url = f"{self.base_url}/data/quotes"
            headers = {
                'Authorization': f'{self.app_id}:{self.access_token}',
                'Content-Type': 'application/json'
            }
            
            params = {'symbols': nse_symbol}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('s') == 'ok' and 'd' in data and data['d']:
                    quote_data = data['d'][0]
                    if 'v' in quote_data:
                        return quote_data['v']
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting quote for {symbol}: {e}")
            return {}
    
    def get_historical_data(self, symbol: str, days: int = 2) -> Dict[str, Any]:
        """Get historical data for compatibility"""
        return self.get_detailed_historical_data(symbol, days)
    
    def is_configured(self) -> bool:
        """Check if Fyers is properly configured"""
        return bool(self.access_token and self.app_id)