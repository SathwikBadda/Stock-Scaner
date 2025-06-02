from typing import List, Dict, Any
import logging
from config.settings import settings

logger = logging.getLogger(__name__)

class FilterAgent:
    """Agent responsible for filtering stocks based on strategy criteria with real data"""
    
    def __init__(self):
        self.min_percentage_increase = settings.MIN_PERCENTAGE_INCREASE
        self.filtered_stocks = []
        logger.info("FilterAgent initialized")
    
    def filter_stocks(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter stocks based on REAL data strategy:
        1. Open > Previous Day High
        2. LTP >= 7% increase from previous close
        3. Additional filters for accurate results
        """
        filtered_stocks = []
        
        logger.info(f"Starting to filter {len(stock_data)} stocks with REAL data...")
        
        for stock in stock_data:
            try:
                symbol = stock.get('symbol', '')
                open_price = float(stock.get('open_price', 0))
                ltp = float(stock.get('ltp', 0))
                prev_close = float(stock.get('prev_close', 0))
                prev_day_high = float(stock.get('prev_day_high', 0))
                prev_day_open = float(stock.get('prev_day_open', 0))
                prev_day_low = float(stock.get('prev_day_low', 0))
                volume = int(stock.get('volume', 0))
                change_in_oi = int(stock.get('change_in_oi', 0))
                
                # Skip if essential data is missing
                if not all([symbol, open_price, ltp, prev_close]):
                    logger.warning(f"Skipping {symbol} - missing essential data")
                    continue
                
                # Use prev_close as prev_day_high if not available (fallback)
                if prev_day_high == 0:
                    prev_day_high = prev_close
                    logger.debug(f"Using prev_close as prev_day_high for {symbol}")
                
                # FILTER CONDITION 1: Open > Previous Day High (Gap Up)
                gap_up_condition = open_price > prev_day_high
                gap_up_percentage = ((open_price - prev_day_high) / prev_day_high) * 100 if prev_day_high > 0 else 0
                
                # FILTER CONDITION 2: Calculate percentage change from previous close
                if prev_close > 0:
                    percentage_change = ((ltp - prev_close) / prev_close) * 100
                else:
                    percentage_change = 0
                
                momentum_condition = percentage_change >= self.min_percentage_increase
                
                # ADDITIONAL FILTERS for better accuracy
                # Filter 3: Volume should be significant (optional)
                volume_condition = volume > 1000  # Minimum volume threshold
                
                # Filter 4: Price should be reasonable (sanity check)
                price_sanity = 10 <= ltp <= 50000  # Between ₹10 and ₹50,000
                
                # Filter 5: Gap up should not be too extreme (avoid data errors)
                reasonable_gap = gap_up_percentage <= 20  # Max 20% gap up
                
                # Log detailed analysis
                logger.debug(f"""
                REAL DATA Analysis for {symbol}:
                - Current LTP: ₹{ltp:.2f}
                - Open: ₹{open_price:.2f}
                - Prev Close: ₹{prev_close:.2f}
                - Prev Day High: ₹{prev_day_high:.2f}
                - Prev Day Open: ₹{prev_day_open:.2f}
                - Volume: {volume:,}
                - Change in OI: {change_in_oi:,}
                - % Change: {percentage_change:.2f}%
                - Gap Up %: {gap_up_percentage:.2f}%
                - Gap Up: {gap_up_condition}
                - Momentum: {momentum_condition}
                - Volume OK: {volume_condition}
                - Price Sane: {price_sanity}
                """)
                
                # Apply all filters
                if (gap_up_condition and momentum_condition and 
                    volume_condition and price_sanity and reasonable_gap):
                    
                    # Calculate additional metrics
                    day_change = ltp - open_price
                    day_change_pct = ((ltp - open_price) / open_price) * 100 if open_price > 0 else 0
                    
                    # Add calculated fields to stock data
                    filtered_stock = stock.copy()
                    filtered_stock.update({
                        'percentage_change': round(percentage_change, 2),
                        'gap_up_percentage': round(gap_up_percentage, 2),
                        'day_change': round(day_change, 2),
                        'day_change_percentage': round(day_change_pct, 2),
                        'gap_up_condition': gap_up_condition,
                        'momentum_condition': momentum_condition,
                        'filter_timestamp': self._get_current_timestamp(),
                        'market_cap_category': self._categorize_by_price(ltp),
                        'volume_category': self._categorize_volume(volume)
                    })
                    
                    filtered_stocks.append(filtered_stock)
                    
                    logger.info(f"✅ {symbol} PASSED all filters:")
                    logger.info(f"   📈 Gap Up: ₹{open_price:.2f} > ₹{prev_day_high:.2f} ({gap_up_percentage:.2f}%)")
                    logger.info(f"   🚀 Momentum: {percentage_change:.2f}% (≥{self.min_percentage_increase}%)")
                    logger.info(f"   📊 Volume: {volume:,}, OI Change: {change_in_oi:,}")
                    
                else:
                    # Log why it failed for debugging
                    reasons = []
                    if not gap_up_condition:
                        reasons.append(f"No gap up: Open ₹{open_price:.2f} <= PDH ₹{prev_day_high:.2f}")
                    if not momentum_condition:
                        reasons.append(f"Low momentum: {percentage_change:.2f}% < {self.min_percentage_increase}%")
                    if not volume_condition:
                        reasons.append(f"Low volume: {volume:,}")
                    if not price_sanity:
                        reasons.append(f"Price issue: ₹{ltp:.2f}")
                    if not reasonable_gap:
                        reasons.append(f"Extreme gap: {gap_up_percentage:.2f}%")
                    
                    logger.debug(f"❌ {symbol} FAILED filters: {'; '.join(reasons)}")
                
            except Exception as e:
                logger.error(f"Error filtering stock {stock.get('symbol', 'Unknown')}: {e}")
                continue
        
        self.filtered_stocks = filtered_stocks
        
        # Enhanced logging
        total_scanned = len(stock_data)
        total_passed = len(filtered_stocks)
        success_rate = (total_passed / total_scanned * 100) if total_scanned > 0 else 0
        
        logger.info(f"🎯 REAL DATA Filter Results:")
        logger.info(f"   📊 Scanned: {total_scanned} stocks")
        logger.info(f"   ✅ Passed: {total_passed} stocks ({success_rate:.1f}%)")
        logger.info(f"   📈 Min % Required: {self.min_percentage_increase}%")
        
        if filtered_stocks:
            # Show top performers
            top_performers = sorted(filtered_stocks, key=lambda x: x['percentage_change'], reverse=True)[:3]
            logger.info(f"   🏆 Top Performers:")
            for i, stock in enumerate(top_performers, 1):
                logger.info(f"      {i}. {stock['symbol']}: +{stock['percentage_change']:.2f}% (Gap: +{stock['gap_up_percentage']:.2f}%)")
        
        return filtered_stocks
    
    def _categorize_by_price(self, price: float) -> str:
        """Categorize stocks by price range"""
        if price < 100:
            return "Small"
        elif price < 1000:
            return "Medium"
        elif price < 5000:
            return "Large"
        else:
            return "Premium"
    
    def _categorize_volume(self, volume: int) -> str:
        """Categorize stocks by volume"""
        if volume < 10000:
            return "Low"
        elif volume < 100000:
            return "Medium"
        elif volume < 1000000:
            return "High"
        else:
            return "Very High"
    
    def get_filtered_stocks(self) -> List[Dict[str, Any]]:
        """Return the filtered stocks"""
        return self.filtered_stocks
    
    def get_detailed_filter_summary(self) -> Dict[str, Any]:
        """Get detailed summary of filtering results with real data insights"""
        if not self.filtered_stocks:
            return {
                'total_filtered': 0,
                'avg_percentage_change': 0,
                'avg_gap_up': 0,
                'top_performers': [],
                'volume_analysis': {},
                'filter_criteria': {
                    'min_percentage_increase': self.min_percentage_increase,
                    'gap_up_required': True,
                    'volume_threshold': 1000
                }
            }
        
        # Calculate comprehensive statistics
        percentage_changes = [stock['percentage_change'] for stock in self.filtered_stocks]
        gap_ups = [stock.get('gap_up_percentage', 0) for stock in self.filtered_stocks]
        volumes = [stock.get('volume', 0) for stock in self.filtered_stocks]
        
        avg_change = sum(percentage_changes) / len(percentage_changes)
        avg_gap_up = sum(gap_ups) / len(gap_ups) if gap_ups else 0
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        
        # Volume analysis
        volume_categories = {}
        for stock in self.filtered_stocks:
            category = stock.get('volume_category', 'Unknown')
            volume_categories[category] = volume_categories.get(category, 0) + 1
        
        # Get top performers with more details
        top_performers = sorted(
            self.filtered_stocks, 
            key=lambda x: x['percentage_change'], 
            reverse=True
        )[:5]
        
        return {
            'total_filtered': len(self.filtered_stocks),
            'avg_percentage_change': round(avg_change, 2),
            'avg_gap_up_percentage': round(avg_gap_up, 2),
            'avg_volume': int(avg_volume),
            'max_percentage_change': max(percentage_changes),
            'min_percentage_change': min(percentage_changes),
            'volume_distribution': volume_categories,
            'top_performers': [
                {
                    'symbol': stock['symbol'],
                    'percentage_change': stock['percentage_change'],
                    'gap_up_percentage': stock.get('gap_up_percentage', 0),
                    'ltp': stock['ltp'],
                    'open_price': stock['open_price'],
                    'volume': stock.get('volume', 0),
                    'change_in_oi': stock.get('change_in_oi', 0),
                    'source': stock.get('source', 'unknown')
                }
                for stock in top_performers
            ],
            'filter_criteria': {
                'min_percentage_increase': self.min_percentage_increase,
                'gap_up_required': True,
                'volume_threshold': 1000,
                'price_range': '₹10 - ₹50,000',
                'max_gap_up': '20%'
            }
        }
    
    def get_filter_summary(self) -> Dict[str, Any]:
        """Get summary of filtering results (backward compatibility)"""
        return self.get_detailed_filter_summary()
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def update_filter_criteria(self, min_percentage: float = None, volume_threshold: int = None):
        """Update filter criteria"""
        if min_percentage is not None:
            self.min_percentage_increase = min_percentage
            logger.info(f"Updated minimum percentage increase to {min_percentage}%")
        
        if volume_threshold is not None:
            self.volume_threshold = volume_threshold
            logger.info(f"Updated volume threshold to {volume_threshold:,}")
    
    def validate_stock_data(self, stock: Dict[str, Any]) -> bool:
        """Validate if stock has required data for filtering"""
        required_fields = ['symbol', 'open_price', 'ltp', 'prev_close']
        
        for field in required_fields:
            if not stock.get(field):
                return False
            
            # Check if numeric fields are valid
            if field != 'symbol':
                try:
                    value = float(stock[field])
                    if value <= 0:
                        return False
                except (ValueError, TypeError):
                    return False
        
        return True
    
    def get_stocks_by_criteria(self, min_pct: float = None, max_gap: float = None) -> List[Dict[str, Any]]:
        """Get filtered stocks by specific criteria"""
        if not self.filtered_stocks:
            return []
        
        result = self.filtered_stocks.copy()
        
        if min_pct is not None:
            result = [stock for stock in result if stock.get('percentage_change', 0) >= min_pct]
        
        if max_gap is not None:
            result = [stock for stock in result if stock.get('gap_up_percentage', 0) <= max_gap]
        
        return result
    
    def get_stocks_by_volume(self, min_volume: int = 0) -> List[Dict[str, Any]]:
        """Get filtered stocks by minimum volume"""
        if not self.filtered_stocks:
            return []
        
        return [stock for stock in self.filtered_stocks if stock.get('volume', 0) >= min_volume]
    
    def export_filtered_data(self) -> List[Dict[str, Any]]:
        """Export filtered data in a clean format"""
        if not self.filtered_stocks:
            return []
        
        export_data = []
        for stock in self.filtered_stocks:
            export_data.append({
                'Symbol': stock['symbol'],
                'LTP': f"₹{stock['ltp']:.2f}",
                'Open': f"₹{stock['open_price']:.2f}",
                'Prev_Close': f"₹{stock['prev_close']:.2f}",
                'Prev_Day_High': f"₹{stock.get('prev_day_high', 0):.2f}",
                'Change_%': f"{stock.get('percentage_change', 0):.2f}%",
                'Gap_Up_%': f"{stock.get('gap_up_percentage', 0):.2f}%",
                'Volume': f"{stock.get('volume', 0):,}",
                'Change_in_OI': f"{stock.get('change_in_oi', 0):,}",
                'Source': stock.get('source', 'unknown'),
                'Timestamp': stock.get('filter_timestamp', 'unknown')
            })
        
        return export_data