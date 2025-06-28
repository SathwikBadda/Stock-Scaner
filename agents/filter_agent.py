from typing import List, Dict, Any
import logging
from config.settings import settings

logger = logging.getLogger(__name__)

class FilterAgent:
    """Agent responsible for filtering stocks based on REAL data strategy criteria"""
    
    def __init__(self):
        self.min_percentage_increase = settings.MIN_PERCENTAGE_INCREASE
        self.filtered_stocks = []
        logger.info("✅ FilterAgent initialized - focusing on REAL data filtering")
    
    def filter_stocks(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter stocks based on REAL data strategy:
        1. Open > Previous Day High (Gap Up)
        2. LTP >= 7% increase from previous close
        3. Only REAL data - no synthetic calculations
        """
        filtered_stocks = []
        
        logger.info(f"🎯 Starting REAL data filtering for {len(stock_data)} stocks...")
        
        if not stock_data:
            logger.warning("❌ No stock data provided for filtering!")
            return []
        
        # First, validate all stocks have real data
        valid_stocks = []
        for stock in stock_data:
            if self._validate_real_stock_data(stock):
                valid_stocks.append(stock)
            else:
                logger.debug(f"❌ Skipping {stock.get('symbol', 'Unknown')} - invalid real data")
        
        logger.info(f"📊 {len(valid_stocks)} stocks have valid REAL data")
        
        for stock in valid_stocks:
            try:
                symbol = stock.get('symbol', '')
                open_price = float(stock.get('open_price', 0))
                ltp = float(stock.get('ltp', 0))
                prev_close = float(stock.get('prev_close', 0))
                prev_day_high = float(stock.get('prev_day_high', 0))
                volume = int(stock.get('volume', 0))
                
                # CRITICAL: Only proceed if we have REAL previous day high
                if prev_day_high <= 0:
                    # Try to use prev_close as fallback only if it's reasonable
                    if prev_close > 0:
                        prev_day_high = prev_close
                        logger.debug(f"⚠️ {symbol}: Using prev_close as prev_day_high fallback")
                    else:
                        logger.debug(f"❌ {symbol}: No valid previous day high data")
                        continue
                
                # FILTER CONDITION 1: Gap Up - Open > Previous Day High
                gap_up_condition = open_price > prev_day_high
                gap_up_percentage = ((open_price - prev_day_high) / prev_day_high) * 100 if prev_day_high > 0 else 0
                
                # FILTER CONDITION 2: Momentum - LTP >= minimum percentage increase
                if prev_close > 0:
                    percentage_change = ((ltp - prev_close) / prev_close) * 100
                else:
                    logger.debug(f"❌ {symbol}: Invalid prev_close = {prev_close}")
                    continue
                
                momentum_condition = percentage_change >= self.min_percentage_increase
                
                # ADDITIONAL REAL DATA FILTERS
                # Filter 3: Volume validation (ensure real trading activity)
                volume_condition = volume > 1000
                
                # Filter 4: Price sanity (real stock price range)
                price_sanity = 10 <= ltp <= 50000
                
                # Filter 5: Gap should be reasonable (not data error)
                reasonable_gap = 0.1 <= gap_up_percentage <= 25  # Between 0.1% and 25%
                
                # Filter 6: Change should be reasonable
                reasonable_change = -50 <= percentage_change <= 100  # Between -50% and +100%
                
                # Log detailed REAL data analysis
                logger.debug(f"""
                REAL DATA Analysis for {symbol}:
                - LTP: ₹{ltp:.2f} | Open: ₹{open_price:.2f} | Prev Close: ₹{prev_close:.2f}
                - Prev Day High: ₹{prev_day_high:.2f} | Volume: {volume:,}
                - Gap Up %: {gap_up_percentage:.2f}% | Change %: {percentage_change:.2f}%
                - Source: {stock.get('source', 'unknown')}
                - Filters: Gap={gap_up_condition} | Momentum={momentum_condition} | Vol={volume_condition} | Price={price_sanity}
                """)
                
                # Apply all REAL data filters
                if (gap_up_condition and momentum_condition and 
                    volume_condition and price_sanity and reasonable_gap and reasonable_change):
                    
                    # Calculate additional real metrics
                    day_change = ltp - open_price
                    day_change_pct = ((ltp - open_price) / open_price) * 100 if open_price > 0 else 0
                    
                    # Add comprehensive data to filtered stock
                    filtered_stock = stock.copy()
                    filtered_stock.update({
                        'percentage_change': round(percentage_change, 2),
                        'gap_up_percentage': round(gap_up_percentage, 2),
                        'day_change': round(day_change, 2),
                        'day_change_percentage': round(day_change_pct, 2),
                        'gap_up_condition': gap_up_condition,
                        'momentum_condition': momentum_condition,
                        'filter_timestamp': self._get_current_timestamp(),
                        'data_quality': self._assess_data_quality(stock),
                        'filter_score': self._calculate_filter_score(stock, gap_up_percentage, percentage_change)
                    })
                    
                    filtered_stocks.append(filtered_stock)
                    
                    logger.info(f"✅ {symbol} PASSED all REAL data filters:")
                    logger.info(f"   📈 Gap Up: ₹{open_price:.2f} > ₹{prev_day_high:.2f} (+{gap_up_percentage:.2f}%)")
                    logger.info(f"   🚀 Momentum: {percentage_change:.2f}% (≥{self.min_percentage_increase}%)")
                    logger.info(f"   📊 Volume: {volume:,} | Quality: {filtered_stock['data_quality']}")
                    
                else:
                    # Log specific failure reasons for debugging
                    reasons = []
                    if not gap_up_condition:
                        reasons.append(f"No gap up: ₹{open_price:.2f} <= ₹{prev_day_high:.2f}")
                    if not momentum_condition:
                        reasons.append(f"Low momentum: {percentage_change:.2f}% < {self.min_percentage_increase}%")
                    if not volume_condition:
                        reasons.append(f"Low volume: {volume:,}")
                    if not price_sanity:
                        reasons.append(f"Price out of range: ₹{ltp:.2f}")
                    if not reasonable_gap:
                        reasons.append(f"Unreasonable gap: {gap_up_percentage:.2f}%")
                    if not reasonable_change:
                        reasons.append(f"Unreasonable change: {percentage_change:.2f}%")
                    
                    logger.debug(f"❌ {symbol} FAILED: {'; '.join(reasons)}")
                
            except Exception as e:
                logger.error(f"❌ Error filtering {stock.get('symbol', 'Unknown')}: {e}")
                continue
        
        self.filtered_stocks = filtered_stocks
        
        # Enhanced logging with real data insights
        total_scanned = len(valid_stocks)
        total_passed = len(filtered_stocks)
        success_rate = (total_passed / total_scanned * 100) if total_scanned > 0 else 0
        
        logger.info(f"🎯 REAL DATA Filter Results:")
        logger.info(f"   📊 Valid Real Data: {total_scanned} stocks")
        logger.info(f"   ✅ Passed Filters: {total_passed} stocks ({success_rate:.1f}%)")
        logger.info(f"   📈 Min Gap Up Required: > Previous Day High")
        logger.info(f"   🚀 Min % Change Required: {self.min_percentage_increase}%")
        
        if filtered_stocks:
            # Show top performers with real data
            top_performers = sorted(filtered_stocks, key=lambda x: x['percentage_change'], reverse=True)[:3]
            logger.info(f"   🏆 Top REAL Data Performers:")
            for i, stock in enumerate(top_performers, 1):
                quality = stock.get('data_quality', 'Unknown')
                source = stock.get('source', 'unknown')
                logger.info(f"      {i}. {stock['symbol']}: +{stock['percentage_change']:.2f}% | Gap: +{stock['gap_up_percentage']:.2f}% | Quality: {quality} | Source: {source}")
        
        return filtered_stocks
    
    def _validate_real_stock_data(self, stock: Dict[str, Any]) -> bool:
        """Validate that stock has real, usable data for filtering"""
        try:
            required_fields = ['symbol', 'open_price', 'ltp', 'prev_close']
            
            for field in required_fields:
                value = stock.get(field, 0)
                if not isinstance(value, (int, float)) or value <= 0:
                    return False
            
            # Check that source indicates real data
            source = stock.get('source', '').lower()
            if 'synthetic' in source or 'fake' in source or 'generated' in source:
                return False
            
            # Additional sanity checks
            ltp = stock['ltp']
            prev_close = stock['prev_close']
            
            # Prices should be in reasonable range
            if not (5 <= ltp <= 100000) or not (5 <= prev_close <= 100000):
                return False
            
            # Change should not be completely unrealistic
            if prev_close > 0:
                change_ratio = ltp / prev_close
                if not (0.3 <= change_ratio <= 3.0):  # Max 300% change either way
                    return False
            
            return True
            
        except Exception:
            return False
    
    def _assess_data_quality(self, stock: Dict[str, Any]) -> str:
        """Assess the quality of real data for this stock"""
        try:
            quality_score = 0
            
            # Source quality
            source = stock.get('source', '').lower()
            if 'nse' in source:
                quality_score += 3
            elif 'yfinance' in source:
                quality_score += 2
            else:
                quality_score += 1
            
            # Data completeness
            if stock.get('prev_day_high', 0) > 0:
                quality_score += 2
            if stock.get('volume', 0) > 1000:
                quality_score += 1
            if stock.get('total_oi', 0) > 0:
                quality_score += 2
            
            # Real data indicators
            if 'real' in source:
                quality_score += 2
            
            if quality_score >= 8:
                return "EXCELLENT"
            elif quality_score >= 6:
                return "GOOD"
            elif quality_score >= 4:
                return "FAIR"
            else:
                return "POOR"
                
        except Exception:
            return "UNKNOWN"
    
    def _calculate_filter_score(self, stock: Dict[str, Any], gap_up_pct: float, change_pct: float) -> float:
        """Calculate a filter score for ranking stocks"""
        try:
            score = 0.0
            
            # Gap up score (0-40 points)
            score += min(gap_up_pct * 2, 40)
            
            # Momentum score (0-40 points)
            score += min(change_pct * 2, 40)
            
            # Volume score (0-10 points)
            volume = stock.get('volume', 0)
            if volume > 100000:
                score += 10
            elif volume > 50000:
                score += 7
            elif volume > 10000:
                score += 5
            elif volume > 1000:
                score += 2
            
            # OI score (0-10 points)
            if stock.get('total_oi', 0) > 0:
                score += 10
            
            return round(score, 2)
            
        except Exception:
            return 0.0
    
    def get_filtered_stocks(self) -> List[Dict[str, Any]]:
        """Return the filtered stocks with real data"""
        return self.filtered_stocks
    
    def get_detailed_filter_summary(self) -> Dict[str, Any]:
        """Get detailed summary of filtering results with real data insights"""
        if not self.filtered_stocks:
            return {
                'total_filtered': 0,
                'avg_percentage_change': 0,
                'avg_gap_up': 0,
                'top_performers': [],
                'data_quality_distribution': {},
                'source_distribution': {},
                'filter_criteria': {
                    'min_percentage_increase': self.min_percentage_increase,
                    'gap_up_required': True,
                    'volume_threshold': 1000,
                    'real_data_only': True
                }
            }
        
        # Calculate comprehensive statistics from real data
        percentage_changes = [stock['percentage_change'] for stock in self.filtered_stocks]
        gap_ups = [stock.get('gap_up_percentage', 0) for stock in self.filtered_stocks]
        volumes = [stock.get('volume', 0) for stock in self.filtered_stocks]
        filter_scores = [stock.get('filter_score', 0) for stock in self.filtered_stocks]
        
        avg_change = sum(percentage_changes) / len(percentage_changes)
        avg_gap_up = sum(gap_ups) / len(gap_ups) if gap_ups else 0
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        avg_score = sum(filter_scores) / len(filter_scores) if filter_scores else 0
        
        # Data quality distribution
        quality_dist = {}
        for stock in self.filtered_stocks:
            quality = stock.get('data_quality', 'Unknown')
            quality_dist[quality] = quality_dist.get(quality, 0) + 1
        
        # Source distribution
        source_dist = {}
        for stock in self.filtered_stocks:
            source = stock.get('source', 'unknown')
            source_dist[source] = source_dist.get(source, 0) + 1
        
        # Get top performers with comprehensive data
        top_performers = sorted(
            self.filtered_stocks, 
            key=lambda x: x.get('filter_score', 0), 
            reverse=True
        )[:5]
        
        return {
            'total_filtered': len(self.filtered_stocks),
            'avg_percentage_change': round(avg_change, 2),
            'avg_gap_up_percentage': round(avg_gap_up, 2),
            'avg_volume': int(avg_volume),
            'avg_filter_score': round(avg_score, 2),
            'max_percentage_change': max(percentage_changes),
            'min_percentage_change': min(percentage_changes),
            'data_quality_distribution': quality_dist,
            'source_distribution': source_dist,
            'top_performers': [
                {
                    'symbol': stock['symbol'],
                    'percentage_change': stock['percentage_change'],
                    'gap_up_percentage': stock.get('gap_up_percentage', 0),
                    'ltp': stock['ltp'],
                    'open_price': stock['open_price'],
                    'volume': stock.get('volume', 0),
                    'total_oi': stock.get('total_oi', 0),
                    'change_in_oi': stock.get('change_in_oi', 0),
                    'data_quality': stock.get('data_quality', 'Unknown'),
                    'filter_score': stock.get('filter_score', 0),
                    'source': stock.get('source', 'unknown')
                }
                for stock in top_performers
            ],
            'filter_criteria': {
                'min_percentage_increase': self.min_percentage_increase,
                'gap_up_required': True,
                'volume_threshold': 1000,
                'price_range': '₹10 - ₹50,000',
                'max_gap_up': '25%',
                'real_data_only': True,
                'no_synthetic_data': True
            }
        }
    
    def get_filter_summary(self) -> Dict[str, Any]:
        """Get summary of filtering results (backward compatibility)"""
        return self.get_detailed_filter_summary()
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def update_filter_criteria(self, min_percentage: float = 0.0, volume_threshold: int = 0):
        """Update filter criteria"""
        if min_percentage is not None:
            self.min_percentage_increase = min_percentage
            logger.info(f"✅ Updated minimum percentage increase to {min_percentage}%")
        
        if volume_threshold is not None:
            self.volume_threshold = volume_threshold
            logger.info(f"✅ Updated volume threshold to {volume_threshold:,}")
    
    def get_stocks_by_quality(self, min_quality: str = "FAIR") -> List[Dict[str, Any]]:
        """Get filtered stocks by minimum data quality"""
        if not self.filtered_stocks:
            return []
        
        quality_order = {"POOR": 0, "FAIR": 1, "GOOD": 2, "EXCELLENT": 3}
        min_quality_score = quality_order.get(min_quality, 1)
        
        return [
            stock for stock in self.filtered_stocks 
            if quality_order.get(stock.get('data_quality', 'POOR'), 0) >= min_quality_score
        ]
    
    def get_stocks_by_source(self, preferred_source: str = "nse") -> List[Dict[str, Any]]:
        """Get filtered stocks by preferred data source"""
        if not self.filtered_stocks:
            return []
        
        return [
            stock for stock in self.filtered_stocks 
            if preferred_source.lower() in stock.get('source', '').lower()
        ]
    
    def get_stocks_with_oi_data(self) -> List[Dict[str, Any]]:
        """Get filtered stocks that have Open Interest data"""
        if not self.filtered_stocks:
            return []
        
        return [
            stock for stock in self.filtered_stocks 
            if stock.get('total_oi', 0) > 0
        ]
    
    def export_filtered_data_enhanced(self) -> List[Dict[str, Any]]:
        """Export filtered data in enhanced format with all real data"""
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
                'Total_OI': f"{stock.get('total_oi', 0):,}",
                'Change_in_OI': f"{stock.get('change_in_oi', 0):,}",
                'Data_Quality': stock.get('data_quality', 'Unknown'),
                'Filter_Score': f"{stock.get('filter_score', 0):.2f}",
                'Source': stock.get('source', 'unknown'),
                'Timestamp': stock.get('filter_timestamp', 'unknown')
            })
        
        return export_data
    
    def validate_filter_results(self) -> Dict[str, Any]:
        """Validate that all filtered results contain real data"""
        if not self.filtered_stocks:
            return {"valid": True, "issues": [], "total_stocks": 0}
        
        issues = []
        valid_count = 0
        
        for stock in self.filtered_stocks:
            stock_issues = []
            
            # Check for real data indicators
            source = stock.get('source', '').lower()
            if 'synthetic' in source or 'fake' in source:
                stock_issues.append("Synthetic data detected")
            
            # Check required fields
            required_fields = ['symbol', 'ltp', 'open_price', 'prev_close', 'percentage_change', 'gap_up_percentage']
            for field in required_fields:
                if not stock.get(field):
                    stock_issues.append(f"Missing {field}")
            
            # Check data quality
            if stock.get('data_quality') == 'POOR':
                stock_issues.append("Poor data quality")
            
            if stock_issues:
                issues.append({
                    'symbol': stock.get('symbol', 'Unknown'),
                    'issues': stock_issues
                })
            else:
                valid_count += 1
        
        return {
            "valid": len(issues) == 0,
            "total_stocks": len(self.filtered_stocks),
            "valid_stocks": valid_count,
            "invalid_stocks": len(issues),
            "issues": issues,
            "validation_passed": valid_count == len(self.filtered_stocks)
        }
    
    def filter_buy_sell_signals(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter stocks for BUY/SELL signals:
        BUY: Open > Prev High and OI > 7%
        SELL: Open < Prev Low and OI > 7%
        """
        filtered = []
        def safe_float(val):
            try:
                return float(val)
            except Exception:
                return 0.0
        for stock in stock_data:
            try:
                symbol = stock.get('symbol', '')
                open_price = safe_float(stock.get('open', 0))
                prev_high = safe_float(stock.get('prev_high', 0))
                prev_low = safe_float(stock.get('prev_low', 0))
                oi_change_pct = stock.get('oi_change_pct', 'N/A')
                try:
                    oi_change_pct_val = safe_float(str(oi_change_pct).replace('%',''))
                except Exception:
                    oi_change_pct_val = 0.0
                # BUY SIGNAL
                if open_price > prev_high and oi_change_pct_val > 7:
                    filtered.append({**stock, 'signal': 'BUY'})
                # SELL SIGNAL
                elif open_price < prev_low and oi_change_pct_val > 7:
                    filtered.append({**stock, 'signal': 'SELL'})
            except Exception as e:
                logger.error(f"Error in buy/sell filter for {stock.get('symbol', 'Unknown')}: {e}")
        return filtered