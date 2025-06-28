from typing import List, Dict, Any
import logging
from data_sources.nse_client import NSEClient

logger = logging.getLogger(__name__)

class DataAgent:
    """Agent responsible for fetching REAL stock data from NSE and Yahoo Finance (fallback)"""
    
    def __init__(self):
        self.nse_client = NSEClient()
        self.fo_stocks = []
        self.stock_data = []
        logger.info("✅ DataAgent initialized - using NSE and Yahoo Finance only")
    
    def fetch_fo_stocks_from_all_sources(self) -> List[str]:
        """Fetch F&O stocks from NSE (robust)"""
        try:
            logger.info("📊 Fetching F&O stocks from NSE...")
            nse_stocks = self.nse_client.get_fo_stocks()
            if nse_stocks:
                self.fo_stocks = nse_stocks
                logger.info(f"✅ NSE: {len(nse_stocks)} stocks")
            else:
                logger.warning("⚠️ NSE: No stocks returned")
            return self.fo_stocks
        except Exception as e:
            logger.error(f"❌ Error fetching F&O stocks: {e}")
            return []
    
    def fetch_stock_data_real_only(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Fetch REAL stock data using NSE and Yahoo Finance (fallback)"""
        try:
            logger.info(f"🎯 Fetching REAL data for {len(symbols)} symbols...")
            all_stock_data = self.nse_client.get_stock_data(symbols)
            if all_stock_data:
                self.stock_data = all_stock_data
                logger.info(f"✅ Data collection complete: {len(all_stock_data)} stocks")
            else:
                logger.error("❌ No real stock data available - check data sources!")
            return self.stock_data
        except Exception as e:
            logger.error(f"❌ Error fetching stock data: {e}")
            return []
    
    def get_all_data(self) -> Dict[str, Any]:
        """Get all F&O stocks and their REAL data"""
        import time  # Fix: Ensure time is imported
        try:
            start_time = time.time()
            
            # Step 1: Fetch F&O stock symbols
            logger.info("🎯 Step 1: Fetching F&O stock symbols...")
            fo_stocks = self.fetch_fo_stocks_from_all_sources()
            
            if not fo_stocks:
                logger.error("❌ No F&O stocks found from any source!")
                return {"fo_stocks": [], "stock_data": [], "error": "No F&O stocks available"}
            
            # Step 2: Fetch REAL stock data for all symbols
            logger.info("🎯 Step 2: Fetching REAL stock data...")
            stock_data = self.fetch_stock_data_real_only(fo_stocks)
            
            duration = time.time() - start_time
            
            result = {
                "fo_stocks": fo_stocks,
                "stock_data": stock_data,
                "total_stocks": len(fo_stocks),
                "data_points": len(stock_data),
                "duration": round(duration, 2),
                "success": len(stock_data) > 0
            }
            
            if stock_data:
                # Calculate data quality metrics
                real_data_count = len([s for s in stock_data if 'real' in s.get('source', '').lower()])
                oi_data_count = len([s for s in stock_data if s.get('total_oi', 0) > 0])
                prev_day_count = len([s for s in stock_data if s.get('prev_day_high', 0) > 0])
                
                result.update({
                    "real_data_percentage": round((real_data_count / len(stock_data)) * 100, 1),
                    "oi_data_count": oi_data_count,
                    "prev_day_data_count": prev_day_count,
                    "data_quality": "HIGH" if real_data_count > len(stock_data) * 0.8 else "MEDIUM"
                })
                
                logger.info(f"✅ Data collection successful:")
                logger.info(f"   📊 F&O Stocks: {len(fo_stocks)}")
                logger.info(f"   📈 Real Data: {len(stock_data)} stocks ({result['real_data_percentage']}% real)")
                logger.info(f"   📋 OI Data: {oi_data_count} stocks")
                logger.info(f"   📅 Prev Day Data: {prev_day_count} stocks")
                logger.info(f"   ⏱️ Duration: {duration:.2f}s")
            else:
                logger.error("❌ No real stock data collected!")
                result["error"] = "No real stock data available"
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in get_all_data: {e}")
            return {
                "fo_stocks": [], 
                "stock_data": [], 
                "error": str(e),
                "success": False
            }
    
    def get_stock_data(self) -> List[Dict[str, Any]]:
        """Return the collected REAL stock data"""
        return self.stock_data
    
    def get_fo_stocks(self) -> List[str]:
        """Return the F&O stock symbols"""
        return self.fo_stocks
    
    def get_data_quality_report(self) -> Dict[str, Any]:
        """Get detailed data quality report"""
        if not self.stock_data:
            return {"error": "No data available"}
        
        total_stocks = len(self.stock_data)
        
        # Count different data types
        real_sources = len([s for s in self.stock_data if 'real' in s.get('source', '').lower()])
        nse_sources = len([s for s in self.stock_data if 'nse' in s.get('source', '').lower()])
        yfinance_sources = len([s for s in self.stock_data if 'yfinance' in s.get('source', '').lower()])
        
        # Count data completeness
        complete_ohlc = len([s for s in self.stock_data if all([
            s.get('open_price', 0) > 0,
            s.get('high_price', 0) > 0,
            s.get('low_price', 0) > 0,
            s.get('ltp', 0) > 0
        ])])
        
        prev_day_complete = len([s for s in self.stock_data if all([
            s.get('prev_day_high', 0) > 0,
            s.get('prev_day_open', 0) > 0,
            s.get('prev_day_low', 0) > 0
        ])])
        
        oi_available = len([s for s in self.stock_data if s.get('total_oi', 0) > 0])
        volume_available = len([s for s in self.stock_data if s.get('volume', 0) > 0])
        
        return {
            "total_stocks": total_stocks,
            "source_breakdown": {
                "real_data_sources": real_sources,
                "nse_sources": nse_sources,
                "yfinance_sources": yfinance_sources
            },
            "data_completeness": {
                "complete_ohlc": complete_ohlc,
                "prev_day_complete": prev_day_complete,
                "oi_available": oi_available,
                "volume_available": volume_available
            },
            "quality_percentages": {
                "real_data": round((real_sources / total_stocks) * 100, 1),
                "complete_ohlc": round((complete_ohlc / total_stocks) * 100, 1),
                "prev_day_data": round((prev_day_complete / total_stocks) * 100, 1),
                "oi_coverage": round((oi_available / total_stocks) * 100, 1)
            }
        }