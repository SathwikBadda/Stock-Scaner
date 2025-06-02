from typing import List, Dict, Any
import logging
from data_sources.fyers_client import FyersClient
from data_sources.nse_client import NSEClient
from data_sources.nse_sprouts_client import NSESproutsClient
import concurrent.futures

logger = logging.getLogger(__name__)

class DataAgent:
    """Agent responsible for fetching stock data from multiple sources"""
    
    def __init__(self):
        self.fyers_client = FyersClient()
        self.nse_client = NSEClient()
        self.nse_sprouts_client = NSESproutsClient()
        self.fo_stocks = []
        self.stock_data = []
        logger.info("DataAgent initialized")
    
    def fetch_fo_stocks_from_all_sources(self) -> List[str]:
        """Fetch F&O stocks from all available sources"""
        all_fo_stocks = set()
        
        try:
            # Fetch from Fyers
            logger.info("Fetching F&O stocks from Fyers...")
            fyers_stocks = self.fyers_client.get_fo_stocks()
            all_fo_stocks.update(fyers_stocks)
            logger.info(f"Fyers: {len(fyers_stocks)} stocks")
            
        except Exception as e:
            logger.error(f"Error fetching from Fyers: {e}")
        
        try:
            # Fetch from NSE
            logger.info("Fetching F&O stocks from NSE...")
            nse_stocks = self.nse_client.get_fo_stocks()
            all_fo_stocks.update(nse_stocks)
            logger.info(f"NSE: {len(nse_stocks)} stocks")
            
        except Exception as e:
            logger.error(f"Error fetching from NSE: {e}")
        
        # Convert to list and store
        self.fo_stocks = list(all_fo_stocks)
        logger.info(f"Total unique F&O stocks collected: {len(self.fo_stocks)}")
        
        return self.fo_stocks
    
    def fetch_stock_data_parallel(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Fetch stock data from multiple sources in parallel"""
        all_stock_data = []
        
        def fetch_from_source(source_name, client, method_name, symbols_chunk):
            """Helper function to fetch data from a source"""
            try:
                logger.info(f"Fetching data from {source_name} for {len(symbols_chunk)} symbols...")
                method = getattr(client, method_name)
                return method(symbols_chunk)
            except Exception as e:
                logger.error(f"Error fetching from {source_name}: {e}")
                return []
        
        # Split symbols into chunks for parallel processing
        chunk_size = 20  # Process 20 symbols at a time
        symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            
            # Submit tasks for each source
            for chunk in symbol_chunks:
                # Fyers
                futures.append(
                    executor.submit(
                        fetch_from_source, 
                        "Fyers", 
                        self.fyers_client, 
                        "get_stock_data", 
                        chunk
                    )
                )
                
                # NSE
                futures.append(
                    executor.submit(
                        fetch_from_source, 
                        "NSE", 
                        self.nse_client, 
                        "get_stock_data", 
                        chunk
                    )
                )
            
            # Collect results
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)  # 30 second timeout
                    all_stock_data.extend(result)
                except Exception as e:
                    logger.error(f"Error in parallel fetch: {e}")
        
        # Also get pre-open data from NSE Sprouts
        try:
            logger.info("Fetching pre-open data from NSE Sprouts...")
            sprouts_data = self.nse_sprouts_client.get_pre_open_data()
            
            # Filter only F&O stocks from sprouts data
            fo_sprouts_data = [
                stock for stock in sprouts_data 
                if stock['symbol'] in symbols
            ]
            all_stock_data.extend(fo_sprouts_data)
            logger.info(f"NSE Sprouts: {len(fo_sprouts_data)} F&O stocks")
            
        except Exception as e:
            logger.error(f"Error fetching from NSE Sprouts: {e}")
        
        # Remove duplicates and merge data
        merged_data = self._merge_duplicate_stocks(all_stock_data)
        
        self.stock_data = merged_data
        logger.info(f"Total stock data collected: {len(merged_data)} stocks")
        
        return merged_data
    
    def _merge_duplicate_stocks(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge duplicate stock data from different sources"""
        merged = {}
        
        for stock in stock_data:
            symbol = stock['symbol']
            
            if symbol not in merged:
                merged[symbol] = stock
            else:
                # Merge data, preferring non-zero values
                existing = merged[symbol]
                
                for key, value in stock.items():
                    if key == 'source':
                        # Combine sources
                        existing_sources = existing.get('source', '').split(',')
                        if value not in existing_sources:
                            existing['source'] = f"{existing['source']},{value}"
                    elif isinstance(value, (int, float)) and value != 0:
                        # Use non-zero values, or newer data if both are non-zero
                        if existing.get(key, 0) == 0 or value != 0:
                            existing[key] = value
                    elif not existing.get(key) and value:
                        # Use non-empty values
                        existing[key] = value
        
        return list(merged.values())
    
    def get_all_data(self) -> Dict[str, Any]:
        """Get all F&O stocks and their data"""
        try:
            # Step 1: Fetch F&O stock symbols
            fo_stocks = self.fetch_fo_stocks_from_all_sources()
            
            if not fo_stocks:
                logger.error("No F&O stocks found")
                return {"fo_stocks": [], "stock_data": []}
            
            # Step 2: Fetch stock data for all symbols
            stock_data = self.fetch_stock_data_parallel(fo_stocks)
            
            return {
                "fo_stocks": fo_stocks,
                "stock_data": stock_data,
                "total_stocks": len(fo_stocks),
                "data_points": len(stock_data)
            }
            
        except Exception as e:
            logger.error(f"Error in get_all_data: {e}")
            return {"fo_stocks": [], "stock_data": []}
    
    def get_stock_data(self) -> List[Dict[str, Any]]:
        """Return the collected stock data"""
        return self.stock_data
    
    def get_fo_stocks(self) -> List[str]:
        """Return the F&O stock symbols"""
        return self.fo_stocks