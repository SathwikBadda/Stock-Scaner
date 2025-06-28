from typing import List, Dict, Any
import logging
from datetime import datetime
from database.db_manager import DatabaseManager
from config.settings import settings

logger = logging.getLogger(__name__)

class LoggerAgent:
    """Agent responsible for logging stock signals to database"""
    
    def __init__(self):
        self.db_manager = DatabaseManager(settings.DATABASE_PATH)
        self.logged_signals = []
        logger.info("LoggerAgent initialized")
    
    def log_signals(self, filtered_stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Log filtered stock signals to the database, overwriting previous signals."""
        try:
            # Overwrite: clear all previous signals first
            self.db_manager.clear_all_signals()
            logged_count = 0
            failed_count = 0
            for stock in filtered_stocks:
                try:
                    signal_data = {
                        'symbol': stock.get('symbol', ''),
                        'open_price': float(stock.get('open', 0)),
                        'ltp': float(stock.get('close', 0)),
                        'prev_close': float(stock.get('prev_close', 0)),
                        'prev_day_high': float(stock.get('prev_high', 0)),
                        'percentage_change': float(stock.get('oi_change_pct', 0)),  # Store OI % as percentage_change for this use-case
                        'volume': int(stock.get('volume', 0)),
                        'market_cap': float(stock.get('market_cap', 0)),
                        'source': stock.get('source', 'unknown')
                    }
                    if self._validate_signal_data(signal_data):
                        if self.db_manager.insert_signal(signal_data):
                            logged_count += 1
                            self.logged_signals.append(signal_data)
                            logger.debug(f"Logged signal for {signal_data['symbol']}")
                        else:
                            failed_count += 1
                            logger.error(f"Failed to insert signal for {signal_data['symbol']}")
                    else:
                        failed_count += 1
                        logger.warning(f"Invalid signal data for {stock.get('symbol', 'Unknown')}")
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error processing signal for {stock.get('symbol', 'Unknown')}: {e}")
            total_signals = logged_count + failed_count
            success_rate = (logged_count / total_signals * 100) if total_signals > 0 else 0
            summary = {
                'total_processed': total_signals,
                'successfully_logged': logged_count,
                'failed': failed_count,
                'success_rate': round(success_rate, 2),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            logger.info(f"Logging complete: {logged_count}/{total_signals} signals logged successfully ({success_rate:.1f}%)")
            return summary
        except Exception as e:
            logger.error(f"Error in log_signals: {e}")
            return {
                'total_processed': 0,
                'successfully_logged': 0,
                'failed': len(filtered_stocks),
                'success_rate': 0,
                'error': str(e),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def _validate_signal_data(self, signal_data: Dict[str, Any]) -> bool:
        """Validate signal data before logging"""
        try:
            # Check required fields
            required_fields = ['symbol', 'open_price', 'ltp', 'prev_close', 'percentage_change']
            
            for field in required_fields:
                if field not in signal_data:
                    logger.warning(f"Missing required field: {field}")
                    return False
                
                if field == 'symbol':
                    if not signal_data[field] or not isinstance(signal_data[field], str):
                        logger.warning(f"Invalid symbol: {signal_data[field]}")
                        return False
                else:
                    if not isinstance(signal_data[field], (int, float)) or signal_data[field] < 0:
                        logger.warning(f"Invalid {field}: {signal_data[field]}")
                        return False
            
            # Additional validation
            if signal_data['percentage_change'] < settings.MIN_PERCENTAGE_INCREASE:
                logger.warning(f"Percentage change {signal_data['percentage_change']} below minimum threshold")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating signal data: {e}")
            return False
    
    def get_recent_signals(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent signals from database"""
        try:
            return self.db_manager.get_recent_signals(hours)
        except Exception as e:
            logger.error(f"Error getting recent signals: {e}")
            return []
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            total_signals = self.db_manager.get_signal_count()
            recent_signals = len(self.db_manager.get_recent_signals(24))
            
            return {
                'total_signals': total_signals,
                'signals_last_24h': recent_signals,
                'database_path': settings.DATABASE_PATH,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {
                'total_signals': 0,
                'signals_last_24h': 0,
                'error': str(e),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def log_scan_session(self, session_data: Dict[str, Any]) -> bool:
        """Log a complete scan session"""
        try:
            # Create session log entry
            session_log = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_stocks_scanned': session_data.get('total_stocks', 0),
                'stocks_filtered': session_data.get('filtered_count', 0),
                'data_sources': session_data.get('sources', []),
                'scan_duration': session_data.get('duration', 0),
                'success': session_data.get('success', False)
            }
            
            logger.info(f"Scan session logged: {session_log}")
            return True
            
        except Exception as e:
            logger.error(f"Error logging scan session: {e}")
            return False
    
    def cleanup_old_signals(self, days: int = 30) -> int:
        """Clean up old signals from database"""
        try:
            # This would require additional database method
            # For now, just return 0
            logger.info(f"Cleanup requested for signals older than {days} days")
            return 0
            
        except Exception as e:
            logger.error(f"Error cleaning up old signals: {e}")
            return 0