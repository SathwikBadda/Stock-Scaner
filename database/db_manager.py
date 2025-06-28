import sqlite3
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Simple database manager for stock signals"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
        # Ensure database directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        self._init_database()
        logger.info(f"DatabaseManager initialized with path: {db_path}")
    
    def _init_database(self):
        """Initialize database tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create signals table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS signals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        open_price REAL NOT NULL,
                        ltp REAL NOT NULL,
                        prev_close REAL NOT NULL,
                        prev_day_high REAL NOT NULL,
                        percentage_change REAL NOT NULL,
                        volume INTEGER DEFAULT 0,
                        market_cap REAL DEFAULT 0,
                        source TEXT DEFAULT 'unknown',
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create sessions table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS scan_sessions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        total_stocks INTEGER DEFAULT 0,
                        filtered_stocks INTEGER DEFAULT 0,
                        data_sources TEXT DEFAULT '',
                        duration REAL DEFAULT 0,
                        success BOOLEAN DEFAULT 0
                    )
                ''')
                
                conn.commit()
                logger.info("Database tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def insert_signal(self, signal_data: Dict[str, Any]) -> bool:
        """Insert a signal into the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO signals (
                        symbol, open_price, ltp, prev_close, prev_day_high, 
                        percentage_change, volume, market_cap, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    signal_data.get('symbol', ''),
                    signal_data.get('open_price', 0),
                    signal_data.get('ltp', 0),
                    signal_data.get('prev_close', 0),
                    signal_data.get('prev_day_high', 0),
                    signal_data.get('percentage_change', 0),
                    signal_data.get('volume', 0),
                    signal_data.get('market_cap', 0),
                    signal_data.get('source', 'unknown')
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error inserting signal: {e}")
            return False
    
    def get_recent_signals(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent signals from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cutoff_time = datetime.now() - timedelta(hours=hours)
                
                cursor.execute('''
                    SELECT * FROM signals 
                    WHERE timestamp >= ? 
                    ORDER BY timestamp DESC
                ''', (cutoff_time,))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting recent signals: {e}")
            return []
    
    def get_signal_count(self) -> int:
        """Get total number of signals in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM signals')
                return cursor.fetchone()[0]
                
        except Exception as e:
            logger.error(f"Error getting signal count: {e}")
            return 0
    
    def insert_scan_session(self, session_data: Dict[str, Any]) -> bool:
        """Insert a scan session record"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO scan_sessions (
                        total_stocks, filtered_stocks, data_sources, duration, success
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    session_data.get('total_stocks', 0),
                    session_data.get('filtered_stocks', 0),
                    ','.join(session_data.get('data_sources', [])),
                    session_data.get('duration', 0),
                    session_data.get('success', False)
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error inserting scan session: {e}")
            return False
    
    def clear_all_signals(self) -> bool:
        """Delete all signals from the database (overwrite mode)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM signals')
                conn.commit()
            logger.info("All previous signals cleared from database.")
            return True
        except Exception as e:
            logger.error(f"Error clearing signals: {e}")
            return False