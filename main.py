#!/usr/bin/env python3
"""
Stock Screener - Main Orchestrator
LangChain-based automated stock scanner for F&O stocks
"""

import os
import sys
import logging
import schedule
import time
from datetime import datetime
from typing import Dict, Any

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from agents.data_agent import DataAgent
from agents.filter_agent import FilterAgent
from agents.alert_agent import AlertAgent
from agents.logger_agent import LoggerAgent

# Configure logging
def setup_logging():
    """Setup logging configuration"""
    os.makedirs(os.path.dirname(settings.LOG_FILE), exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(settings.LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger(__name__)

class StockScreenerOrchestrator:
    """Main orchestrator for the stock screening system"""
    
    def __init__(self):
        self.data_agent = DataAgent()
        self.filter_agent = FilterAgent()
        self.alert_agent = AlertAgent()
        self.logger_agent = LoggerAgent()
        self.scan_count = 0
        
        logger.info("🚀 Stock Screener Orchestrator initialized")
    
    def run_scan(self) -> Dict[str, Any]:
        """Run a complete stock scan cycle"""
        start_time = time.time()
        self.scan_count += 1
        
        logger.info(f"🔍 Starting scan #{self.scan_count} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        scan_results = {
            'scan_number': self.scan_count,
            'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'success': False,
            'total_stocks': 0,
            'filtered_stocks': 0,
            'alerts_sent': 0,
            'logged_signals': 0,
            'duration': 0,
            'errors': []
        }
        
        try:
            # Step 1: Data Collection
            logger.info("📊 Step 1: Collecting F&O stock data...")
            data_results = self.data_agent.get_all_data()
            
            if not data_results.get('stock_data'):
                error_msg = "No stock data collected"
                logger.error(error_msg)
                scan_results['errors'].append(error_msg)
                return scan_results
            
            scan_results['total_stocks'] = len(data_results['stock_data'])
            logger.info(f"✅ Collected data for {scan_results['total_stocks']} stocks")
            
            # Step 2: Filtering
            logger.info("🎯 Step 2: Applying filter criteria...")
            filtered_stocks = self.filter_agent.filter_stocks(data_results['stock_data'])
            
            scan_results['filtered_stocks'] = len(filtered_stocks)
            logger.info(f"✅ {scan_results['filtered_stocks']} stocks passed the filters")
            
            # Step 3: Alerts (if stocks found)
            if filtered_stocks:
                logger.info("📢 Step 3: Sending alerts...")
                alert_results = self.alert_agent.send_alerts(filtered_stocks)
                scan_results['alerts_sent'] = alert_results.get('alerts_sent', 0)
                
                if alert_results.get('success'):
                    logger.info(f"✅ {scan_results['alerts_sent']} alerts sent successfully")
                else:
                    error_msg = f"Alert sending failed: {alert_results.get('message')}"
                    logger.error(error_msg)
                    scan_results['errors'].append(error_msg)
            else:
                logger.info("📢 Step 3: No alerts to send (no stocks passed filters)")
            
            # Step 4: Logging
            logger.info("💾 Step 4: Logging signals to database...")
            log_results = self.logger_agent.log_signals(filtered_stocks)
            scan_results['logged_signals'] = log_results.get('successfully_logged', 0)
            
            if log_results.get('successfully_logged', 0) > 0:
                logger.info(f"✅ {scan_results['logged_signals']} signals logged to database")
            
            # Calculate duration
            scan_results['duration'] = round(time.time() - start_time, 2)
            scan_results['success'] = True
            scan_results['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Print summary
            self._print_scan_summary(scan_results, filtered_stocks)
            
            return scan_results
            
        except Exception as e:
            error_msg = f"Scan failed with error: {str(e)}"
            logger.error(error_msg)
            scan_results['errors'].append(error_msg)
            scan_results['duration'] = round(time.time() - start_time, 2)
            
            # Send error alert
            try:
                self.alert_agent.send_system_alert(
                    f"Scan #{self.scan_count} failed: {str(e)}", 
                    "ERROR"
                )
            except:
                pass
            
            return scan_results
    
    def _print_scan_summary(self, results: Dict[str, Any], filtered_stocks: list):
        """Print a formatted scan summary"""
        print("\n" + "="*60)
        print(f"📊 SCAN #{results['scan_number']} SUMMARY")
        print("="*60)
        print(f"⏰ Duration: {results['duration']}s")
        print(f"📈 Total Stocks Scanned: {results['total_stocks']}")
        print(f"🎯 Stocks Matching Criteria: {results['filtered_stocks']}")
        print(f"📢 Alerts Sent: {results['alerts_sent']}")
        print(f"💾 Signals Logged: {results['logged_signals']}")
        
        if filtered_stocks:
            print(f"\n🏆 TOP PERFORMERS:")
            top_stocks = sorted(filtered_stocks, key=lambda x: x.get('percentage_change', 0), reverse=True)[:5]
            
            for i, stock in enumerate(top_stocks, 1):
                print(f"{i}. {stock['symbol']}: +{stock['percentage_change']:.2f}% (₹{stock['ltp']:.2f})")
        
        if results.get('errors'):
            print(f"\n❌ Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"   - {error}")
        
        print("="*60 + "\n")
    
    def test_system(self) -> bool:
        """Test all system components"""
        logger.info("🧪 Testing system components...")
        
        try:
            # Test Telegram connection
            if self.alert_agent.test_telegram_connection():
                logger.info("✅ Telegram connection successful")
            else:
                logger.warning("⚠️ Telegram connection failed")
            
            # Test database
            db_stats = self.logger_agent.get_database_stats()
            logger.info(f"✅ Database connection successful - {db_stats['total_signals']} total signals")
            
            # Test data sources (basic check)
            fo_stocks = self.data_agent.fetch_fo_stocks_from_all_sources()
            if fo_stocks:
                logger.info(f"✅ Data sources accessible - {len(fo_stocks)} F&O stocks found")
            else:
                logger.warning("⚠️ No F&O stocks retrieved from data sources")
            
            logger.info("🧪 System test completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ System test failed: {e}")
            return False
    
    def start_scheduler(self):
        """Start the scheduled scanning"""
        logger.info(f"⏰ Starting scheduler - scans every {settings.SCAN_INTERVAL_MINUTES} minutes")
        
        # Schedule the scan
        schedule.every(settings.SCAN_INTERVAL_MINUTES).minutes.do(self.run_scan)
        
        # Send startup notification
        try:
            self.alert_agent.send_system_alert(
                f"Stock Screener started! Scanning every {settings.SCAN_INTERVAL_MINUTES} minutes.", 
                "SUCCESS"
            )
        except:
            pass
        
        # Run initial scan
        logger.info("🚀 Running initial scan...")
        self.run_scan()
        
        # Keep the scheduler running
        logger.info("💤 Scheduler active - waiting for next scan...")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("🛑 Scheduler stopped by user")
            try:
                self.alert_agent.send_system_alert("Stock Screener stopped.", "WARNING")
            except:
                pass

def main():
    """Main entry point"""
    setup_logging()
    
    logger.info("🚀 Starting Stock Screener System")
    
    # Initialize orchestrator
    orchestrator = StockScreenerOrchestrator()
    
    # Test system first
    if not orchestrator.test_system():
        logger.error("❌ System test failed - check configuration")
        sys.exit(1)
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "test":
            logger.info("🧪 Running test scan only...")
            results = orchestrator.run_scan()
            sys.exit(0 if results['success'] else 1)
        
        elif command == "once":
            logger.info("🔍 Running single scan...")
            results = orchestrator.run_scan()
            sys.exit(0 if results['success'] else 1)
        
        elif command == "schedule":
            logger.info("⏰ Starting scheduled mode...")
            orchestrator.start_scheduler()
        
        else:
            print("Usage: python main.py [test|once|schedule]")
            print("  test     - Test system and run one scan")
            print("  once     - Run single scan and exit")
            print("  schedule - Start scheduled scanning (default)")
            sys.exit(1)
    else:
        # Default: start scheduler
        orchestrator.start_scheduler()

if __name__ == "__main__":
    main()