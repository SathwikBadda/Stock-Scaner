from typing import List, Dict, Any
import logging
import requests
import json
from datetime import datetime
from config.settings import settings

logger = logging.getLogger(__name__)

class AlertAgent:
    """Agent responsible for sending Telegram alerts"""
    
    def __init__(self):
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.telegram_api_url = f"https://api.telegram.org/bot{self.bot_token}"
        
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram credentials not configured properly")
        
        logger.info("AlertAgent initialized")
    
    def send_telegram_message(self, message: str) -> bool:
        """Send a message to Telegram"""
        try:
            if not self.bot_token or not self.chat_id:
                logger.error("Telegram credentials not configured")
                return False
            
            # Clean up the bot token - remove any extra characters
            clean_bot_token = self.bot_token.strip()
            clean_chat_id = self.chat_id.strip()
            
            url = f"https://api.telegram.org/bot{clean_bot_token}/sendMessage"
            
            payload = {
                'chat_id': clean_chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('ok'):
                    logger.info("Telegram message sent successfully")
                    return True
                else:
                    logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                    return False
            else:
                logger.error(f"Telegram HTTP error {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram message: {e}")
            return False
    
    def format_stock_alert(self, stock: Dict[str, Any]) -> str:
        """Format a single stock alert message"""
        try:
            symbol = stock.get('symbol', 'N/A')
            open_price = stock.get('open_price', 0)
            ltp = stock.get('ltp', 0)
            prev_close = stock.get('prev_close', 0)
            prev_day_high = stock.get('prev_day_high', 0)
            percentage_change = stock.get('percentage_change', 0)
            volume = stock.get('volume', 0)
            source = stock.get('source', 'Unknown')
            
            # Format volume
            if volume > 10000000:  # 1 crore
                volume_str = f"{volume/10000000:.1f}Cr"
            elif volume > 100000:  # 1 lakh
                volume_str = f"{volume/100000:.1f}L"
            else:
                volume_str = f"{volume:,}"
            
            alert_message = f"""🔔 <b>STOCK ALERT</b>
            
📈 <b>{symbol}</b>
💰 Open: ₹{open_price:.2f} | LTP: ₹{ltp:.2f}
📊 Prev Close: ₹{prev_close:.2f} | PDH: ₹{prev_day_high:.2f}
🚀 Change: <b>+{percentage_change:.2f}%</b>
📦 Volume: {volume_str}
🔗 Source: {source}

⏰ {datetime.now().strftime('%H:%M:%S')}"""
            
            return alert_message
            
        except Exception as e:
            logger.error(f"Error formatting stock alert: {e}")
            return f"🔔 STOCK ALERT\n📈 {stock.get('symbol', 'N/A')}\n❌ Error formatting data"
    
    def format_summary_alert(self, filtered_stocks: List[Dict[str, Any]]) -> str:
        """Format a summary alert with all filtered stocks"""
        try:
            if not filtered_stocks:
                return "🔍 <b>STOCK SCAN COMPLETE</b>\n\n❌ No stocks found matching the criteria.\n\n⏰ " + datetime.now().strftime('%H:%M:%S')
            
            # Header
            summary = f"""🔍 <b>STOCK SCAN RESULTS</b>
📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🎯 <b>{len(filtered_stocks)} stocks found!</b>

"""
            
            # Sort stocks by percentage change (descending)
            sorted_stocks = sorted(filtered_stocks, key=lambda x: x.get('percentage_change', 0), reverse=True)
            
            # Add each stock
            for i, stock in enumerate(sorted_stocks[:10], 1):  # Limit to top 10
                symbol = stock.get('symbol', 'N/A')
                ltp = stock.get('ltp', 0)
                open_price = stock.get('open_price', 0)
                percentage_change = stock.get('percentage_change', 0)
                
                summary += f"{i}. <b>{symbol}</b>\n"
                summary += f"   Open: ₹{open_price:.2f} → LTP: ₹{ltp:.2f}\n"
                summary += f"   🚀 <b>+{percentage_change:.2f}%</b>\n\n"
            
            # Add footer if more than 10 stocks
            if len(filtered_stocks) > 10:
                summary += f"... and {len(filtered_stocks) - 10} more stocks\n\n"
            
            # Add criteria info
            summary += f"📋 <b>Criteria:</b>\n"
            summary += f"• Open > Previous Day High\n"
            summary += f"• Change ≥ {settings.MIN_PERCENTAGE_INCREASE}%\n"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error formatting summary alert: {e}")
            return f"🔍 STOCK SCAN COMPLETE\n❌ Error formatting summary\n⏰ {datetime.now().strftime('%H:%M:%S')}"
    
    def send_alerts(self, filtered_stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Send alerts for filtered stocks"""
        try:
            if not self.bot_token or not self.chat_id:
                logger.error("Cannot send alerts - Telegram not configured")
                return {
                    'success': False,
                    'message': 'Telegram not configured',
                    'alerts_sent': 0
                }
            
            alerts_sent = 0
            
            # Send summary alert
            summary_message = self.format_summary_alert(filtered_stocks)
            if self.send_telegram_message(summary_message):
                alerts_sent += 1
                logger.info("Summary alert sent successfully")
            
            # Send individual alerts for top performers (limit to 5 to avoid spam)
            if filtered_stocks:
                top_stocks = sorted(
                    filtered_stocks, 
                    key=lambda x: x.get('percentage_change', 0), 
                    reverse=True
                )[:5]
                
                for stock in top_stocks:
                    individual_alert = self.format_stock_alert(stock)
                    if self.send_telegram_message(individual_alert):
                        alerts_sent += 1
                        logger.info(f"Individual alert sent for {stock.get('symbol', 'Unknown')}")
                    
                    # Small delay between messages
                    import time
                    time.sleep(1)
            
            return {
                'success': True,
                'message': f'Successfully sent {alerts_sent} alerts',
                'alerts_sent': alerts_sent,
                'stocks_count': len(filtered_stocks)
            }
            
        except Exception as e:
            logger.error(f"Error sending alerts: {e}")
            return {
                'success': False,
                'message': f'Error sending alerts: {str(e)}',
                'alerts_sent': 0
            }
    
    def send_system_alert(self, message: str, alert_type: str = "INFO") -> bool:
        """Send system/status alerts"""
        try:
            icons = {
                'INFO': 'ℹ️',
                'SUCCESS': '✅',
                'WARNING': '⚠️',
                'ERROR': '❌'
            }
            
            icon = icons.get(alert_type, 'ℹ️')
            
            formatted_message = f"""{icon} <b>SYSTEM ALERT</b>

🤖 <b>Stock Screener Bot</b>
📝 {message}

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
            
            return self.send_telegram_message(formatted_message)
            
        except Exception as e:
            logger.error(f"Error sending system alert: {e}")
            return False
    
    def test_telegram_connection(self) -> bool:
        """Test Telegram bot connection"""
        try:
            test_message = """🧪 <b>TEST MESSAGE</b>

✅ Telegram bot is working correctly!

🤖 Stock Screener Bot
⏰ """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return self.send_telegram_message(test_message)
            
        except Exception as e:
            logger.error(f"Error testing Telegram connection: {e}")
            return False