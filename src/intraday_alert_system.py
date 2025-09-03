"""
Intraday Alert System - Complete Implementation
Monitors price breaks against morning forecast levels and correlates with news
Uses free Yahoo Finance API + existing news infrastructure
Includes spam prevention and professional alert generation
"""

import os
import sqlite3
import yfinance as yf
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import pytz
import time
import json
from typing import Dict, List, Optional, Tuple
import snowflake.connector

load_dotenv()

class IntradayAlertSystem:
    """Complete intraday alert system with spam prevention"""
    
    def __init__(self):
        """Initialize intraday alert system"""
        self.et_tz = pytz.timezone('US/Eastern')
        
        # Alert state tracking to prevent spam
        self.last_alert_state = {
            'support_breaks': [],
            'resistance_breaks': [],
            'bias_contradiction': None,
            'last_alert_time': None
        }
        self.min_alert_interval = 3600  # Minimum 1 hour between similar alerts
        
        # Snowflake connection for forecast data
        self.conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
        
        # Email configuration
        self.smtp_config = {
            'server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'port': int(os.getenv('SMTP_PORT', '587')),
            'user': os.getenv('SMTP_USER'),
            'password': os.getenv('SMTP_PASS'),
            'from_email': os.getenv('FROM_EMAIL', 'alerts@zenmarket.ai'),
            'from_name': 'ZenMarket Intraday Alert'
        }
        
        # Alert thresholds and settings
        self.price_check_interval = 15 * 60  # 15 minutes in seconds
        self.news_lookback_hours = 2  # Look back 2 hours for news correlation
        self.min_price_move_pct = 0.5  # Minimum 0.5% move to trigger alert
        
        # Initialize news database path
        self.news_db_path = 'data/news.db'
        
    def is_market_hours(self) -> bool:
        """Check if current time is within market hours (9:30 AM - 4:00 PM ET)"""
        now = datetime.now(self.et_tz)
        
        # Check if weekend
        if now.weekday() >= 5:
            return False
            
        # Check market hours
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_open <= now <= market_close
    
    def get_current_price(self, symbol: str = '^GSPC') -> Optional[float]:
        """Get current price using Yahoo Finance"""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1d', interval='1m')
            
            if data.empty:
                return None
                
            return float(data['Close'].iloc[-1])
            
        except Exception as e:
            print(f"Error fetching price for {symbol}: {e}")
            return None
    
    def get_current_vix(self) -> Optional[float]:
        """Get current VIX level"""
        return self.get_current_price('^VIX')
    
    def get_morning_forecast(self) -> Optional[Dict]:
        """Get today's morning forecast levels from database"""
        try:
            with snowflake.connector.connect(**self.conn_params) as conn:
                cur = conn.cursor()
                
                today = datetime.now(self.et_tz).strftime('%Y-%m-%d')
                
                # Try to get today's forecast first
                cur.execute("""
                    SELECT DATE, FORECAST_BIAS, SUPPORT_LEVELS, RESISTANCE_LEVELS,
                           ATM_STRADDLE, RSI_CONTEXT, NOTES
                    FROM FORECAST_JOBS
                    WHERE DATE = %s
                    ORDER BY FORECAST_TS DESC
                    LIMIT 1
                """, (today,))
                
                result = cur.fetchone()
                
                # If no forecast for today, get the most recent one
                if not result:
                    print(f"No forecast for {today}, using most recent available")
                    cur.execute("""
                        SELECT DATE, FORECAST_BIAS, SUPPORT_LEVELS, RESISTANCE_LEVELS,
                               ATM_STRADDLE, RSI_CONTEXT, NOTES
                        FROM FORECAST_JOBS
                        ORDER BY FORECAST_TS DESC
                        LIMIT 1
                    """)
                    result = cur.fetchone()
                
                if not result:
                    return None
                
                date, bias, support, resistance, straddle, rsi, notes = result
                
                # Parse levels - handle both string formats and potential None values
                support_levels = []
                resistance_levels = []
                
                if support:
                    if isinstance(support, str):
                        # Handle comma-separated format: "6420, 6400"
                        support_levels = [float(x.strip()) for x in support.split(',') if x.strip().replace('.', '').replace('-', '').isdigit()]
                    else:
                        # Handle case where it might already be a number
                        try:
                            support_levels = [float(support)]
                        except:
                            support_levels = []
                
                if resistance:
                    if isinstance(resistance, str):
                        # Handle comma-separated format: "6520, 6540"
                        resistance_levels = [float(x.strip()) for x in resistance.split(',') if x.strip().replace('.', '').replace('-', '').isdigit()]
                    else:
                        # Handle case where it might already be a number
                        try:
                            resistance_levels = [float(resistance)]
                        except:
                            resistance_levels = []
                
                return {
                    'date': date,
                    'bias': bias,
                    'support_levels': support_levels,
                    'resistance_levels': resistance_levels,
                    'straddle': float(straddle) if straddle else None,
                    'rsi_context': rsi,
                    'notes': notes
                }
                
        except Exception as e:
            print(f"Error getting morning forecast: {e}")
            return None
    
    def get_recent_news(self, hours_back: int = 2) -> List[Dict]:
        """Get recent news articles from local database"""
        try:
            if not os.path.exists(self.news_db_path):
                return []
                
            conn = sqlite3.connect(self.news_db_path)
            cur = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            cur.execute("""
                SELECT title, description, url, published_at, source
                FROM articles
                WHERE published_at >= ?
                ORDER BY published_at DESC
                LIMIT 20
            """, (cutoff_time.isoformat(),))
            
            articles = []
            for row in cur.fetchall():
                title, desc, url, pub_at, source = row
                articles.append({
                    'title': title,
                    'description': desc,
                    'url': url,
                    'published_at': pub_at,
                    'source': source
                })
            
            conn.close()
            return articles
            
        except Exception as e:
            print(f"Error getting recent news: {e}")
            return []
    
    def analyze_price_break(self, current_price: float, forecast: Dict) -> Optional[Dict]:
        """Analyze if current price breaks forecast levels"""
        breaks = []
        
        # Check support breaks (price below support)
        for support in forecast['support_levels']:
            if current_price < support:
                pct_break = ((support - current_price) / support) * 100
                if pct_break >= self.min_price_move_pct:
                    breaks.append({
                        'type': 'support_break',
                        'level': support,
                        'current_price': current_price,
                        'break_pct': pct_break,
                        'severity': 'critical' if pct_break > 1.0 else 'moderate'
                    })
        
        # Check resistance breaks (price above resistance)
        for resistance in forecast['resistance_levels']:
            if current_price > resistance:
                pct_break = ((current_price - resistance) / resistance) * 100
                if pct_break >= self.min_price_move_pct:
                    breaks.append({
                        'type': 'resistance_break',
                        'level': resistance,
                        'current_price': current_price,
                        'break_pct': pct_break,
                        'severity': 'critical' if pct_break > 1.0 else 'moderate'
                    })
        
        # Check bias contradiction
        bias_contradiction = None
        if forecast['bias'].lower() == 'bearish' and breaks and breaks[0]['type'] == 'resistance_break':
            bias_contradiction = {
                'forecast_bias': forecast['bias'],
                'actual_direction': 'bullish_breakout',
                'contradiction_severity': 'high'
            }
        elif forecast['bias'].lower() == 'bullish' and breaks and breaks[0]['type'] == 'support_break':
            bias_contradiction = {
                'forecast_bias': forecast['bias'],
                'actual_direction': 'bearish_breakdown',
                'contradiction_severity': 'high'
            }
        
        if breaks or bias_contradiction:
            return {
                'breaks': breaks,
                'bias_contradiction': bias_contradiction,
                'analysis_time': datetime.now(self.et_tz)
            }
        
        return None
    
    def correlate_with_news(self, price_analysis: Dict) -> List[Dict]:
        """Find news articles that might explain the price move"""
        recent_news = self.get_recent_news(self.news_lookback_hours)
        
        # Keywords that suggest market-moving news
        market_keywords = [
            'fed', 'federal reserve', 'powell', 'fomc', 'rate', 'inflation',
            'cpi', 'ppi', 'employment', 'jobs', 'earnings', 'guidance',
            'recession', 'gdp', 'economic', 'treasury', 'bond', 'yield',
            'bank', 'financial', 'crisis', 'emergency', 'breaking'
        ]
        
        relevant_news = []
        for article in recent_news:
            relevance_score = 0
            title_lower = article['title'].lower()
            desc_lower = (article['description'] or '').lower()
            
            # Score based on keyword matches
            for keyword in market_keywords:
                if keyword in title_lower:
                    relevance_score += 3
                if keyword in desc_lower:
                    relevance_score += 1
            
            # Higher score for recent articles
            try:
                pub_time = datetime.fromisoformat(article['published_at'].replace('Z', '+00:00'))
                hours_ago = (datetime.now(pytz.UTC) - pub_time).total_seconds() / 3600
                if hours_ago < 1:
                    relevance_score += 2
                elif hours_ago < 2:
                    relevance_score += 1
            except:
                pass  # Skip if date parsing fails
            
            if relevance_score > 0:
                article['relevance_score'] = relevance_score
                relevant_news.append(article)
        
        # Sort by relevance score
        relevant_news.sort(key=lambda x: x['relevance_score'], reverse=True)
        return relevant_news[:5]  # Return top 5 most relevant
    
    def should_send_alert(self, price_analysis: Dict) -> bool:
        """Determine if alert should be sent based on state tracking"""
        current_time = datetime.now(self.et_tz)
        
        # Check if minimum time has passed since last alert
        if (self.last_alert_state['last_alert_time'] and 
            (current_time - self.last_alert_state['last_alert_time']).total_seconds() < self.min_alert_interval):
            return False
        
        # Check if this is a new condition vs existing one
        current_breaks = price_analysis.get('breaks', [])
        current_contradiction = price_analysis.get('bias_contradiction')
        
        # Compare with last alert state
        new_condition = False
        
        # Check for new support breaks
        for break_info in current_breaks:
            if break_info['type'] == 'support_break':
                # Check if this level was already broken
                if not any(abs(break_info['level'] - old_break) < 5 
                          for old_break in self.last_alert_state['support_breaks']):
                    new_condition = True
                    
            elif break_info['type'] == 'resistance_break':
                # Check if this level was already broken  
                if not any(abs(break_info['level'] - old_break) < 5 
                          for old_break in self.last_alert_state['resistance_breaks']):
                    new_condition = True
        
        # Check for new bias contradiction
        if (current_contradiction and 
            current_contradiction != self.last_alert_state['bias_contradiction']):
            new_condition = True
            
        return new_condition
    
    def update_alert_state(self, price_analysis: Dict):
        """Update alert state tracking after sending alert"""
        current_time = datetime.now(self.et_tz)
        
        # Update broken levels
        for break_info in price_analysis.get('breaks', []):
            if break_info['type'] == 'support_break':
                self.last_alert_state['support_breaks'].append(break_info['level'])
            elif break_info['type'] == 'resistance_break':
                self.last_alert_state['resistance_breaks'].append(break_info['level'])
        
        # Update bias contradiction
        self.last_alert_state['bias_contradiction'] = price_analysis.get('bias_contradiction')
        
        # Update last alert time
        self.last_alert_state['last_alert_time'] = current_time
    
    def generate_alert_html(self, price_analysis: Dict, forecast: Dict, 
                           correlated_news: List[Dict]) -> str:
        """Generate HTML alert email"""
        current_time = datetime.now(self.et_tz).strftime('%H:%M ET')
        
        # Determine alert type and styling
        breaks = price_analysis['breaks']
        bias_contradiction = price_analysis['bias_contradiction']
        
        if breaks:
            primary_break = breaks[0]
            if primary_break['type'] == 'support_break':
                alert_type = "SUPPORT BREAK"
                alert_color = "#dc2626"
                alert_icon = "⬇️"
            else:
                alert_type = "RESISTANCE BREAK"
                alert_color = "#059669"
                alert_icon = "⬆️"
        else:
            alert_type = "BIAS CONTRADICTION"
            alert_color = "#d97706"
            alert_icon = "🔄"
        
        # News section
        news_html = ""
        if correlated_news:
            news_html = '<h3 style="color: #374151; margin-top: 20px;">📰 Potential News Catalyst</h3>'
            for i, article in enumerate(correlated_news[:3]):
                try:
                    pub_time = datetime.fromisoformat(article['published_at'].replace('Z', '+00:00'))
                    time_ago = pub_time.strftime('%H:%M ET')
                except:
                    time_ago = "Recent"
                    
                news_html += f"""
                <div style="background: #f9fafb; border-left: 4px solid #6b7280; padding: 12px; margin: 8px 0;">
                    <strong>{article['title']}</strong><br>
                    <small style="color: #6b7280;">{article['source']} • {time_ago} • Relevance: {article['relevance_score']}/10</small>
                    {f'<p style="margin: 8px 0 0 0; font-size: 13px;">{article["description"]}</p>' if article['description'] else ''}
                </div>
                """
        
        # Break details
        break_html = ""
        if breaks:
            break_html = f"""
            <div style="background: #fee2e2; border: 1px solid #fecaca; padding: 16px; border-radius: 6px; margin: 16px 0;">
                <h3 style="margin: 0 0 8px 0; color: #991b1b;">🎯 Level Break Details</h3>
                <p style="margin: 0;"><strong>Level:</strong> {breaks[0]['level']}</p>
                <p style="margin: 4px 0;"><strong>Current Price:</strong> {breaks[0]['current_price']:.2f}</p>
                <p style="margin: 4px 0;"><strong>Break Size:</strong> {breaks[0]['break_pct']:.2f}%</p>
                <p style="margin: 4px 0;"><strong>Severity:</strong> {breaks[0]['severity'].upper()}</p>
            </div>
            """
        
        # Bias contradiction
        contradiction_html = ""
        if bias_contradiction:
            contradiction_html = f"""
            <div style="background: #fef3c7; border: 1px solid #fcd34d; padding: 16px; border-radius: 6px; margin: 16px 0;">
                <h3 style="margin: 0 0 8px 0; color: #92400e;">🔄 Forecast Contradiction</h3>
                <p style="margin: 0;"><strong>Morning Bias:</strong> {bias_contradiction['forecast_bias']}</p>
                <p style="margin: 4px 0;"><strong>Actual Move:</strong> {bias_contradiction['actual_direction'].replace('_', ' ').title()}</p>
                <p style="margin: 4px 0;"><strong>Contradiction Level:</strong> {bias_contradiction['contradiction_severity'].upper()}</p>
            </div>
            """
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>{alert_type} Alert - {current_time}</title>
        </head>
        <body style="font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; background-color: #f8fafc;">
            <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                
                <!-- Alert Header -->
                <div style="background: {alert_color}; color: white; padding: 20px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">{alert_icon} {alert_type}</h1>
                    <p style="margin: 8px 0 0 0; opacity: 0.9;">Market Alert • {current_time}</p>
                </div>
                
                <!-- Alert Details -->
                <div style="padding: 24px;">
                    <h2 style="margin: 0 0 16px 0; color: #1f2937;">Alert Summary</h2>
                    <p style="margin: 0 0 16px 0; color: #4b5563; line-height: 1.6;">
                        Price action is contradicting this morning's forecast. Review your positions and consider risk management.
                    </p>
                    
                    {break_html}
                    {contradiction_html}
                    
                    <!-- Morning Forecast Context -->
                    <div style="background: #f0f9ff; border-left: 4px solid #0ea5e9; padding: 16px; margin: 20px 0;">
                        <h3 style="margin: 0 0 8px 0; color: #0c4a6e;">🌅 Morning Forecast Context</h3>
                        <p style="margin: 0;"><strong>Bias:</strong> {forecast['bias']}</p>
                        <p style="margin: 4px 0;"><strong>Support:</strong> {', '.join(map(str, forecast['support_levels']))}</p>
                        <p style="margin: 4px 0;"><strong>Resistance:</strong> {', '.join(map(str, forecast['resistance_levels']))}</p>
                        <p style="margin: 4px 0;"><strong>Notes:</strong> {forecast['notes'] or 'None'}</p>
                    </div>
                    
                    {news_html}
                </div>
                
                <!-- Footer -->
                <div style="background: #f9fafb; padding: 16px; text-align: center; border-top: 1px solid #e5e7eb;">
                    <p style="margin: 0; color: #6b7280; font-size: 12px;">
                        ZenMarket Intraday Alert • Generated at {current_time}
                    </p>
                    <p style="margin: 4px 0 0 0; color: #9ca3af; font-size: 11px;">
                        Educational purposes only • Not investment advice
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def send_alert(self, html_content: str, alert_type: str):
        """Send alert email"""
        recipients = []
        if os.getenv("EMAIL_TO"):
            recipients = [email.strip() for email in os.getenv("EMAIL_TO").split(",")]
        
        if not recipients:
            print("No alert recipients configured")
            return
        
        current_time = datetime.now(self.et_tz).strftime('%H:%M ET')
        subject = f"🚨 {alert_type} Alert - {current_time}"
        
        for recipient in recipients:
            try:
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From'] = f"{self.smtp_config['from_name']} <{self.smtp_config['from_email']}>"
                msg['To'] = recipient
                
                html_part = MIMEText(html_content, 'html')
                msg.attach(html_part)
                
                with smtplib.SMTP(self.smtp_config['server'], self.smtp_config['port']) as server:
                    server.starttls()
                    server.login(self.smtp_config['user'], self.smtp_config['password'])
                    server.send_message(msg)
                
                print(f"Alert sent to {recipient}")
                
            except Exception as e:
                print(f"Failed to send alert to {recipient}: {e}")
    
    def run_price_check(self):
        """Run a single price check cycle"""
        if not self.is_market_hours():
            print("Outside market hours - skipping price check")
            return
        
        print(f"Running price check at {datetime.now(self.et_tz).strftime('%H:%M:%S ET')}")
        
        # Get current data
        current_price = self.get_current_price('^GSPC')
        if not current_price:
            print("Could not fetch current price")
            return
        
        current_vix = self.get_current_vix()
        forecast = self.get_morning_forecast()
        
        if not forecast:
            print("No morning forecast available")
            return
        
        vix_display = f"{current_vix:.2f}" if current_vix else "N/A"
        print(f"SPX: {current_price:.2f}, VIX: {vix_display}")
        
        # Analyze price action
        price_analysis = self.analyze_price_break(current_price, forecast)
        
        if price_analysis:
            print("Price break detected - checking if alert should be sent")
            
            # Check if we should send alert (avoid spam)
            if not self.should_send_alert(price_analysis):
                print("Alert suppressed - similar condition already alerted or too soon")
                return
            
            print("New condition detected - generating alert")
            
            # Correlate with news
            correlated_news = self.correlate_with_news(price_analysis)
            
            # Generate and send alert
            alert_type = "LEVEL BREAK" if price_analysis['breaks'] else "BIAS CONTRADICTION"
            html_content = self.generate_alert_html(price_analysis, forecast, correlated_news)
            
            # Save alert preview
            timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
            preview_dir = "audit_exports/intraday_alerts"
            os.makedirs(preview_dir, exist_ok=True)
            
            preview_path = f"{preview_dir}/{timestamp}_intraday_alert.html"
            with open(preview_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"Alert preview saved: {preview_path}")
            
            # Send alert
            self.send_alert(html_content, alert_type)
            
            # Update alert state to prevent duplicates
            self.update_alert_state(price_analysis)
            
        else:
            print("No significant price breaks detected")
    
    def run_monitoring_loop(self):
        """Run continuous monitoring during market hours"""
        print("Starting intraday monitoring system")
        print("Press Ctrl+C to stop monitoring")
        
        while True:
            try:
                self.run_price_check()
                
                # Wait for next check
                print(f"Next check in {self.price_check_interval // 60} minutes")
                time.sleep(self.price_check_interval)
                
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user")
                break
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying


def main():
    """Main function"""
    import sys
    
    alert_system = IntradayAlertSystem()
    
    if '--test' in sys.argv:
        # Run single test check
        alert_system.run_price_check()
    else:
        # Run continuous monitoring
        alert_system.run_monitoring_loop()


if __name__ == "__main__":
    main()