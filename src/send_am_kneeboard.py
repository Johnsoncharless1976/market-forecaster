"""
AM Kneeboard Sender with Macro Gate Timing
Default: 9:00 AM ET
High-impact 8:30 AM macro: 9:15 AM ET
Smoke preview: 20 minutes before chosen send time (8:40 or 8:55)
"""

import os
import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import pytz

load_dotenv()

class AMKneeboardSender:
    """AM Kneeboard with dynamic timing based on macro calendar"""
    
    def __init__(self):
        """Initialize AM kneeboard sender"""
        self.conn_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
        
        self.smtp_config = {
            'server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'port': int(os.getenv('SMTP_PORT', '587')),
            'user': os.getenv('SMTP_USER'),
            'password': os.getenv('SMTP_PASS'),
            'from_email': os.getenv('FROM_EMAIL', 'noreply@zenmarket.ai'),
            'from_name': os.getenv('FROM_NAME', 'ZenMarket Morning Kneeboard')
        }
        
        self.et_tz = pytz.timezone('US/Eastern')
    
    def get_db_connection(self):
        """Get Snowflake database connection"""
        return snowflake.connector.connect(**self.conn_params)
    
    def check_macro_gate(self, target_date: str = None) -> Tuple[bool, str, str]:
        """
        Check if target date has high-impact 8:30 AM macro event
        
        Returns:
            (has_macro, send_time, preview_time)
        """
        if target_date is None:
            target_date = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                # Use the HAS_HIGH_IMPACT_MACRO function
                cur.execute("SELECT HAS_HIGH_IMPACT_MACRO(%s) as has_macro", (target_date,))
                result = cur.fetchone()
                has_macro = result[0] if result else False
                
                # Get macro event details if present
                macro_events = []
                if has_macro:
                    cur.execute("""
                        SELECT EVENT_NAME, EVENT_TIME, IMPACT_LEVEL, DESCRIPTION
                        FROM MACRO_CALENDAR 
                        WHERE DATE = %s 
                          AND EVENT_TIME = '08:30:00'
                          AND IMPACT_LEVEL = 'HIGH'
                        ORDER BY EVENT_NAME
                    """, (target_date,))
                    macro_events = cur.fetchall()
                
                # Determine send times
                if has_macro:
                    send_time = "09:15"
                    preview_time = "08:55"
                    event_names = [event[0] for event in macro_events]
                    reason = f"High-impact 8:30 AM macro: {', '.join(event_names)}"
                else:
                    send_time = "09:00"
                    preview_time = "08:40"
                    reason = "Normal day - no high-impact macro"
                
                return has_macro, send_time, preview_time, reason
                
        except Exception as e:
            print(f"⚠️ Error checking macro gate: {e}")
            # Default to normal timing on error
            return False, "09:00", "08:40", "Error checking macro calendar - defaulting to normal timing"
    
    def is_market_day(self) -> bool:
        """Check if today is a market day (not weekend or NYSE holiday)"""
        today = datetime.now(self.et_tz)
        
        # Check weekend
        if today.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check NYSE holidays
        today_str = today.strftime("%Y-%m-%d")
        
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT COUNT(*) FROM HOLIDAYS_NYSE 
                    WHERE DATE = %s
                """, (today_str,))
                result = cur.fetchone()
                is_holiday = (result[0] > 0) if result else False
                
                return not is_holiday
                
        except Exception as e:
            print(f"⚠️ Error checking market day: {e}")
            # Fall back to basic weekend check
            return today.weekday() < 5
    
    def get_beta_config(self) -> Tuple[bool, List[str]]:
        """Get beta rollout configuration"""
        beta_enabled = os.getenv("EMAIL_BETA_ENABLED", "false").lower() == "true"
        
        allowlist = []
        if beta_enabled and os.getenv("EMAIL_TO"):
            allowlist = [email.strip() for email in os.getenv("EMAIL_TO").split(",")]
        
        return beta_enabled, allowlist
    
    def get_am_recipients(self) -> List[str]:
        """Get recipients eligible for AM kneeboard based on AM consent"""
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                # Get recipients with AM consent
                cur.execute("""
                    SELECT EMAIL 
                    FROM EMAIL_RECIPIENTS 
                    WHERE AM_CONSENT = TRUE
                      AND AM_UNSUBSCRIBED_AT IS NULL
                      AND UNSUBSCRIBED_AT IS NULL
                      AND CONSENT_TS IS NOT NULL
                    ORDER BY EMAIL
                """)
                
                recipients = [row[0] for row in cur.fetchall()]
                
                # For beta, also check EMAIL_TO environment variable
                env_recipients = []
                if os.getenv("EMAIL_TO"):
                    env_recipients = [email.strip() for email in os.getenv("EMAIL_TO").split(",")]
                
                # Intersect with database recipients for beta testing
                if env_recipients:
                    recipients = [r for r in recipients if r in env_recipients]
                
                return recipients
                
        except Exception as e:
            print(f"⚠️ Error getting AM recipients: {e}")
            # Fallback to environment variable
            if os.getenv("EMAIL_TO"):
                return [email.strip() for email in os.getenv("EMAIL_TO").split(",")]
            return []
    
    def fetch_am_forecast_data(self) -> Dict:
        """Fetch latest forecast data for AM kneeboard"""
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                # Get latest forecast from FORECAST_JOBS or FORECAST_DAILY
                cur.execute("""
                    SELECT DATE, INDEX, FORECAST_BIAS, ATM_STRADDLE, 
                           SUPPORT_LEVELS, RESISTANCE_LEVELS, RSI_CONTEXT, NOTES
                    FROM FORECAST_JOBS
                    ORDER BY DATE DESC LIMIT 1
                """)
                
                forecast_row = cur.fetchone()
                if not forecast_row:
                    return None
                
                f_date, index_name, bias, straddle, support, resistance, rsi, notes = forecast_row
                
                # Get market regime context if available
                regime_context = "Standard market conditions"
                try:
                    cur.execute("""
                        SELECT REGIME_TYPE, CONFIDENCE_SCORE 
                        FROM MARKET_REGIME 
                        WHERE DATE = %s
                    """, (f_date,))
                    regime_row = cur.fetchone()
                    if regime_row:
                        regime_type, confidence = regime_row
                        regime_context = f"{regime_type} regime (confidence: {confidence}%)"
                except:
                    pass  # Table may not exist yet
                
                return {
                    'date': f_date,
                    'index': index_name,
                    'bias': bias,
                    'straddle': straddle,
                    'support': support,
                    'resistance': resistance,
                    'rsi': rsi,
                    'notes': notes,
                    'regime': regime_context
                }
                
        except Exception as e:
            print(f"⚠️ Error fetching AM forecast data: {e}")
            return None
    
    def generate_am_kneeboard_html(self, forecast_data: Dict, macro_info: Dict) -> str:
        """Generate HTML content for AM kneeboard"""
        
        send_time = macro_info['send_time']
        has_macro = macro_info['has_macro']
        reason = macro_info['reason']
        
        # Macro gate badge
        if has_macro:
            macro_badge = f"""
            <div style="background: #f59e0b; color: white; padding: 8px 16px; border-radius: 4px; margin: 10px 0; text-align: center;">
                <strong>⏰ MACRO GATE ACTIVE</strong><br>
                <small>AM kneeboard delayed to {send_time} ET due to high-impact macro</small>
            </div>
            """
        else:
            macro_badge = f"""
            <div style="background: #10b981; color: white; padding: 6px 12px; border-radius: 4px; margin: 10px 0; text-align: center; font-size: 12px;">
                Standard {send_time} ET send time
            </div>
            """
        
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>AM Kneeboard - {forecast_data['date']}</title>
        </head>
        <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f8fafc;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                
                <!-- Header -->
                <div style="background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%); color: white; padding: 24px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px; font-weight: 600;">🌅 Morning Kneeboard</h1>
                    <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">{forecast_data['date']} • Sent at {send_time} ET</p>
                </div>
                
                {macro_badge}
                
                <!-- Market Outlook -->
                <div style="padding: 24px;">
                    <h2 style="margin: 0 0 16px 0; color: #1f2937; font-size: 20px; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px;">
                        📈 Market Outlook
                    </h2>
                    
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px;">
                        <div style="background: #f8fafc; padding: 16px; border-radius: 6px; border-left: 4px solid #3b82f6;">
                            <h3 style="margin: 0 0 8px 0; color: #374151; font-size: 14px; text-transform: uppercase; font-weight: 600;">Index</h3>
                            <p style="margin: 0; font-size: 16px; font-weight: 500; color: #1f2937;">{forecast_data['index']}</p>
                        </div>
                        <div style="background: #f8fafc; padding: 16px; border-radius: 6px; border-left: 4px solid #10b981;">
                            <h3 style="margin: 0 0 8px 0; color: #374151; font-size: 14px; text-transform: uppercase; font-weight: 600;">Bias</h3>
                            <p style="margin: 0; font-size: 16px; font-weight: 500; color: #1f2937;">{forecast_data['bias']}</p>
                        </div>
                    </div>
                    
                    <div style="background: #fefce8; border: 1px solid #fde047; border-radius: 6px; padding: 16px; margin-bottom: 20px;">
                        <h3 style="margin: 0 0 8px 0; color: #a16207; font-size: 14px; font-weight: 600;">🎯 KEY LEVELS</h3>
                        <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; font-size: 14px;">
                            <div>
                                <strong style="color: #dc2626;">Support:</strong><br>{forecast_data['support']}
                            </div>
                            <div>
                                <strong style="color: #7c3aed;">ATM Straddle:</strong><br>{forecast_data['straddle']}
                            </div>
                            <div>
                                <strong style="color: #059669;">Resistance:</strong><br>{forecast_data['resistance']}
                            </div>
                        </div>
                    </div>
                    
                    <div style="background: #f0f9ff; border-left: 4px solid #0ea5e9; padding: 16px; margin-bottom: 20px;">
                        <h3 style="margin: 0 0 8px 0; color: #0c4a6e; font-size: 14px; font-weight: 600;">📊 TECHNICAL CONTEXT</h3>
                        <p style="margin: 0; color: #1e293b; font-size: 14px;"><strong>RSI:</strong> {forecast_data['rsi']}</p>
                        <p style="margin: 8px 0 0 0; color: #1e293b; font-size: 14px;"><strong>Regime:</strong> {forecast_data['regime']}</p>
                    </div>
                    
                    <!-- Notes Section -->
                    <div style="background: #f9fafb; border-radius: 6px; padding: 16px;">
                        <h3 style="margin: 0 0 12px 0; color: #374151; font-size: 16px; font-weight: 600;">📝 Trading Notes</h3>
                        <p style="margin: 0; color: #4b5563; line-height: 1.6; font-size: 14px;">{forecast_data['notes']}</p>
                    </div>
                    
                    <!-- Macro Information -->
                    <div style="background: {'#fef2f2' if has_macro else '#f0fdf4'}; border-radius: 6px; padding: 16px; margin-top: 20px; border-left: 4px solid {'#ef4444' if has_macro else '#22c55e'};">
                        <h3 style="margin: 0 0 8px 0; color: {'#991b1b' if has_macro else '#166534'}; font-size: 14px; font-weight: 600;">
                            {'⚠️ MACRO CALENDAR' if has_macro else '✅ MACRO CALENDAR'}
                        </h3>
                        <p style="margin: 0; color: {'#7f1d1d' if has_macro else '#15803d'}; font-size: 13px;">{reason}</p>
                    </div>
                </div>
                
                <!-- Footer -->
                <div style="background: #f9fafb; padding: 20px; text-align: center; border-top: 1px solid #e5e7eb;">
                    <p style="margin: 0; color: #6b7280; font-size: 12px;">
                        ZenMarket AM Kneeboard • Generated at {datetime.now(self.et_tz).strftime('%H:%M ET')}
                    </p>
                    <p style="margin: 8px 0 0 0; color: #9ca3af; font-size: 11px;">
                        For educational purposes only • Not investment advice
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html_template
    
    def log_am_send(self, run_id: str, recipient: str, subject: str, status: str, 
                   reason: str, send_time: str, has_macro: bool):
        """Log AM kneeboard send attempt"""
        
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO EMAIL_SEND_LOG (
                        RUN_ID, RECIPIENT, SUBJECT, BADGES, STATUS, REASON,
                        BETA_ENABLED, IS_MARKET_DAY, FORECAST_DATE, COHORT, 
                        CONSENT_AGE_DAYS, SENT_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """, (
                    run_id, recipient, subject, 
                    f"AM_KNEEBOARD,MACRO_GATE={has_macro},SEND_TIME={send_time}",
                    status, reason, True, self.is_market_day(),
                    datetime.now(self.et_tz).strftime('%Y-%m-%d'), 'am_stream', 0
                ))
                conn.commit()
                
        except Exception as e:
            print(f"⚠️ Failed to log AM send for {recipient}: {e}")
    
    def send_am_kneeboard(self, preview_mode: bool = False) -> Dict:
        """Send AM kneeboard with macro gate timing"""
        
        timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
        target_date = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        
        # Check macro gate
        has_macro, send_time, preview_time, reason = self.check_macro_gate(target_date)
        
        macro_info = {
            'has_macro': has_macro,
            'send_time': send_time,
            'preview_time': preview_time,
            'reason': reason
        }
        
        print(f"🔍 Macro Gate Check: {reason}")
        print(f"📅 Target Date: {target_date}")
        print(f"⏰ Send Time: {send_time} ET")
        print(f"👀 Preview Time: {preview_time} ET")
        
        # Check if it's a market day
        if not self.is_market_day():
            print("📅 Weekend/holiday detected - generating preview only")
        
        # Get beta configuration
        beta_enabled, allowlist = self.get_beta_config()
        
        # Get recipients
        recipients = self.get_am_recipients()
        if not recipients:
            print("❌ No AM kneeboard recipients configured")
            return {'status': 'error', 'message': 'No recipients'}
        
        # Get forecast data
        forecast_data = self.fetch_am_forecast_data()
        if not forecast_data:
            print("❌ No forecast data available")
            return {'status': 'error', 'message': 'No forecast data'}
        
        # Generate email content
        html_content = self.generate_am_kneeboard_html(forecast_data, macro_info)
        
        # Save preview
        mode_str = "smoke" if preview_mode else "send"
        preview_dir = "audit_exports/email_previews"
        os.makedirs(preview_dir, exist_ok=True)
        
        preview_path = f"{preview_dir}/{timestamp}_am_{mode_str}.html"
        with open(preview_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        run_id = f"AM_{timestamp}_{target_date.replace('-', '')}"
        subject = f"🌅 Morning Kneeboard – {forecast_data['date']} (sent {send_time} ET)"
        
        # Send emails if not preview mode and conditions are met
        emails_sent = 0
        should_send = beta_enabled and self.is_market_day() and not preview_mode
        
        if should_send:
            for recipient in recipients:
                if recipient not in allowlist:
                    print(f"⏭️ Skipping {recipient} - not in beta allowlist")
                    self.log_am_send(run_id, recipient, subject, "SKIP", 
                                   "not in beta allowlist", send_time, has_macro)
                    continue
                
                try:
                    # Create email message
                    msg = MIMEMultipart('alternative')
                    msg['Subject'] = subject
                    msg['From'] = f"{self.smtp_config['from_name']} <{self.smtp_config['from_email']}>"
                    msg['To'] = recipient
                    
                    # Attach HTML part
                    html_part = MIMEText(html_content, 'html')
                    msg.attach(html_part)
                    
                    # Send email
                    with smtplib.SMTP(self.smtp_config['server'], self.smtp_config['port']) as server:
                        server.starttls()
                        server.login(self.smtp_config['user'], self.smtp_config['password'])
                        server.send_message(msg)
                    
                    print(f"✅ AM kneeboard sent to {recipient}")
                    self.log_am_send(run_id, recipient, subject, "SENT", 
                                   "delivered successfully", send_time, has_macro)
                    emails_sent += 1
                    
                except Exception as e:
                    print(f"❌ Failed to send to {recipient}: {e}")
                    self.log_am_send(run_id, recipient, subject, "ERROR", 
                                   str(e)[:100], send_time, has_macro)
        
        else:
            skip_reason = "preview mode" if preview_mode else ("not market day" if not self.is_market_day() else "beta disabled")
            for recipient in recipients:
                self.log_am_send(run_id, recipient, subject, "SKIP", 
                               skip_reason, send_time, has_macro)
        
        return {
            'status': 'success',
            'emails_sent': emails_sent,
            'preview_path': preview_path,
            'macro_gate': has_macro,
            'send_time': send_time,
            'preview_time': preview_time,
            'run_id': run_id
        }


def main():
    """Main function for AM kneeboard sending"""
    
    sender = AMKneeboardSender()
    
    # Check command line args for preview mode
    import sys
    preview_mode = '--preview' in sys.argv or '--smoke' in sys.argv
    
    if preview_mode:
        print("👀 Running in preview/smoke mode")
    
    # Send AM kneeboard
    result = sender.send_am_kneeboard(preview_mode=preview_mode)
    
    if result['status'] == 'success':
        print(f"\n📊 AM Kneeboard Summary:")
        print(f"   Emails sent: {result['emails_sent']}")
        print(f"   Macro gate: {'ACTIVE' if result['macro_gate'] else 'INACTIVE'}")
        print(f"   Send time: {result['send_time']} ET")
        print(f"   Preview: {result['preview_path']}")
        print(f"   Run ID: {result['run_id']}")
    else:
        print(f"❌ AM kneeboard failed: {result['message']}")


if __name__ == "__main__":
    main()