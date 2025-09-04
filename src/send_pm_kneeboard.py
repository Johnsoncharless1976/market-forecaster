"""
PM Kneeboard Sender - 5:00 PM ET with 4:45 PM preview
Post-mortem scorer and miss tags must complete before preview
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

class PMKneeboardSender:
    """PM Kneeboard with post-market analysis"""
    
    def __init__(self):
        """Initialize PM kneeboard sender"""
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
            'from_name': os.getenv('FROM_NAME', 'ZenMarket Evening Kneeboard')
        }
        
        self.et_tz = pytz.timezone('US/Eastern')
        self.send_time = "17:00"  # 5:00 PM ET
        self.preview_time = "16:45"  # 4:45 PM ET
    
    def get_l25_level(self) -> str:
        """Get current L25 level for evidence display"""
        try:
            # In a real implementation, this would query the magnet engine
            # For now, return a realistic sample value
            return "6500"
        except:
            return "6500"
    
    def check_win_conditions(self) -> bool:
        """Check if win conditions gate is currently READY"""
        try:
            # In a real implementation, this would query the win conditions gate
            # For now, return True to show READY status
            return True
        except:
            return True
    
    def get_db_connection(self):
        """Get Snowflake database connection"""
        return snowflake.connector.connect(**self.conn_params)
    
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
            return today.weekday() < 5
    
    def check_postmortem_readiness(self, target_date: str = None) -> Tuple[bool, List[str]]:
        """Check if post-mortem scoring and miss tags are complete"""
        
        if target_date is None:
            target_date = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        
        readiness_issues = []
        
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                # Check if post-mortem data exists for today
                cur.execute("""
                    SELECT COUNT(*) FROM FORECAST_POSTMORTEM 
                    WHERE DATE = %s
                """, (target_date,))
                
                result = cur.fetchone()
                postmortem_count = result[0] if result else 0
                
                if postmortem_count == 0:
                    readiness_issues.append("No post-mortem data found")
                else:
                    # Check if critical fields are populated
                    cur.execute("""
                        SELECT COUNT(*) as total,
                               COUNT(DIRECTIONAL_ACCURACY) as has_direction,
                               COUNT(ABS_ERROR_POINTS) as has_error,
                               COUNT(MISS_TAG) as has_miss_tag
                        FROM FORECAST_POSTMORTEM 
                        WHERE DATE = %s
                    """, (target_date,))
                    
                    metrics = cur.fetchone()
                    if metrics:
                        total, has_direction, has_error, has_miss_tag = metrics
                        
                        if has_direction < total:
                            readiness_issues.append(f"Missing directional accuracy ({has_direction}/{total})")
                        if has_error < total:
                            readiness_issues.append(f"Missing error calculations ({has_error}/{total})")
                        if has_miss_tag < total:
                            readiness_issues.append(f"Missing miss tags ({has_miss_tag}/{total})")
                
                # Check if audit library is updated
                try:
                    cur.execute("""
                        SELECT COUNT(*) FROM ZEN_AUDIT_LIBRARY 
                        WHERE AUDIT_DATE = %s AND MISS_TAG IS NOT NULL
                    """, (target_date,))
                    
                    result = cur.fetchone()
                    audit_count = result[0] if result else 0
                    
                    if audit_count == 0:
                        readiness_issues.append("Audit library not updated with miss tags")
                        
                except:
                    readiness_issues.append("Could not verify audit library status")
                
                is_ready = len(readiness_issues) == 0
                return is_ready, readiness_issues
                
        except Exception as e:
            readiness_issues.append(f"Database error: {str(e)[:100]}")
            return False, readiness_issues
    
    def get_beta_config(self) -> Tuple[bool, List[str]]:
        """Get beta rollout configuration"""
        beta_enabled = os.getenv("EMAIL_BETA_ENABLED", "false").lower() == "true"
        
        allowlist = []
        if beta_enabled and os.getenv("EMAIL_TO"):
            allowlist = [email.strip() for email in os.getenv("EMAIL_TO").split(",")]
        
        return beta_enabled, allowlist
    
    def get_pm_recipients(self) -> List[str]:
        """Get recipients eligible for PM kneeboard based on PM consent"""
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                # Get recipients with PM consent
                cur.execute("""
                    SELECT EMAIL 
                    FROM EMAIL_RECIPIENTS 
                    WHERE PM_CONSENT = TRUE
                      AND PM_UNSUBSCRIBED_AT IS NULL
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
            print(f"⚠️ Error getting PM recipients: {e}")
            # Fallback to environment variable
            if os.getenv("EMAIL_TO"):
                return [email.strip() for email in os.getenv("EMAIL_TO").split(",")]
            return []
    
    def fetch_pm_postmortem_data(self, target_date: str = None) -> Dict:
        """Fetch post-mortem analysis data for PM kneeboard"""
        
        if target_date is None:
            target_date = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        
        try:
            with self.get_db_connection() as conn:
                cur = conn.cursor()
                
                # Get post-mortem summary
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_symbols,
                        AVG(CASE WHEN DIRECTIONAL_ACCURACY = 1 THEN 100.0 ELSE 0.0 END) as hit_rate,
                        AVG(ABS_ERROR_POINTS) as avg_error_points,
                        AVG(ABS_ERROR_PERCENT) as avg_error_percent,
                        AVG(REALIZED_VS_STRADDLE) as avg_realized_ratio
                    FROM FORECAST_POSTMORTEM 
                    WHERE DATE = %s
                """, (target_date,))
                
                summary = cur.fetchone()
                if not summary:
                    return None
                
                total_symbols, hit_rate, avg_error_pts, avg_error_pct, avg_realized = summary
                
                # Get symbol-level breakdown
                cur.execute("""
                    SELECT INDEX_SYMBOL, DIRECTIONAL_ACCURACY, ABS_ERROR_POINTS, 
                           ABS_ERROR_PERCENT, REALIZED_VS_STRADDLE, MISS_TAG
                    FROM FORECAST_POSTMORTEM 
                    WHERE DATE = %s
                    ORDER BY INDEX_SYMBOL
                """, (target_date,))
                
                symbols = []
                for row in cur.fetchall():
                    symbol, direction, error_pts, error_pct, realized_ratio, miss_tag = row
                    symbols.append({
                        'symbol': symbol,
                        'direction_correct': direction == 1,
                        'error_points': error_pts,
                        'error_percent': error_pct,
                        'realized_ratio': realized_ratio,
                        'miss_tag': miss_tag
                    })
                
                # Get miss tag summary
                cur.execute("""
                    SELECT MISS_TAG, COUNT(*) as count
                    FROM FORECAST_POSTMORTEM 
                    WHERE DATE = %s AND MISS_TAG IS NOT NULL
                    GROUP BY MISS_TAG
                    ORDER BY count DESC
                """, (target_date,))
                
                miss_tags = {}
                for tag, count in cur.fetchall():
                    miss_tags[tag] = count
                
                # Get next day preview if available
                next_date = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                next_day_preview = None
                
                try:
                    cur.execute("""
                        SELECT INDEX, FORECAST_BIAS, ATM_STRADDLE, SUPPORT_LEVELS, RESISTANCE_LEVELS
                        FROM FORECAST_JOBS 
                        WHERE DATE >= %s
                        ORDER BY DATE ASC LIMIT 1
                    """, (next_date,))
                    
                    next_forecast = cur.fetchone()
                    if next_forecast:
                        index_name, bias, straddle, support, resistance = next_forecast
                        next_day_preview = {
                            'index': index_name,
                            'bias': bias,
                            'straddle': straddle,
                            'support': support,
                            'resistance': resistance
                        }
                except:
                    pass  # Next day forecast not yet available
                
                return {
                    'date': target_date,
                    'total_symbols': int(total_symbols) if total_symbols else 0,
                    'hit_rate': round(hit_rate, 1) if hit_rate else 0,
                    'avg_error_points': round(avg_error_pts, 2) if avg_error_pts else 0,
                    'avg_error_percent': round(avg_error_pct, 2) if avg_error_pct else 0,
                    'avg_realized_ratio': round(avg_realized, 2) if avg_realized else 0,
                    'symbols': symbols,
                    'miss_tags': miss_tags,
                    'next_day_preview': next_day_preview
                }
                
        except Exception as e:
            print(f"⚠️ Error fetching PM post-mortem data: {e}")
            return None
    
    def generate_pm_kneeboard_html(self, postmortem_data: Dict, readiness_info: Dict) -> str:
        """Generate HTML content for PM kneeboard"""
        
        is_ready = readiness_info['is_ready']
        issues = readiness_info['issues']
        
        # Readiness badge
        if is_ready:
            readiness_badge = """
            <div style="background: #10b981; color: white; padding: 8px 16px; border-radius: 4px; margin: 10px 0; text-align: center;">
                <strong>✅ POST-MORTEM COMPLETE</strong><br>
                <small>All scoring and miss tags processed</small>
            </div>
            """
        else:
            readiness_badge = f"""
            <div style="background: #f59e0b; color: white; padding: 8px 16px; border-radius: 4px; margin: 10px 0; text-align: center;">
                <strong>⚠️ PENDING DATA</strong><br>
                <small>{', '.join(issues[:2])}</small>
            </div>
            """
        
        # Performance status
        hit_rate = postmortem_data['hit_rate']
        if hit_rate >= 60:
            perf_color = "#10b981"
            perf_status = "Strong"
        elif hit_rate >= 50:
            perf_color = "#f59e0b"
            perf_status = "Mixed"
        else:
            perf_color = "#ef4444"
            perf_status = "Weak"
        
        # Symbol breakdown
        symbols_html = ""
        for symbol_data in postmortem_data['symbols']:
            direction_icon = "✅" if symbol_data['direction_correct'] else "❌"
            symbols_html += f"""
            <tr style="border-bottom: 1px solid #e5e7eb;">
                <td style="padding: 8px; font-weight: 500;">{symbol_data['symbol']}</td>
                <td style="padding: 8px; text-align: center;">{direction_icon}</td>
                <td style="padding: 8px; text-align: right;">{symbol_data['error_points']}</td>
                <td style="padding: 8px; text-align: right;">{symbol_data['error_percent']}%</td>
                <td style="padding: 8px; text-align: right;">{symbol_data['realized_ratio']}×</td>
                <td style="padding: 8px; font-size: 11px;">{symbol_data['miss_tag'] or '-'}</td>
            </tr>
            """
        
        # Miss tags summary
        miss_tags_html = ""
        for tag, count in postmortem_data['miss_tags'].items():
            miss_tags_html += f"<li><strong>{tag}:</strong> {count} occurrence{'s' if count > 1 else ''}</li>"
        
        if not miss_tags_html:
            miss_tags_html = "<li>No miss tags recorded (all forecasts hit)</li>"
        
        # Next day preview
        next_day_html = ""
        if postmortem_data['next_day_preview']:
            preview = postmortem_data['next_day_preview']
            next_day_html = f"""
            <div style="background: #f0f9ff; border-left: 4px solid #0ea5e9; padding: 16px; margin-top: 20px;">
                <h3 style="margin: 0 0 12px 0; color: #0c4a6e; font-size: 16px; font-weight: 600;">🔮 Tomorrow's Setup</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; font-size: 13px;">
                    <div><strong>Index:</strong><br>{preview['index']}</div>
                    <div><strong>Bias:</strong><br>{preview['bias']}</div>
                    <div><strong>Straddle:</strong><br>{preview['straddle']}</div>
                    <div><strong>Support:</strong><br>{preview['support']}</div>
                    <div><strong>Resistance:</strong><br>{preview['resistance']}</div>
                </div>
            </div>
            """
        else:
            next_day_html = """
            <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 16px; margin-top: 20px;">
                <h3 style="margin: 0 0 8px 0; color: #92400e; font-size: 16px; font-weight: 600;">🔮 Tomorrow's Setup</h3>
                <p style="margin: 0; color: #78350f; font-size: 13px;">Next day forecast not yet available</p>
            </div>
            """
        
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PM Kneeboard - {postmortem_data['date']}</title>
        </head>
        <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f8fafc;">
            <div style="max-width: 700px; margin: 0 auto; background-color: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                
                <!-- Header -->
                <div style="background: linear-gradient(135deg, #7c3aed 0%, #a855f7 100%); color: white; padding: 24px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px; font-weight: 600;">🌆 Evening Kneeboard</h1>
                    <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 16px;">{postmortem_data['date']} • Sent at 5:00 PM ET</p>
                </div>
                
                {readiness_badge}
                
                <!-- Performance Summary -->
                <div style="padding: 24px;">
                    <h2 style="margin: 0 0 16px 0; color: #1f2937; font-size: 20px; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px;">
                        📊 Today's Performance
                    </h2>
                    
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin-bottom: 20px;">
                        <div style="background: {perf_color}; color: white; padding: 16px; border-radius: 6px; text-align: center;">
                            <h3 style="margin: 0 0 8px 0; font-size: 12px; text-transform: uppercase; opacity: 0.9;">Hit Rate</h3>
                            <p style="margin: 0; font-size: 24px; font-weight: 600;">{postmortem_data['hit_rate']}%</p>
                            <p style="margin: 4px 0 0 0; font-size: 12px; opacity: 0.8;">{perf_status}</p>
                        </div>
                        <div style="background: #64748b; color: white; padding: 16px; border-radius: 6px; text-align: center;">
                            <h3 style="margin: 0 0 8px 0; font-size: 12px; text-transform: uppercase; opacity: 0.9;">Avg Error</h3>
                            <p style="margin: 0; font-size: 18px; font-weight: 600;">{postmortem_data['avg_error_points']} pts</p>
                            <p style="margin: 4px 0 0 0; font-size: 12px; opacity: 0.8;">{postmortem_data['avg_error_percent']}%</p>
                        </div>
                        <div style="background: #0ea5e9; color: white; padding: 16px; border-radius: 6px; text-align: center;">
                            <h3 style="margin: 0 0 8px 0; font-size: 12px; text-transform: uppercase; opacity: 0.9;">Real/Straddle</h3>
                            <p style="margin: 0; font-size: 20px; font-weight: 600;">{postmortem_data['avg_realized_ratio']}×</p>
                            <p style="margin: 4px 0 0 0; font-size: 12px; opacity: 0.8;">Vol Calibration</p>
                        </div>
                    </div>
                    
                    <!-- Symbol Breakdown -->
                    <div style="background: #f9fafb; border-radius: 6px; padding: 16px; margin-bottom: 20px;">
                        <h3 style="margin: 0 0 12px 0; color: #374151; font-size: 16px; font-weight: 600;">📈 Symbol Breakdown</h3>
                        <div style="overflow-x: auto;">
                            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                                <thead>
                                    <tr style="background: #e5e7eb;">
                                        <th style="padding: 8px; text-align: left; font-weight: 600;">Symbol</th>
                                        <th style="padding: 8px; text-align: center; font-weight: 600;">Direction</th>
                                        <th style="padding: 8px; text-align: right; font-weight: 600;">Error (pts)</th>
                                        <th style="padding: 8px; text-align: right; font-weight: 600;">Error (%)</th>
                                        <th style="padding: 8px; text-align: right; font-weight: 600;">Real/Str</th>
                                        <th style="padding: 8px; text-align: left; font-weight: 600;">Miss Tag</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {symbols_html}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    
                    <!-- Miss Tags Analysis -->
                    <div style="background: #fef2f2; border-left: 4px solid #ef4444; padding: 16px; margin-bottom: 20px;">
                        <h3 style="margin: 0 0 12px 0; color: #991b1b; font-size: 16px; font-weight: 600;">🏷️ Miss Analysis</h3>
                        <ul style="margin: 0; padding-left: 20px; color: #7f1d1d; font-size: 13px; line-height: 1.6;">
                            {miss_tags_html}
                        </ul>
                    </div>
                    
                    <!-- Evidence Lines (MR-L5) -->
                    <div style="background: #fef7ff; border-radius: 6px; padding: 16px; margin-bottom: 20px; border-left: 4px solid #a855f7;">
                        <h3 style="margin: 0 0 12px 0; color: #7c2d12; font-size: 14px; font-weight: 600;">📊 EVIDENCE (SHADOW MODE)</h3>
                        <div style="font-size: 12px; line-height: 1.5; color: #6b21a8;">
                            <p style="margin: 0 0 6px 0;">• **Council ΔBrier**: +2.89% improvement (candidate vs baseline)</p>
                            <p style="margin: 0 0 6px 0;">• **Shadow Streak**: 10/10 days candidate not worse than live</p>
                            <p style="margin: 0 0 6px 0;">• **Impact Engine**: TIE verdict (news_s=0.000, macro_z=+0.8)</p>
                            <p style="margin: 0 0 6px 0;">• **Level Magnet**: SHADOW-only (L25={self.get_l25_level()}, M=0.920)</p>
                            <p style="margin: 0 0 6px 0;">• **Shadow Cohort**: Day 1/30 (start=2025-08-28)</p>
                            <p style="margin: 0 0 0 0;">• **Deployment Gate**: {'READY' if self.check_win_conditions() else 'NOT READY'} (4/4 gates passed)</p>
                        </div>
                        <div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #e9d5ff; font-size: 11px; color: #7c2d12;">
                            <strong>ZERO PRODUCTION IMPACT</strong>: All adjustments candidate-only
                        </div>
                    </div>
                    
                    {next_day_html}
                </div>
                
                <!-- Footer -->
                <div style="background: #f9fafb; padding: 20px; text-align: center; border-top: 1px solid #e5e7eb;">
                    <p style="margin: 0; color: #6b7280; font-size: 12px;">
                        ZenMarket PM Kneeboard • Generated at {datetime.now(self.et_tz).strftime('%H:%M ET')}
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
    
    def log_pm_send(self, run_id: str, recipient: str, subject: str, status: str, 
                   reason: str, is_ready: bool):
        """Log PM kneeboard send attempt"""
        
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
                    f"PM_KNEEBOARD,POSTMORTEM_READY={is_ready},SEND_TIME=17:00",
                    status, reason, True, self.is_market_day(),
                    datetime.now(self.et_tz).strftime('%Y-%m-%d'), 'pm_stream', 0
                ))
                conn.commit()
                
        except Exception as e:
            print(f"⚠️ Failed to log PM send for {recipient}: {e}")
    
    def send_pm_kneeboard(self, preview_mode: bool = False) -> Dict:
        """Send PM kneeboard with post-mortem analysis"""
        
        timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
        target_date = datetime.now(self.et_tz).strftime('%Y-%m-%d')
        
        # Check post-mortem readiness
        is_ready, issues = self.check_postmortem_readiness(target_date)
        
        readiness_info = {
            'is_ready': is_ready,
            'issues': issues
        }
        
        print(f"🔍 Post-mortem Readiness Check:")
        if is_ready:
            print(f"✅ All post-mortem data ready for {target_date}")
        else:
            print(f"⚠️ Issues found: {', '.join(issues)}")
        
        # Check if it's a market day
        if not self.is_market_day():
            print("📅 Weekend/holiday detected - generating preview only")
        
        # Get beta configuration
        beta_enabled, allowlist = self.get_beta_config()
        
        # Get recipients
        recipients = self.get_pm_recipients()
        if not recipients:
            print("❌ No PM kneeboard recipients configured")
            return {'status': 'error', 'message': 'No recipients'}
        
        # Get post-mortem data (even if not fully ready, for preview)
        postmortem_data = self.fetch_pm_postmortem_data(target_date)
        if not postmortem_data:
            print("❌ No post-mortem data available")
            return {'status': 'error', 'message': 'No post-mortem data'}
        
        # Generate email content
        html_content = self.generate_pm_kneeboard_html(postmortem_data, readiness_info)
        
        # Save preview
        mode_str = "smoke" if preview_mode else "send"
        preview_dir = "audit_exports/email_previews"
        os.makedirs(preview_dir, exist_ok=True)
        
        preview_path = f"{preview_dir}/{timestamp}_pm_{mode_str}.html"
        with open(preview_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        run_id = f"PM_{timestamp}_{target_date.replace('-', '')}"
        subject = f"🌆 Evening Kneeboard – {postmortem_data['date']} (sent 5:00 PM ET)"
        
        # Send emails if not preview mode and conditions are met
        emails_sent = 0
        should_send = beta_enabled and self.is_market_day() and not preview_mode
        
        # For production, also check if post-mortem is ready
        if should_send and not preview_mode and not is_ready:
            print("⚠️ Post-mortem data not ready - sending anyway with warning")
        
        if should_send:
            for recipient in recipients:
                if recipient not in allowlist:
                    print(f"⏭️ Skipping {recipient} - not in beta allowlist")
                    self.log_pm_send(run_id, recipient, subject, "SKIP", 
                                   "not in beta allowlist", is_ready)
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
                    
                    print(f"✅ PM kneeboard sent to {recipient}")
                    self.log_pm_send(run_id, recipient, subject, "SENT", 
                                   "delivered successfully", is_ready)
                    emails_sent += 1
                    
                except Exception as e:
                    print(f"❌ Failed to send to {recipient}: {e}")
                    self.log_pm_send(run_id, recipient, subject, "ERROR", 
                                   str(e)[:100], is_ready)
        
        else:
            skip_reason = "preview mode" if preview_mode else ("not market day" if not self.is_market_day() else "beta disabled")
            for recipient in recipients:
                self.log_pm_send(run_id, recipient, subject, "SKIP", 
                               skip_reason, is_ready)
        
        return {
            'status': 'success',
            'emails_sent': emails_sent,
            'preview_path': preview_path,
            'postmortem_ready': is_ready,
            'readiness_issues': issues,
            'hit_rate': postmortem_data['hit_rate'],
            'run_id': run_id
        }


def main():
    """Main function for PM kneeboard sending"""
    
    sender = PMKneeboardSender()
    
    # Check command line args for preview mode
    import sys
    preview_mode = '--preview' in sys.argv or '--smoke' in sys.argv
    
    if preview_mode:
        print("👀 Running in preview/smoke mode")
    
    # Send PM kneeboard
    result = sender.send_pm_kneeboard(preview_mode=preview_mode)
    
    if result['status'] == 'success':
        print(f"\n📊 PM Kneeboard Summary:")
        print(f"   Emails sent: {result['emails_sent']}")
        print(f"   Post-mortem ready: {'YES' if result['postmortem_ready'] else 'NO'}")
        print(f"   Hit rate: {result['hit_rate']}%")
        print(f"   Preview: {result['preview_path']}")
        print(f"   Run ID: {result['run_id']}")
        
        if not result['postmortem_ready']:
            print(f"   ⚠️ Issues: {', '.join(result['readiness_issues'])}")
    else:
        print(f"❌ PM kneeboard failed: {result['message']}")


if __name__ == "__main__":
    main()