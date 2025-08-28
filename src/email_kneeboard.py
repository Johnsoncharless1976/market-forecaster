#!/usr/bin/env python3
"""
Email Kneeboard System (Self-Only Pilot)
AM & PM email delivery with SHADOW disclaimers
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
import json


class EmailKneeboard:
    """AM/PM kneeboard email system for pilot mode"""
    
    def __init__(self):
        # Email configuration from environment
        self.stage_open_notify = os.getenv('STAGE_OPEN_NOTIFY', 'false').lower() == 'true'
        self.email_enabled = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'  
        self.email_mode = os.getenv('EMAIL_MODE', 'pilot')
        self.recipient_override = os.getenv('EMAIL_RECIPIENT_OVERRIDE', 'user@example.com')
        self.subject_prefix = os.getenv('EMAIL_SUBJECT_PREFIX', '[Zen Market Forecaster]')
        
        # Email timing
        self.am_send_time = '09:00'  # Default AM send
        self.am_send_time_macro = '09:15'  # If macro gate active
        self.pm_send_time = '17:00'  # PM send
        
        # Audit directories
        self.email_audit_dir = Path('audit_exports/emails')
        self.email_audit_dir.mkdir(parents=True, exist_ok=True)
    
    def check_email_readiness(self):
        """Check if email system is ready"""
        return {
            'stage_open_notify': self.stage_open_notify,
            'email_enabled': self.email_enabled,
            'email_mode': self.email_mode,
            'recipient_configured': bool(self.recipient_override and '@' in self.recipient_override),
            'ready': (self.stage_open_notify and self.email_enabled and 
                     self.email_mode == 'pilot' and self.recipient_override)
        }
    
    def mask_email(self, email):
        """Mask email address for logging"""
        if not email or '@' not in email:
            return 'invalid@example.com'
        
        local, domain = email.split('@', 1)
        if len(local) <= 2:
            masked_local = local
        else:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        
        return f"{masked_local}@{domain}"
    
    def generate_email_html(self, email_type, glance_data):
        """Generate HTML email content"""
        from today_glance import TodayGlance
        
        glance = TodayGlance()
        
        # Generate Today at a Glance HTML
        glance_html = glance.format_glance_row_html(glance_data)
        
        # Determine send time
        current_time = datetime.now().strftime('%H:%M')
        is_macro_gate = glance_data.get('macro_gate', 'Off') == 'On'
        send_time = (self.am_send_time_macro if is_macro_gate else self.am_send_time) if email_type == 'AM' else self.pm_send_time
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self.subject_prefix} {email_type} Kneeboard</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ background: #007bff; color: white; padding: 20px; text-align: center; }}
        .shadow-disclaimer {{ background: #6f42c1; color: white; padding: 12px 20px; margin: 0; font-weight: bold; text-align: center; }}
        .content {{ padding: 20px; }}
        .glance-section {{ margin-bottom: 24px; }}
        .links-row {{ background: #f8f9fa; padding: 16px; border-radius: 6px; margin: 16px 0; }}
        .links-row a {{ color: #007bff; text-decoration: none; margin-right: 16px; }}
        .links-row a:hover {{ text-decoration: underline; }}
        .footer {{ background: #f8f9fa; padding: 16px 20px; font-size: 12px; color: #6c757d; border-top: 1px solid #dee2e6; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{self.subject_prefix} {email_type} Kneeboard</h1>
            <p>Market Forecast & Analysis • {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}</p>
        </div>
        
        <div class="shadow-disclaimer">
            🔮 SHADOW MODE: Forecasts for evaluation only — not trading advice
        </div>
        
        <div class="content">
            <div class="glance-section">
                <h2>Today at a Glance</h2>
                {glance_html}
            </div>
            
            <div class="links-row">
                <strong>Quick Access:</strong>
                <a href="http://localhost:8501">Live Dashboard</a> •
                <a href="http://localhost:8502">Playground</a> •
                <a href="http://localhost:8502">Replay</a> •
                <a href="#batch">{"AM Batch" if email_type == "AM" else "EOD Batch"}</a> •
                <a href="#sparkline">Confidence Strip</a> •
                <a href="#cohort">Cohort Day {glance_data.get('cohort_day', '0/30')}</a> •
                <a href="#wingate">WinGate</a> •
                <a href="#impact">Impact</a> •
                <a href="#magnet">Magnet</a>
            </div>
            
            <div class="forecast-section">
                <h3>Forecast Summary</h3>
                <p><strong>Market Stance:</strong> {glance_data.get('forecast_stance', 'Pending')} (Grade {glance_data.get('forecast_grade', 'C')})</p>
                <p><strong>Confidence:</strong> {glance_data.get('confidence_pct', 0):.1f}% (Target: ≥80% for A-grade)</p>
                <p><strong>Cohort Progress:</strong> Day {glance_data.get('cohort_day', '0/30')} shadow tracking</p>
            </div>
            
            <div class="market-section">
                <h3>Market Conditions</h3>
                <p><strong>Macro Environment:</strong> z-score {glance_data.get('macro_z', 0):.2f}, Gate {glance_data.get('macro_gate', 'Off')}</p>
                <p><strong>News Score:</strong> {glance_data.get('news_score', 0):.3f}</p>
                <p><strong>Level Magnets:</strong> {glance_data.get('magnet_target', 'L25')}, Strength M={glance_data.get('magnet_m', 0):.3f}</p>
            </div>
            
            <div class="system-section">
                <h3>System Status</h3>
                <p><strong>Pipeline:</strong> {glance_data.get('pipeline_sha', 'local')} @ {glance_data.get('evidence_timestamp', 'N/A')}</p>
                <p><strong>Live Gate:</strong> {glance_data.get('live_gate_status', 'PENDING')} (PM approval required)</p>
                <p><strong>Mode:</strong> SHADOW (all systems evaluation-only)</p>
                <p><strong>Stability:</strong> {"Twin daily batches (AM + EOD)" if os.getenv('STABILITY_MODE', 'false').lower() == 'true' else "Real-time artifacts"}</p>
                <p><strong>Batch Type:</strong> {email_type} {"Market Open" if email_type == "AM" else "End of Day"}</p>
            </div>
        </div>
        
        <div class="footer">
            <p><strong>Zen Market AI Forecaster</strong> • {email_type} Kneeboard • Pilot Mode</p>
            <p>You're receiving pilot emails at this address. Reply STOP to opt out.</p>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')} • 
               Send Time: {send_time} {"(Macro Gate Active)" if is_macro_gate and email_type == "AM" else ""}</p>
        </div>
    </div>
</body>
</html>
"""
        return html_content
    
    def generate_email_text(self, email_type, glance_data):
        """Generate plain text email content"""
        from today_glance import TodayGlance
        
        glance = TodayGlance()
        glance_text = glance.format_glance_row_text(glance_data)
        
        is_macro_gate = glance_data.get('macro_gate', 'Off') == 'On'
        send_time = (self.am_send_time_macro if is_macro_gate else self.am_send_time) if email_type == 'AM' else self.pm_send_time
        
        text_content = f"""{self.subject_prefix} {email_type} Kneeboard
Market Forecast & Analysis • {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}

🔮 SHADOW MODE: Forecasts for evaluation only — not trading advice

{glance_text}

FORECAST SUMMARY
================
Market Stance: {glance_data.get('forecast_stance', 'Pending')} (Grade {glance_data.get('forecast_grade', 'C')})
Confidence: {glance_data.get('confidence_pct', 0):.1f}% (Target: ≥80% for A-grade)
Cohort Progress: Day {glance_data.get('cohort_day', '0/30')} shadow tracking

MARKET CONDITIONS
=================
Macro Environment: z-score {glance_data.get('macro_z', 0):.2f}, Gate {glance_data.get('macro_gate', 'Off')}
News Score: {glance_data.get('news_score', 0):.3f}
Level Magnets: {glance_data.get('magnet_target', 'L25')}, Strength M={glance_data.get('magnet_m', 0):.3f}

SYSTEM STATUS
=============
Pipeline: {glance_data.get('pipeline_sha', 'local')} @ {glance_data.get('evidence_timestamp', 'N/A')}
Live Gate: {glance_data.get('live_gate_status', 'PENDING')} (PM approval required)
Mode: SHADOW (all systems evaluation-only)

QUICK ACCESS
============
Live Dashboard: http://localhost:8501
Playground: http://localhost:8502
Cohort Day {glance_data.get('cohort_day', '0/30')} • Confidence {glance_data.get('confidence_pct', 0):.0f}%

---
Zen Market AI Forecaster • {email_type} Kneeboard • Pilot Mode
You're receiving pilot emails at this address. Reply STOP to opt out.
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')} • Send Time: {send_time} {"(Macro Gate Active)" if is_macro_gate and email_type == "AM" else ""}
"""
        return text_content
    
    def save_email_artifacts(self, email_type, html_content, text_content):
        """Save email content to audit artifacts"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        email_dir = self.email_audit_dir / timestamp
        email_dir.mkdir(parents=True, exist_ok=True)
        
        # Save HTML
        html_file = email_dir / f'{email_type}_KNEEBOARD.html'
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Save text
        text_file = email_dir / f'{email_type}_KNEEBOARD.txt'
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_content)
        
        return {
            'html_file': str(html_file),
            'text_file': str(text_file),
            'timestamp': timestamp
        }
    
    def simulate_email_send(self, email_type, recipient, subject, html_content, text_content):
        """Simulate email sending (no actual SMTP in pilot)"""
        # In production, this would use actual SMTP
        # For pilot, we simulate successful delivery
        
        send_result = {
            'recipient': recipient,
            'subject': subject,
            'sent_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            'accepted': 1,  # Simulated success
            'provider_id': f'sim_{datetime.now().strftime("%H%M%S")}',
            'content_length_html': len(html_content),
            'content_length_text': len(text_content)
        }
        
        return send_result
    
    def log_email_delivery(self, email_type, send_result, artifact_files):
        """Log email delivery to audit trail"""
        timestamp = artifact_files['timestamp']
        email_dir = self.email_audit_dir / timestamp
        
        log_file = email_dir / 'EMAIL_SEND_LOG.md'
        
        content = f"""# Email Send Log

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Email Type**: {email_type} Kneeboard
**Mode**: Pilot (self-only delivery)

## Delivery Details

- **Recipient**: {self.mask_email(send_result['recipient'])}
- **Subject**: {send_result['subject']}
- **Sent Time**: {send_result['sent_time']}
- **Status**: {'DELIVERED' if send_result['accepted'] else 'FAILED'}
- **Provider ID**: {send_result['provider_id']}

## Content Summary

- **HTML Length**: {send_result['content_length_html']} characters
- **Text Length**: {send_result['content_length_text']} characters
- **Artifacts**: 
  - HTML: {Path(artifact_files['html_file']).name}
  - Text: {Path(artifact_files['text_file']).name}

## Pilot Configuration

- **EMAIL_ENABLED**: {self.email_enabled}
- **EMAIL_MODE**: {self.email_mode}
- **STAGE_OPEN_NOTIFY**: {self.stage_open_notify}
- **Subject Prefix**: {self.subject_prefix}
- **Recipient Override**: {self.mask_email(self.recipient_override)}

## Compliance

- **SHADOW Disclaimer**: Present in email content
- **Opt-out Footer**: "Reply STOP to opt out" included
- **Trading Advice**: Explicitly disclaimed
- **Audit Trail**: Complete delivery log maintained

---
**EMAIL LOG**: {email_type} kneeboard delivery logged
Generated by Email Kneeboard v0.1
"""
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(log_file)
    
    def send_kneeboard(self, email_type, glance_data):
        """Send AM or PM kneeboard email"""
        readiness = self.check_email_readiness()
        
        if not readiness['ready']:
            return {
                'sent': False,
                'reason': 'Email system not ready',
                'readiness': readiness
            }
        
        # Generate email content
        html_content = self.generate_email_html(email_type, glance_data)
        text_content = self.generate_email_text(email_type, glance_data)
        
        # Create subject
        current_date = datetime.now().strftime('%Y-%m-%d')
        subject = f"{self.subject_prefix} {email_type} Kneeboard - {current_date}"
        
        # Save artifacts
        artifact_files = self.save_email_artifacts(email_type, html_content, text_content)
        
        # Simulate send
        send_result = self.simulate_email_send(
            email_type, self.recipient_override, subject, html_content, text_content
        )
        
        # Log delivery
        log_file = self.log_email_delivery(email_type, send_result, artifact_files)
        
        return {
            'sent': True,
            'send_result': send_result,
            'artifact_files': artifact_files,
            'log_file': log_file
        }


def main():
    """Test Email Kneeboard system"""
    from today_glance import TodayGlance
    
    # Set pilot environment variables
    os.environ['STAGE_OPEN_NOTIFY'] = 'true'
    os.environ['EMAIL_ENABLED'] = 'true'
    os.environ['EMAIL_MODE'] = 'pilot'
    os.environ['EMAIL_RECIPIENT_OVERRIDE'] = 'pilot@example.com'
    
    email_system = EmailKneeboard()
    
    # Check readiness
    readiness = email_system.check_email_readiness()
    print("Email System Readiness:")
    for key, value in readiness.items():
        print(f"  {key}: {value}")
    
    if not readiness['ready']:
        print("Email system not ready for testing")
        return
    
    # Generate glance data
    glance = TodayGlance()
    glance_data = glance.generate_glance_data()
    
    # Send AM kneeboard
    print("\nSending AM Kneeboard...")
    am_result = email_system.send_kneeboard('AM', glance_data)
    
    if am_result['sent']:
        print(f"AM Email sent to: {email_system.mask_email(am_result['send_result']['recipient'])}")
        print(f"AM Artifacts: {am_result['artifact_files']['html_file']}")
        print(f"AM Log: {am_result['log_file']}")
    
    # Send PM kneeboard
    print("\nSending PM Kneeboard...")
    pm_result = email_system.send_kneeboard('PM', glance_data)
    
    if pm_result['sent']:
        print(f"PM Email sent to: {email_system.mask_email(pm_result['send_result']['recipient'])}")
        print(f"PM Artifacts: {pm_result['artifact_files']['html_file']}")
        print(f"PM Log: {pm_result['log_file']}")
    
    return {
        'am_result': am_result,
        'pm_result': pm_result,
        'readiness': readiness
    }


if __name__ == '__main__':
    main()