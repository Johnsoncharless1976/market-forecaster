# send_email.py
# ✉️ ZeroDay Zen Forecast Email Sender - Backwards Compatible
# Purpose: Supports both legacy 1-arg and new 2-arg calling styles

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Optional import for legacy usage
try:
    from brain import forecast_body
except ImportError:
    forecast_body = "Default forecast content"

load_dotenv()

def send_email(*args):
    """
    Backwards-compatible send_email function supporting both:
    - send_email(body) - legacy 1-arg style 
    - send_email(subject, body) - new 2-arg style
    """
    
    if len(args) == 1:
        # Legacy 1-argument style: send_email(body)
        body = args[0]
        subject = "ZeroDay Zen Forecast"
    elif len(args) == 2:
        # New 2-argument style: send_email(subject, body)
        subject, body = args
    else:
        raise TypeError(f"send_email() takes 1 or 2 positional arguments but {len(args)} were given")
    
    # Use environment variables compatible with send_forecast_email.py
    smtp_user = os.getenv("SMTP_USER") or os.getenv("EMAIL_USER")
    smtp_pass = os.getenv("SMTP_PASS") or os.getenv("EMAIL_PASS") 
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port_str = os.getenv("SMTP_PORT", "587")
    smtp_port = int(smtp_port_str) if smtp_port_str else 587
    email_to = os.getenv("EMAIL_TO")
    
    if not smtp_user or not smtp_pass:
        print("SMTP credentials missing. Ensure SMTP_USER/EMAIL_USER and SMTP_PASS/EMAIL_PASS are set.")
        return False
    
    if not email_to:
        print("EMAIL_TO not configured.")
        return False
    
    try:
        # Determine if body is HTML or plain text
        is_html = "<html>" in body.lower() or "<body>" in body.lower() or "<h1>" in body.lower()
        
        if is_html:
            # Create multipart message for HTML
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = smtp_user
            msg['To'] = email_to
            
            # Attach HTML part
            html_part = MIMEText(body, 'html')
            msg.attach(html_part)
        else:
            # Create simple text message
            msg = MIMEText(body, 'plain')
            msg["Subject"] = subject
            msg["From"] = smtp_user
            msg["To"] = email_to
        
        # Use SMTP with STARTTLS (matches send_forecast_email.py)
        if smtp_port == 465:
            # Use SSL for port 465 (legacy behavior)
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            # Use STARTTLS for port 587 (current standard)
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        
        print(f"Email sent to {email_to}: {subject}")
        return True
        
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

if __name__ == "__main__":
    # Test both calling styles
    print("Testing legacy 1-arg style:")
    send_email(forecast_body)
    
    print("\nTesting new 2-arg style:")
    send_email("Test Subject", "Test body content")
