# File: daily_digest_email_notion_sync.py
# Title: Claude Daily Digest + Notion Dashboard Sync
# Commit Notes: Sends daily summary email and syncs dashboard metrics to Notion

import os
import smtplib
import re
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

# Configuration
EMAIL_SENDER = os.getenv("DIGEST_EMAIL_FROM")
EMAIL_RECEIVER = os.getenv("DIGEST_EMAIL_TO")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
NOTION_TOKEN = os.getenv("NOTION_API_KEY")
NOTION_DB_ID = os.getenv("NOTION_DASHBOARD_DB")
PROMPT_DIR = "claude_prompts"

# Optional Notion import
try:
    from notion_client import Client as NotionClient
    NOTION_AVAILABLE = True
except ImportError:
    NOTION_AVAILABLE = False
    print("WARNING: notion-client not installed. Notion sync disabled.")

def get_prompt_summary(target_date=None):
    """Extract prompt data for a specific date (defaults to today)"""
    if target_date is None:
        target_date = datetime.utcnow().date()
    
    if not os.path.exists(PROMPT_DIR):
        print(f"WARNING: Prompt directory '{PROMPT_DIR}' not found.")
        return pd.DataFrame()
    
    rows = []
    target_date_str = target_date.strftime("%Y%m%d")
    
    for fname in os.listdir(PROMPT_DIR):
        if not fname.endswith(".md"):
            continue
            
        fpath = os.path.join(PROMPT_DIR, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                content = f.read()
            
            # Check if this prompt was created on target date
            timestamp_match = re.search(r"Created: (\d{8}T\d{6})", content)
            if not timestamp_match:
                continue
                
            creation_date = timestamp_match.group(1)[:8]  # Extract YYYYMMDD
            if creation_date != target_date_str:
                continue
            
            # Extract metadata
            title_match = re.search(r"^# (.+?)$", content, re.MULTILINE)
            title = title_match.group(1).strip() if title_match else "Untitled"
            
            tags = "none"
            tags_match = re.search(r"🏷 Tags: (.+?)$", content, re.MULTILINE)
            if tags_match:
                tags = tags_match.group(1).strip()
            
            # Extract content sections
            prompt_section = ""
            response_section = ""
            
            if "## 💭 Prompt" in content:
                parts = content.split("## 💭 Prompt", 1)
                if len(parts) > 1:
                    prompt_part = parts[1]
                    if "## 🤖 Response" in prompt_part:
                        prompt_section = prompt_part.split("## 🤖 Response")[0].strip()
                        response_section = prompt_part.split("## 🤖 Response")[1].strip()
                    else:
                        prompt_section = prompt_part.strip()
            
            rows.append({
                "title": title,
                "tags": tags,
                "prompt_words": len(prompt_section.split()) if prompt_section else 0,
                "response_words": len(response_section.split()) if response_section else 0,
                "total_words": len(content.split()),
                "created_date": target_date_str,
                "filename": fname
            })
            
        except Exception as e:
            print(f"ERROR: Failed to process {fname}: {e}")
            continue
    
    return pd.DataFrame(rows)

def create_digest_html(df, target_date):
    """Create HTML email content for the digest"""
    date_str = target_date.strftime("%B %d, %Y")
    
    if df.empty:
        return f"""
        <html>
        <body>
        <h2>Claude Daily Digest - {date_str}</h2>
        <p>No Claude prompts were run today.</p>
        <p><em>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
        </body>
        </html>
        """
    
    total_prompts = len(df)
    total_words = df["total_words"].sum()
    avg_words = df["total_words"].mean()
    
    # Tag analysis
    all_tags = []
    for tags_str in df["tags"]:
        if tags_str != "none":
            all_tags.extend([tag.strip() for tag in tags_str.split(",") if tag.strip()])
    
    top_tags = pd.Series(all_tags).value_counts().head(3) if all_tags else pd.Series()
    
    # Find longest and shortest prompts
    longest_prompt = df.iloc[df["total_words"].idxmax()] if not df.empty else None
    
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; margin: 20px;">
        <h2 style="color: #2c3e50;">Claude Daily Digest - {date_str}</h2>
        
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 15px 0;">
            <h3 style="color: #34495e; margin-top: 0;">Summary Statistics</h3>
            <ul style="list-style-type: none; padding-left: 0;">
                <li><strong>Total Prompts:</strong> {total_prompts}</li>
                <li><strong>Total Words:</strong> {total_words:,}</li>
                <li><strong>Average Words per Prompt:</strong> {avg_words:.0f}</li>
                <li><strong>Unique Tags Used:</strong> {len(set(all_tags)) if all_tags else 0}</li>
            </ul>
        </div>
        
        {f'''
        <div style="background-color: #e8f5e8; padding: 15px; border-radius: 8px; margin: 15px 0;">
            <h3 style="color: #27ae60; margin-top: 0;">Top Tags</h3>
            <ul>
                {"".join([f"<li><strong>{tag}:</strong> {count} prompts</li>" for tag, count in top_tags.items()])}
            </ul>
        </div>
        ''' if not top_tags.empty else ''}
        
        {f'''
        <div style="background-color: #fff3cd; padding: 15px; border-radius: 8px; margin: 15px 0;">
            <h3 style="color: #856404; margin-top: 0;">Longest Prompt</h3>
            <p><strong>Title:</strong> {longest_prompt["title"]}</p>
            <p><strong>Words:</strong> {longest_prompt["total_words"]}</p>
            <p><strong>Tags:</strong> {longest_prompt["tags"]}</p>
        </div>
        ''' if longest_prompt is not None else ''}
        
        <div style="background-color: #f1f3f4; padding: 15px; border-radius: 8px; margin: 15px 0;">
            <h3 style="color: #5f6368; margin-top: 0;">All Prompts Today</h3>
            <ol>
                {"".join([f'<li><strong>{row["title"]}</strong> ({row["total_words"]} words, tags: {row["tags"]})</li>' for _, row in df.iterrows()])}
            </ol>
        </div>
        
        <p style="color: #6c757d; font-size: 12px; margin-top: 30px;">
            <em>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by Claude Digest System</em>
        </p>
    </body>
    </html>
    """
    
    return html

def send_digest_email(df, target_date):
    """Send the digest email"""
    if not all([EMAIL_SENDER, EMAIL_RECEIVER, SMTP_USER, SMTP_PASSWORD]):
        print("ERROR: Email configuration incomplete. Skipping email send.")
        return False
    
    try:
        html_content = create_digest_html(df, target_date)
        
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Claude Daily Digest - {target_date.strftime('%Y-%m-%d')}"
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        print(f"SUCCESS: Digest email sent to {EMAIL_RECEIVER}")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to send email: {e}")
        return False

def sync_notion_dashboard(df, target_date):
    """Sync data to Notion dashboard"""
    if not NOTION_AVAILABLE or not NOTION_TOKEN or not NOTION_DB_ID:
        print("WARNING: Notion sync disabled (missing credentials or library)")
        return False
    
    if df.empty:
        print("INFO: No data to sync to Notion")
        return True
    
    try:
        notion = NotionClient(auth=NOTION_TOKEN)
        
        for _, row in df.iterrows():
            # Parse tags for Notion multi-select
            tag_list = []
            if row["tags"] != "none":
                tag_list = [{"name": tag.strip()} for tag in row["tags"].split(",") if tag.strip()]
            
            notion.pages.create(
                parent={"database_id": NOTION_DB_ID},
                properties={
                    "Title": {"title": [{"text": {"content": row["title"]}}]},
                    "Tags": {"multi_select": tag_list},
                    "Prompt Words": {"number": row["prompt_words"]},
                    "Response Words": {"number": row["response_words"]},
                    "Total Words": {"number": row["total_words"]},
                    "Date": {"date": {"start": target_date.isoformat()}},
                    "Filename": {"rich_text": [{"text": {"content": row["filename"]}}]}
                }
            )
        
        print(f"SUCCESS: Synced {len(df)} prompts to Notion dashboard")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to sync to Notion: {e}")
        return False

def main():
    """Main execution function"""
    print("Claude Daily Digest Generator")
    print("-" * 40)
    
    # Allow running for different dates via command line or default to today
    target_date = datetime.utcnow().date()
    print(f"Generating digest for: {target_date}")
    
    # Get prompt data
    df = get_prompt_summary(target_date)
    print(f"Found {len(df)} prompts for {target_date}")
    
    # Send email digest
    email_success = send_digest_email(df, target_date)
    
    # Sync to Notion
    notion_success = sync_notion_dashboard(df, target_date)
    
    # Summary
    print("\nSummary:")
    print(f"Email sent: {'Yes' if email_success else 'No'}")
    print(f"Notion synced: {'Yes' if notion_success else 'No'}")
    print(f"Prompts processed: {len(df)}")

if __name__ == "__main__":
    main()