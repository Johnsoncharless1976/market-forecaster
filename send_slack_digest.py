# File: send_slack_digest.py
# Title: Claude Runner Slack Digest Sender
# Commit Notes: Posts daily Claude digest summary to Slack via webhook

import os
import requests
import pandas as pd
from datetime import datetime
from daily_digest_email_notion_sync import get_prompt_summary

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
DIGEST_DATE = datetime.utcnow().date()

def format_digest_for_slack(df):
    """Format prompt data for Slack message"""
    total = len(df)
    
    # Process tags
    all_tags = []
    for tags_str in df["tags"]:
        if tags_str != "none":
            all_tags.extend([tag.strip() for tag in tags_str.split(",") if tag.strip()])
    
    tag_counts = pd.Series(all_tags).value_counts().head(3)
    top_tags = ", ".join(f"{k} ({v})" for k, v in tag_counts.items()) if not tag_counts.empty else "None"
    
    # Build message
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Claude Digest - {DIGEST_DATE}"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn", 
                "text": f"*Total Prompts:* {total}\n*Top Tags:* {top_tags}\n*Total Words:* {df['total_words'].sum():,}"
            }
        },
        {"type": "divider"}
    ]
    
    # Add individual prompts (max 5)
    for _, row in df.head(5).iterrows():
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"• *{row['title']}*\n_{row['tags']}_ • {row['total_words']} words"
            }
        })
    
    if len(df) > 5:
        blocks.append({
            "type": "context",
            "elements": [{"type": "plain_text", "text": f"...and {len(df) - 5} more prompts"}]
        })
    
    return {"blocks": blocks}

def send_digest():
    """Send digest to Slack"""
    if not SLACK_WEBHOOK_URL:
        print("ERROR: SLACK_WEBHOOK_URL not configured")
        return False
    
    # Get prompt data
    df = get_prompt_summary(DIGEST_DATE)
    
    if df.empty:
        # Send empty digest message
        payload = {
            "text": f"Claude Digest - {DIGEST_DATE}",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"Claude Digest - {DIGEST_DATE}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": "No prompts today. Time to get creative! 🎯"}}
            ]
        }
    else:
        payload = format_digest_for_slack(df)
    
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"SUCCESS: Slack digest sent ({len(df)} prompts)")
            return True
        else:
            print(f"ERROR: Slack response {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"ERROR: Failed to send Slack message: {e}")
        return False

if __name__ == "__main__":
    send_digest()