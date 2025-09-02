#!/usr/bin/env python3
"""
AM Kneeboard Generator
Summarizes latest forecasts and posts to Slack + Notion Daily DB
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import requests
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)

def main():
    # Load environment variables
    load_dotenv()
    
    # Get Snowflake connection details from .env
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")
    
    # Validate required environment variables
    if not all([account, user, warehouse, database, schema]):
        logging.error("Missing required Snowflake environment variables")
        sys.exit(1)
    
    if not password and not private_key_path:
        logging.error("Must provide either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
        sys.exit(1)
    
    # Get notification settings
    slack_webhooks = [
        os.getenv("SLACK_WEBHOOK_URL"),
        os.getenv("SLACK_WEBHOOK_URL1"),
        os.getenv("SLACK_WEBHOOK_URL2"),
        os.getenv("SLACK_WEBHOOK_URL3")
    ]
    slack_webhooks = [w for w in slack_webhooks if w and w.startswith('https://hooks.slack.com/services/')]
    
    notion_token = os.getenv("NOTION_TOKEN")
    notion_daily_db_id = os.getenv("NOTION_DAILY_DB_ID")
    band_pct = float(os.getenv("BAND_PCT", "0.02"))
    
    # Create Snowflake engine
    if private_key_path and os.path.exists(private_key_path):
        connection_string = f"snowflake://{user}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
        engine = create_engine(connection_string, connect_args={
            "private_key_path": private_key_path,
            "private_key_passphrase": private_key_passphrase
        })
    else:
        connection_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
        engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Get latest trading day from FORECAST_DAILY
        latest_date_query = text("SELECT MAX(forecast_date) as latest_date FROM FORECAST_DAILY")
        result = conn.execute(latest_date_query)
        latest_date = result.fetchone()[0]
        
        if not latest_date:
            logging.warning("No forecasts found in FORECAST_DAILY")
            print("KNEEBOARD: posted_to_slack=false posted_to_notion=false rows=0")
            return
        
        logging.info(f"Latest forecast date: {latest_date}")
        
        # Fetch forecasts with optional actuals and ATM straddle data
        forecast_query = text("""
        SELECT 
            f.symbol,
            f.forecast_date as date,
            f.forecast,
            f.model_version,
            a.close as latest_close,
            opt.atm_straddle
        FROM FORECAST_DAILY f
        LEFT JOIN FEATURES_DAILY a 
            ON f.symbol = a.symbol 
            AND f.forecast_date = a.trade_date
        LEFT JOIN (
            SELECT symbol, date, atm_straddle
            FROM OPTIONS_SUMMARY 
            WHERE date = :latest_date
        ) opt ON f.symbol = opt.symbol AND f.forecast_date = opt.date
        WHERE f.forecast_date = :latest_date
        ORDER BY f.symbol
        """)
        
        try:
            forecast_df = pd.read_sql(forecast_query, conn, params={'latest_date': latest_date})
        except Exception as e:
            # OPTIONS_SUMMARY might not exist, try without it
            logging.warning(f"OPTIONS_SUMMARY not available, using fallback: {e}")
            fallback_query = text("""
            SELECT 
                f.symbol,
                f.forecast_date as date,
                f.forecast,
                f.model_version,
                a.close as latest_close,
                NULL as atm_straddle
            FROM FORECAST_DAILY f
            LEFT JOIN FEATURES_DAILY a 
                ON f.symbol = a.symbol 
                AND f.forecast_date = a.trade_date
            WHERE f.forecast_date = :latest_date
            ORDER BY f.symbol
            """)
            forecast_df = pd.read_sql(fallback_query, conn, params={'latest_date': latest_date})
        
        if forecast_df.empty:
            logging.warning(f"No forecasts found for date: {latest_date}")
            print("KNEEBOARD: posted_to_slack=false posted_to_notion=false rows=0")
            return
        
        # Calculate ATM straddle bands
        forecast_df['band'] = forecast_df.apply(
            lambda row: row['atm_straddle'] if pd.notna(row['atm_straddle']) else row['forecast'] * band_pct,
            axis=1
        )
        forecast_df['band_lower'] = forecast_df['forecast'] - forecast_df['band']
        forecast_df['band_upper'] = forecast_df['forecast'] + forecast_df['band']
        
        # Create text table
        table_lines = [
            "Symbol    Date        Forecast    Latest     Band Lower  Band Upper",
            "--------  ----------  ----------  ---------  ----------  ----------"
        ]
        
        for _, row in forecast_df.iterrows():
            latest_str = f"{row['latest_close']:8.2f}" if pd.notna(row['latest_close']) else "     N/A"
            line = f"{row['symbol']:8s}  {row['date']}  {row['forecast']:10.2f}  {latest_str}  {row['band_lower']:10.2f}  {row['band_upper']:10.2f}"
            table_lines.append(line)
        
        table_text = "\n".join(table_lines)
        date_str = latest_date.strftime("%Y-%m-%d")
        
        logging.info(f"Generated kneeboard for {len(forecast_df)} symbols")
        
        # Post to Slack
        posted_to_slack = False
        if slack_webhooks:
            slack_payload = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"🚀 AM Kneeboard — {date_str}"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"Daily forecast summary for {len(forecast_df)} symbols"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"```\n{table_text}\n```"
                        }
                    }
                ]
            }
            
            success_count = 0
            for webhook_url in slack_webhooks:
                try:
                    response = requests.post(
                        webhook_url,
                        json=slack_payload,
                        headers={'Content-Type': 'application/json'}
                    )
                    if response.status_code == 200:
                        success_count += 1
                        logging.info(f"Posted to Slack webhook: {webhook_url[:50]}...")
                    else:
                        logging.error(f"Slack webhook failed: {response.status_code}")
                except Exception as e:
                    logging.error(f"Slack post error: {e}")
            
            posted_to_slack = success_count > 0
        
        # Post to Notion
        posted_to_notion = False
        if notion_token and notion_daily_db_id:
            try:
                notion_payload = {
                    "parent": {"database_id": notion_daily_db_id},
                    "properties": {
                        "Name": {
                            "title": [
                                {
                                    "type": "text",
                                    "text": {"content": f"AM Kneeboard — {date_str}"}
                                }
                            ]
                        },
                        "Date": {
                            "date": {"start": date_str}
                        },
                        "Items": {
                            "number": len(forecast_df)
                        },
                        "Note": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {"content": f"Daily forecast summary with ATM straddle bands for {len(forecast_df)} symbols"}
                                }
                            ]
                        }
                    },
                    "children": [
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [
                                    {
                                        "type": "text",
                                        "text": {"content": f"Forecast summary for {date_str} covering {len(forecast_df)} symbols with baseline persistence model."}
                                    }
                                ]
                            }
                        },
                        {
                            "object": "block",
                            "type": "code",
                            "code": {
                                "language": "plain text",
                                "rich_text": [
                                    {
                                        "type": "text",
                                        "text": {"content": table_text}
                                    }
                                ]
                            }
                        }
                    ]
                }
                
                response = requests.post(
                    "https://api.notion.com/v1/pages",
                    json=notion_payload,
                    headers={
                        "Authorization": f"Bearer {notion_token}",
                        "Notion-Version": "2022-06-28",
                        "Content-Type": "application/json"
                    }
                )
                
                if response.status_code == 200:
                    page_id = response.json().get('id', 'unknown')
                    logging.info(f"Created Notion page: {page_id}")
                    posted_to_notion = True
                else:
                    logging.error(f"Notion API failed: {response.status_code} - {response.text}")
                    
            except Exception as e:
                logging.error(f"Notion post error: {e}")
        
        # Print final status line
        print(f"KNEEBOARD: posted_to_slack={str(posted_to_slack).lower()} posted_to_notion={str(posted_to_notion).lower()} rows={len(forecast_df)}")

if __name__ == "__main__":
    main()