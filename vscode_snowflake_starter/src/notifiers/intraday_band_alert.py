#!/usr/bin/env python3
"""
Intraday Band Alert
Monitors live prices and alerts Slack if price exits forecast bands
"""

import os
import sys
import logging
from datetime import datetime, date
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

def get_live_price_polygon(symbol, api_key):
    """Get live price from Polygon.io API"""
    try:
        url = f"https://api.polygon.io/v2/last/nbbo/{symbol}?apikey={api_key}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'OK' and 'results' in data:
                # Use bid/ask midpoint as live price
                bid = data['results'].get('bid', 0)
                ask = data['results'].get('ask', 0)
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
        logging.warning(f"Polygon API failed for {symbol}: {response.status_code}")
        return None
    except Exception as e:
        logging.error(f"Polygon API error for {symbol}: {e}")
        return None

def get_live_price_cache(symbol, engine):
    """Get live price from Snowflake LIVE_PRICE_CACHE view"""
    try:
        query = text("SELECT price FROM LIVE_PRICE_CACHE WHERE symbol = :symbol ORDER BY updated_at DESC LIMIT 1")
        with engine.connect() as conn:
            result = conn.execute(query, {'symbol': symbol})
            row = result.fetchone()
            if row:
                return float(row[0])
        logging.warning(f"No cached price found for {symbol}")
        return None
    except Exception as e:
        logging.error(f"Cache lookup error for {symbol}: {e}")
        return None

def get_live_price(symbol, polygon_api_key=None, engine=None):
    """Pluggable interface to get live price with fallback"""
    # Try Polygon.io first if API key is available
    if polygon_api_key:
        price = get_live_price_polygon(symbol, polygon_api_key)
        if price is not None:
            return price
    
    # Fallback to cache if engine is available
    if engine:
        price = get_live_price_cache(symbol, engine)
        if price is not None:
            return price
    
    return None

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
    
    # Get notification and data settings
    slack_webhooks = [
        os.getenv("SLACK_WEBHOOK_URL"),
        os.getenv("SLACK_WEBHOOK_URL1"),
        os.getenv("SLACK_WEBHOOK_URL2"),
        os.getenv("SLACK_WEBHOOK_URL3")
    ]
    slack_webhooks = [w for w in slack_webhooks if w and w.startswith('https://hooks.slack.com/services/')]
    
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    band_pct = float(os.getenv("BAND_PCT", "0.02"))
    
    # Check if we have any price source
    if not polygon_api_key:
        logging.warning("No POLYGON_API_KEY configured, will try Snowflake cache fallback")
    
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
        # Get today's forecasts
        today = date.today()
        
        # Try to get forecasts for today, fallback to latest available
        forecast_query = text("""
        SELECT 
            f.symbol,
            f.forecast_date,
            f.forecast,
            f.model_version,
            opt.atm_straddle
        FROM FORECAST_DAILY f
        LEFT JOIN (
            SELECT symbol, date, atm_straddle
            FROM OPTIONS_SUMMARY 
            WHERE date = :today
        ) opt ON f.symbol = opt.symbol AND f.forecast_date = opt.date
        WHERE f.forecast_date = :today
        ORDER BY f.symbol
        """)
        
        try:
            forecast_df = pd.read_sql(forecast_query, conn, params={'today': today})
        except Exception as e:
            # OPTIONS_SUMMARY might not exist, try without it
            logging.warning(f"OPTIONS_SUMMARY not available, using fallback: {e}")
            fallback_query = text("""
            SELECT 
                f.symbol,
                f.forecast_date,
                f.forecast,
                f.model_version,
                NULL as atm_straddle
            FROM FORECAST_DAILY f
            WHERE f.forecast_date = :today
            ORDER BY f.symbol
            """)
            forecast_df = pd.read_sql(fallback_query, conn, params={'today': today})
        
        # If no forecasts for today, try latest available
        if forecast_df.empty:
            logging.info(f"No forecasts for today ({today}), trying latest available")
            latest_query = text("""
            SELECT 
                f.symbol,
                f.forecast_date,
                f.forecast,
                f.model_version,
                NULL as atm_straddle
            FROM FORECAST_DAILY f
            WHERE f.forecast_date = (SELECT MAX(forecast_date) FROM FORECAST_DAILY)
            ORDER BY f.symbol
            """)
            forecast_df = pd.read_sql(latest_query, conn)
        
        if forecast_df.empty:
            logging.warning("No forecasts found in FORECAST_DAILY")
            if not polygon_api_key:
                logging.error("No price source configured and no forecasts available")
                sys.exit(1)
            print("ALERTS: checked=0 breaches=0 posted_to_slack=false")
            return
        
        logging.info(f"Found {len(forecast_df)} forecasts to check")
        
        # Calculate forecast bands
        forecast_df['band'] = forecast_df.apply(
            lambda row: row['atm_straddle'] if pd.notna(row['atm_straddle']) else row['forecast'] * band_pct,
            axis=1
        )
        forecast_df['band_lower'] = forecast_df['forecast'] - forecast_df['band']
        forecast_df['band_upper'] = forecast_df['forecast'] + forecast_df['band']
        
        # Check each symbol for band breaches
        checked = 0
        breaches = []
        
        for _, row in forecast_df.iterrows():
            symbol = row['symbol']
            checked += 1
            
            # Get live price
            live_price = get_live_price(symbol, polygon_api_key, engine)
            
            if live_price is None:
                logging.warning(f"No live price available for {symbol}")
                continue
            
            # Check for band breach
            if live_price < row['band_lower'] or live_price > row['band_upper']:
                # Calculate percentage outside band
                if live_price < row['band_lower']:
                    pct_outside = ((row['band_lower'] - live_price) / row['forecast']) * 100
                    direction = "below"
                else:
                    pct_outside = ((live_price - row['band_upper']) / row['forecast']) * 100
                    direction = "above"
                
                breach_info = {
                    'symbol': symbol,
                    'forecast_date': row['forecast_date'],
                    'forecast': row['forecast'],
                    'band_lower': row['band_lower'],
                    'band_upper': row['band_upper'],
                    'live_price': live_price,
                    'pct_outside': pct_outside,
                    'direction': direction,
                    'model_version': row['model_version']
                }
                breaches.append(breach_info)
                
                logging.warning(f"BREACH: {symbol} @ {live_price:.2f} is {direction} band [{row['band_lower']:.2f}, {row['band_upper']:.2f}] by {pct_outside:.1f}%")
        
        # Post breach alerts to Slack
        posted_to_slack = False
        if breaches and slack_webhooks:
            for breach in breaches:
                slack_payload = {
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"⚠️ Band Break — {breach['symbol']}"
                            }
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Date:* {breach['forecast_date']}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Forecast:* ${breach['forecast']:.2f}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Band:* ${breach['band_lower']:.2f} - ${breach['band_upper']:.2f}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Live Price:* ${breach['live_price']:.2f}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Outside Band:* {breach['pct_outside']:.1f}% {breach['direction']}"
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Model:* {breach['model_version']}"
                                }
                            ]
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"Alert generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}"
                                }
                            ]
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
                        else:
                            logging.error(f"Slack webhook failed: {response.status_code}")
                    except Exception as e:
                        logging.error(f"Slack post error: {e}")
                
                if success_count > 0:
                    posted_to_slack = True
                    logging.info(f"Posted breach alert for {breach['symbol']} to Slack")
        
        elif breaches and not slack_webhooks:
            logging.warning(f"Found {len(breaches)} breaches but no Slack webhooks configured")
        
        # Print final status line
        print(f"ALERTS: checked={checked} breaches={len(breaches)} posted_to_slack={str(posted_to_slack).lower()}")
        
        # Exit with non-zero if breaches occurred and no Slack hooks configured
        if breaches and not slack_webhooks:
            logging.error("Breaches detected but no notification system configured")
            sys.exit(1)

if __name__ == "__main__":
    main()