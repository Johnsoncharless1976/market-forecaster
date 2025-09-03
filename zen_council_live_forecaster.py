# zen_council_live_forecaster.py
#!/usr/bin/env python3
"""
Zen Council Live Forecasting System - Production Operations
Using optimized parameters: 67.9% baseline accuracy achieved
RSI 25/75, VIX 12/24, Volume 1.0x, 3+ confirmations
Bull >0.15%, Bear <-0.05%, Chop ±0.85%
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
from sqlalchemy import create_engine
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class ZenCouncilLiveForecaster:
    def __init__(self):
        self.council_version = "LIVE_1.0"
        self.optimized_params = {
            'rsi_bull_threshold': 25,
            'rsi_bear_threshold': 75,
            'vix_fear_threshold': 24,
            'vix_complacency_threshold': 12,
            'volume_ratio_threshold': 1.0,
            'confirmation_required': 3,
            'bull_success_threshold': 0.15,
            'bear_success_threshold': -0.05,
            'chop_success_range': 0.85
        }
        
    def connect_to_snowflake(self):
        # Read .env file manually as backup
        env_vars = {}
        try:
            with open('.env', 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value.strip('"\'')
        except Exception as e:
            print(f"Error reading .env file: {e}")
        
        # Use manual env vars or os.getenv as fallback
        account = env_vars.get('SNOWFLAKE_ACCOUNT') or os.getenv('SNOWFLAKE_ACCOUNT')
        user = env_vars.get('SNOWFLAKE_USER') or os.getenv('SNOWFLAKE_USER')
        password = env_vars.get('SNOWFLAKE_PASSWORD') or os.getenv('SNOWFLAKE_PASSWORD')
        database = env_vars.get('SNOWFLAKE_DATABASE') or os.getenv('SNOWFLAKE_DATABASE') or 'ZEN_MARKET'
        schema = env_vars.get('SNOWFLAKE_SCHEMA') or os.getenv('SNOWFLAKE_SCHEMA') or 'FORECASTING'
        warehouse = env_vars.get('SNOWFLAKE_WAREHOUSE') or os.getenv('SNOWFLAKE_WAREHOUSE')
        
        if not account:
            print("SNOWFLAKE_ACCOUNT not found in .env file")
            print("Please add SNOWFLAKE_ACCOUNT=your_account to .env file")
            raise Exception("Snowflake credentials not configured")
        
        print(f"Connecting to Snowflake account: {account}")
        return snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse
        )
    
    def load_current_market_data(self, days_back: int = 60) -> pd.DataFrame:
        """Load recent market data for live forecasting"""
        env_vars = {}
        try:
            with open('.env', 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value.strip('"\'')
        except Exception as e:
            print(f"Error reading .env file: {e}")
        
        # Create SQLAlchemy engine
        account = env_vars.get('SNOWFLAKE_ACCOUNT') or os.getenv('SNOWFLAKE_ACCOUNT')
        user = env_vars.get('SNOWFLAKE_USER') or os.getenv('SNOWFLAKE_USER')
        password = env_vars.get('SNOWFLAKE_PASSWORD') or os.getenv('SNOWFLAKE_PASSWORD')
        database = env_vars.get('SNOWFLAKE_DATABASE') or os.getenv('SNOWFLAKE_DATABASE') or 'ZEN_MARKET'
        schema = env_vars.get('SNOWFLAKE_SCHEMA') or os.getenv('SNOWFLAKE_SCHEMA') or 'FORECASTING'
        warehouse = env_vars.get('SNOWFLAKE_WAREHOUSE') or os.getenv('SNOWFLAKE_WAREHOUSE')
        
        engine = create_engine(
            f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
        )
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        query = """
        SELECT 
            s.DATE,
            s.OPEN as spx_open,
            s.HIGH as spx_high, 
            s.LOW as spx_low,
            s.CLOSE as spx_close,
            s.VOLUME as spx_volume,
            v.CLOSE as vix_close
        FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL s
        LEFT JOIN ZEN_MARKET.FORECASTING.VIX_HISTORICAL v ON s.DATE = v.DATE
        WHERE s.DATE >= %(start_date)s AND s.DATE <= %(end_date)s
        ORDER BY s.DATE
        """
        
        df = pd.read_sql(query, engine, params={'start_date': start_date, 'end_date': end_date})
        
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()
        
        print(f"Zen Council live system loaded {len(df)} recent market sessions")
        return df
    
    def calculate_live_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate indicators using optimized Council parameters"""
        df = df.copy()
        
        # Basic calculations
        df['daily_return'] = df['spx_close'].pct_change() * 100
        df['prev_close'] = df['spx_close'].shift(1)
        
        # RSI (14-day) with optimized sensitivity
        delta = df['spx_close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # ATR (14-day) 
        df['tr'] = np.maximum(
            df['spx_high'] - df['spx_low'],
            np.maximum(
                abs(df['spx_high'] - df['prev_close']),
                abs(df['spx_low'] - df['prev_close'])
            )
        )
        df['atr'] = df['tr'].rolling(window=14).mean()
        
        # Support/Resistance - Optimized 1.2x ATR
        df['support_level'] = df['spx_close'] - (df['atr'] * 1.2)
        df['resistance_level'] = df['spx_close'] + (df['atr'] * 1.2)
        
        # Volume analysis
        df['volume_20ma'] = df['spx_volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['spx_volume'] / df['volume_20ma']
        
        # VIX analysis
        df['vix_change'] = df['vix_close'].diff()
        df['vix_percentile'] = df['vix_close'].rolling(60).rank(pct=True) * 100
        
        # SPX-VIX Correlation
        df['vix_returns'] = df['vix_close'].pct_change() * 100
        df['spx_vix_correlation'] = df['daily_return'].rolling(window=5).corr(df['vix_returns'])
        
        # VIX regime classification
        df['vix_regime'] = 'NORMAL'
        df.loc[df['vix_close'] < 17, 'vix_regime'] = 'LOW_VOL'
        df.loc[df['vix_close'] > 21, 'vix_regime'] = 'HIGH_VOL'
        
        # RSI momentum
        df['rsi_momentum'] = df['rsi'].diff().rolling(3).mean()
        
        return df
    
    def generate_live_forecast(self, df: pd.DataFrame) -> dict:
        """Generate live forecast using optimized Council parameters"""
        
        if len(df) == 0:
            return {"error": "No market data available"}
        
        # Get the most recent complete data
        latest_row = df.iloc[-1]
        
        # Check for required data
        required_fields = ['rsi', 'vix_close', 'atr', 'volume_ratio']
        for field in required_fields:
            if pd.isna(latest_row[field]):
                return {"error": f"Missing required data: {field}"}
        
        # Extract variables
        rsi = latest_row['rsi']
        vix = latest_row['vix_close']
        vix_change = latest_row['vix_change'] if pd.notna(latest_row['vix_change']) else 0
        volume_ratio = latest_row['volume_ratio']
        price = latest_row['spx_close']
        support = latest_row['support_level']
        resistance = latest_row['resistance_level']
        regime = latest_row['vix_regime']
        vix_percentile = latest_row['vix_percentile'] if pd.notna(latest_row['vix_percentile']) else 50
        rsi_momentum = latest_row['rsi_momentum'] if pd.notna(latest_row['rsi_momentum']) else 0
        
        # Signal analysis using OPTIMIZED PARAMETERS
        bull_signals = 0
        bear_signals = 0
        chop_signals = 0
        signal_details = []
        
        # BULL SIGNAL ANALYSIS - OPTIMIZED THRESHOLDS
        
        # RSI oversold - EXACT BACKTEST LOGIC
        if rsi < self.optimized_params['rsi_bull_threshold']:  # 25
            bull_signals += 1
            signal_details.append(f"RSI_OVERSOLD({rsi:.1f})")
        
        # Technical breach
        if price < support:
            bull_signals += 1
            signal_details.append("SUPPORT_BREACH")
        
        # VIX fear - EXACT BACKTEST LOGIC  
        if vix > self.optimized_params['vix_fear_threshold'] or vix_change > 2.5:  # 24
            bull_signals += 1
            signal_details.append(f"VIX_FEAR({vix:.1f})")
        
        # VIX percentile fear
        if vix_percentile > 75:
            bull_signals += 1
            signal_details.append(f"VIX_PERCENTILE_HIGH({vix_percentile:.1f})")
        
        # Volume conviction - EXACT BACKTEST LOGIC
        if volume_ratio > self.optimized_params['volume_ratio_threshold']:  # 1.0
            bull_signals += 1
            signal_details.append(f"VOLUME_CONVICTION({volume_ratio:.2f})")
        
        # RSI momentum
        if rsi < 40 and rsi_momentum > 0.5:
            bull_signals += 1
            signal_details.append("RSI_MOMENTUM_UP")
        
        # BEAR SIGNAL ANALYSIS - OPTIMIZED THRESHOLDS
        
        # RSI overbought - EXACT BACKTEST LOGIC
        if rsi > self.optimized_params['rsi_bear_threshold']:  # 75
            bear_signals += 1
            signal_details.append(f"RSI_OVERBOUGHT({rsi:.1f})")
        
        # Technical breach
        if price > resistance:
            bear_signals += 1
            signal_details.append("RESISTANCE_BREACH")
        
        # VIX complacency - EXACT BACKTEST LOGIC
        if vix < self.optimized_params['vix_complacency_threshold'] or vix_change < -1.5:  # 12
            bear_signals += 1
            signal_details.append(f"VIX_COMPLACENCY({vix:.1f})")
        
        # VIX percentile low
        if vix_percentile < 25:
            bear_signals += 1
            signal_details.append(f"VIX_PERCENTILE_LOW({vix_percentile:.1f})")
        
        # Distribution volume - EXACT BACKTEST LOGIC
        if volume_ratio > self.optimized_params['volume_ratio_threshold']:  # 1.0
            bear_signals += 1
            signal_details.append(f"DISTRIBUTION_VOLUME({volume_ratio:.2f})")
        
        # RSI momentum
        if rsi > 60 and rsi_momentum < -0.5:
            bear_signals += 1
            signal_details.append("RSI_MOMENTUM_DOWN")
        
        # CHOP CONDITIONS - EXACT BACKTEST LOGIC
        chop_conditions = [
            35 <= rsi <= 65,
            support * 0.995 <= price <= resistance * 1.005,
            volume_ratio < 1.15,
            abs(vix_change) < 2.0,
            25 <= vix_percentile <= 75
        ]
        chop_signals = sum(chop_conditions)
        
        # COUNCIL DECISION LOGIC - EXACT BACKTEST LOGIC
        forecast_bias = 'Chop'  # Default
        
        if bull_signals >= self.optimized_params['confirmation_required'] and bull_signals > bear_signals:
            forecast_bias = 'Bull'
        elif bear_signals >= self.optimized_params['confirmation_required'] and bear_signals > bull_signals:
            forecast_bias = 'Bear'
        elif chop_signals >= 4:
            forecast_bias = 'Chop'
        
        # Generate forecast report
        forecast_report = {
            'timestamp': datetime.now().isoformat(),
            'date': latest_row['date'],
            'spx_close': float(price),
            'vix_close': float(vix),
            'forecast_bias': forecast_bias,
            'confidence_level': max(bull_signals, bear_signals, chop_signals),
            'bull_signals': int(bull_signals),
            'bear_signals': int(bear_signals), 
            'chop_signals': int(chop_signals),
            'signal_details': '; '.join(signal_details),
            'technical_data': {
                'rsi': float(rsi),
                'vix_regime': regime,
                'volume_ratio': float(volume_ratio),
                'support_level': float(support),
                'resistance_level': float(resistance),
                'vix_percentile': float(vix_percentile)
            },
            'council_version': self.council_version,
            'optimization_baseline': '67.9%'
        }
        
        return forecast_report
    
    def save_forecast_to_database(self, forecast: dict):
        """Save live forecast to Snowflake for audit trail"""
        conn = self.connect_to_snowflake()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO ZEN_MARKET.FORECASTING.LIVE_FORECASTS 
        (TIMESTAMP, DATE, SPX_CLOSE, VIX_CLOSE, FORECAST_BIAS, CONFIDENCE_LEVEL,
         BULL_SIGNALS, BEAR_SIGNALS, CHOP_SIGNALS, SIGNAL_DETAILS, RSI, VIX_REGIME, 
         VOLUME_RATIO, COUNCIL_VERSION)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            forecast['timestamp'],
            forecast['date'],
            float(forecast['spx_close']),
            float(forecast['vix_close']),
            forecast['forecast_bias'],
            int(forecast['confidence_level']),
            int(forecast['bull_signals']),
            int(forecast['bear_signals']),
            int(forecast['chop_signals']),
            forecast['signal_details'],
            float(forecast['technical_data']['rsi']),
            forecast['technical_data']['vix_regime'],
            float(forecast['technical_data']['volume_ratio']),
            forecast['council_version']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Forecast saved to database: {forecast['forecast_bias']}")
    
    def format_forecast_email(self, forecast: dict) -> str:
        """Format forecast for email delivery"""
        
        forecast_date = datetime.fromisoformat(forecast['timestamp']).strftime("%B %d, %Y")
        forecast_time = datetime.fromisoformat(forecast['timestamp']).strftime("%I:%M %p ET")
        
        email_body = f"""
        ZEN COUNCIL MARKET FORECAST
        Generated: {forecast_date} at {forecast_time}
        
        📊 FORECAST: {forecast['forecast_bias'].upper()}
        🎯 Confidence: {forecast['confidence_level']}/6 signals
        
        📈 Market Data:
        SPX Close: ${forecast['spx_close']:,.2f}
        VIX Level: {forecast['vix_close']:.2f} ({forecast['technical_data']['vix_regime']})
        RSI: {forecast['technical_data']['rsi']:.1f}
        Volume Ratio: {forecast['technical_data']['volume_ratio']:.2f}x
        
        🔍 Council Analysis:
        Bull Signals: {forecast['bull_signals']}
        Bear Signals: {forecast['bear_signals']}
        Chop Signals: {forecast['chop_signals']}
        
        📋 Signal Details: {forecast['signal_details']}
        
        🎯 Technical Levels:
        Support: ${forecast['technical_data']['support_level']:,.2f}
        Resistance: ${forecast['technical_data']['resistance_level']:,.2f}
        
        ⚙️ Council Version: {forecast['council_version']}
        📊 Mathematical Baseline: {forecast['optimization_baseline']} accuracy
        
        ---
        Zen Council: 10,000 Chi Masters + 10,000 SPX Options Traders
        Mathematical Framework: Bull/Bear/Chop Multi-Confirmation Logic
        """
        
        return email_body.strip()
    
    def send_forecast_email(self, forecast: dict):
        """Send forecast via email to subscribers using existing .env configuration"""
        
        # Read email configuration from .env file manually
        env_vars = {}
        try:
            with open('.env', 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value.strip('"\'')
        except Exception as e:
            print(f"Error reading .env file for email config: {e}")
            return
        
        # Match your existing .env format - check what variables you actually have
        print("Available .env variables:")
        for key in sorted(env_vars.keys()):
            if 'EMAIL' in key or 'SMTP' in key:
                print(f"  {key}")
        
    def get_email_recipients(self):
        """Get email recipients from Snowflake FORECAST_RECIPIENTS table"""
        try:
            conn = self.connect_to_snowflake()
            cursor = conn.cursor()
            
            # Query FORECAST_RECIPIENTS table with correct column names: EMAIL, NAME, ACTIVE, SUBSCRIBED_TS
            cursor.execute("SELECT EMAIL, NAME FROM ZEN_MARKET.FORECASTING.FORECAST_RECIPIENTS WHERE ACTIVE = TRUE ORDER BY SUBSCRIBED_TS")
            results = cursor.fetchall()
            recipient_list = [row[0] for row in results]
            
            cursor.close()
            conn.close()
            
            print(f"Found {len(recipient_list)} active email recipients in FORECAST_RECIPIENTS table")
            return recipient_list
            
        except Exception as e:
            print(f"Error querying FORECAST_RECIPIENTS table: {e}")
            return []
    
    def send_forecast_email(self, forecast: dict):
        """Send forecast via email to subscribers from database"""
        
        # Read email configuration from .env file manually
        env_vars = {}
        try:
            with open('.env', 'r') as f:
                for line in f:
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value.strip('"\'')
        except Exception as e:
            print(f"Error reading .env file for email config: {e}")
            return
        
        # Use your exact .env variable names
        smtp_server = env_vars.get('SMTP_HOST')
        smtp_port = int(env_vars.get('SMTP_PORT', '587'))
        email_user = env_vars.get('SMTP_USER')
        email_password = env_vars.get('SMTP_PASS')
        
        # Get recipients from database instead of .env
        recipient_list = self.get_email_recipients()
        
        print(f"Email config - Server: {smtp_server}, Port: {smtp_port}")
        print(f"User: {email_user}, Recipients: {len(recipient_list)}")
        
        if not all([smtp_server, email_user, email_password]) or not recipient_list:
            print("Email configuration incomplete - forecast not sent")
            return
        
        print(f"Email config - Server: {smtp_server}, Port: {smtp_port}")
        print(f"User: {email_user}, Recipients: {len(recipient_list)}")
        
        if not all([smtp_server, email_user, email_password]) or not recipient_list:
            print("Email configuration incomplete - forecast not sent")
            return
        
        # Create email
        msg = MIMEMultipart()
        msg['From'] = email_user
        msg['To'] = ', '.join(recipient_list)
        msg['Subject'] = f"Zen Council Forecast: {forecast['forecast_bias']} | {datetime.now().strftime('%m/%d/%Y')}"
        
        # Add body
        email_body = self.format_forecast_email(forecast)
        msg.attach(MIMEText(email_body, 'plain'))
        
        # Send email
        try:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(email_user, email_password)
            text = msg.as_string()
            server.sendmail(email_user, recipient_list, text)
            server.quit()
            print(f"Zen Council forecast emailed to {len(recipient_list)} subscribers")
        except Exception as e:
            print(f"Email delivery failed: {str(e)}")
    
    def run_live_forecast(self) -> dict:
        """Execute live forecasting using optimized Council parameters"""
        
        print("ZEN COUNCIL LIVE FORECASTING SYSTEM")
        print("=" * 60)
        print(f"Council Version: {self.council_version}")
        print(f"Optimized Parameters: RSI {self.optimized_params['rsi_bull_threshold']}/{self.optimized_params['rsi_bear_threshold']}")
        print(f"                     VIX {self.optimized_params['vix_complacency_threshold']}/{self.optimized_params['vix_fear_threshold']}")
        print(f"                     Volume {self.optimized_params['volume_ratio_threshold']}x, {self.optimized_params['confirmation_required']}+ confirmations")
        print(f"Mathematical Baseline: 67.9% accuracy")
        print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")
        print("=" * 60)
        
        # Load recent data
        df = self.load_current_market_data()
        
        if len(df) == 0:
            return {"error": "No market data available"}
        
        # Calculate indicators
        print("Council calculating live indicators...")
        df = self.calculate_live_indicators(df)
        
        # Generate forecast
        print("Council generating live forecast...")
        forecast = self.generate_live_forecast(df)
        
        if 'error' in forecast:
            print(f"Council forecast error: {forecast['error']}")
            return forecast
        
        # Display results
        print("\n" + "=" * 60)
        print("ZEN COUNCIL LIVE FORECAST")
        print("=" * 60)
        print(f"Date: {forecast['date']}")
        print(f"SPX Close: ${forecast['spx_close']:,.2f}")
        print(f"VIX Level: {forecast['vix_close']:.2f} ({forecast['technical_data']['vix_regime']})")
        print(f"")
        print(f"🎯 FORECAST: {forecast['forecast_bias']}")
        print(f"Confidence: {forecast['confidence_level']}/6 signals")
        print(f"")
        print(f"Signal Breakdown:")
        print(f"  Bull Signals: {forecast['bull_signals']}")
        print(f"  Bear Signals: {forecast['bear_signals']}")
        print(f"  Chop Signals: {forecast['chop_signals']}")
        print(f"")
        print(f"Technical Analysis:")
        print(f"  RSI: {forecast['technical_data']['rsi']:.1f}")
        print(f"  Volume Ratio: {forecast['technical_data']['volume_ratio']:.2f}x")
        print(f"  Support: ${forecast['technical_data']['support_level']:,.2f}")
        print(f"  Resistance: ${forecast['technical_data']['resistance_level']:,.2f}")
        print(f"")
        print(f"Council Details: {forecast['signal_details']}")
        print("=" * 60)
        
        # Save to database
        try:
            self.save_forecast_to_database(forecast)
        except Exception as e:
            print(f"Database save failed: {str(e)}")
        
        # Send email
        try:
            self.send_forecast_email(forecast)
        except Exception as e:
            print(f"Email delivery failed: {str(e)}")
        
        return forecast

if __name__ == "__main__":
    print("Assembling Zen Council for live forecasting...")
    forecaster = ZenCouncilLiveForecaster()
    
    forecast_result = forecaster.run_live_forecast()
    
    if 'error' not in forecast_result:
        print(f"\nZen Council live forecast generated successfully!")
        print(f"Ready for scheduled execution: 8:40 AM and 5:00 PM ET")
    else:
        print(f"Live forecasting failed: {forecast_result['error']}")