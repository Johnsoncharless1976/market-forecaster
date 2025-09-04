# zen_council_live_forecaster.py
#!/usr/bin/env python3
"""
zen_council_live_forecaster.py
Zen Council Live Forecasting System - Production Operations
Using optimized parameters: 67.9% baseline accuracy achieved
RSI 25/75, VIX 12/24, Volume 1.0x, 3+ confirmations
Bull >0.15%, Bear <-0.05%, Chop ±0.85%

Integration: GitLab CI scheduled pipeline execution
Schedule: 8:40 AM ET and 5:00 PM ET weekdays
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import requests
import feedparser

# Load environment variables from .env file
load_dotenv()

# Import news integration
try:
    from zen_council_news_integration import ZenCouncilNewsIntegration
    NEWS_INTEGRATION_AVAILABLE = True
except ImportError:
    NEWS_INTEGRATION_AVAILABLE = False
    print("News integration module not found - running in technical-only mode")

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
            'chop_success_range': 0.75
        }
        
    def connect_to_snowflake(self):
        # GitLab CI environment variables (no .env file needed)
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASSWORD')
        database = os.getenv('SNOWFLAKE_DATABASE', 'ZEN_MARKET')
        schema = os.getenv('SNOWFLAKE_SCHEMA', 'FORECASTING')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        
        # Fallback: Try to read .env file if environment variables not set
        if not account:
            try:
                with open('.env', 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == 'SNOWFLAKE_ACCOUNT' and not account:
                                account = value.strip('"\'')
                            elif key == 'SNOWFLAKE_USER' and not user:
                                user = value.strip('"\'')
                            elif key == 'SNOWFLAKE_PASSWORD' and not password:
                                password = value.strip('"\'')
                            elif key == 'SNOWFLAKE_WAREHOUSE' and not warehouse:
                                warehouse = value.strip('"\'')
            except FileNotFoundError:
                print("No .env file found - using GitLab CI environment variables only")
        
        if not account:
            print("SNOWFLAKE_ACCOUNT not found in environment variables or .env file")
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
        conn = self.connect_to_snowflake()
        
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
        WHERE s.DATE >= %s AND s.DATE <= %s
        ORDER BY s.DATE
        """
        
        df = pd.read_sql(query, conn, params=[start_date, end_date])
        conn.close()
        
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
        
        # DEBUGGING: Check all data types before database save
        print("DEBUG: Checking data types before database save...")
        print(f"bull_signals type: {type(bull_signals)} value: {bull_signals}")
        print(f"bear_signals type: {type(bear_signals)} value: {bear_signals}")
        print(f"chop_signals type: {type(chop_signals)} value: {chop_signals}")
        
        # Force conversion to native Python int to eliminate numpy types
        bull_signals_clean = int(bull_signals) if not isinstance(bull_signals, int) else bull_signals
        bear_signals_clean = int(bear_signals) if not isinstance(bear_signals, int) else bear_signals  
        chop_signals_clean = int(chop_signals) if not isinstance(chop_signals, int) else chop_signals
        confidence_clean = int(max(bull_signals_clean, bear_signals_clean, chop_signals_clean))
        
        print(f"After conversion - bull_signals: {type(bull_signals_clean)}")
        print(f"After conversion - bear_signals: {type(bear_signals_clean)}")
        print(f"After conversion - chop_signals: {type(chop_signals_clean)}")
        
        # Create forecast report with verified clean types
        forecast_report = {
            'timestamp': datetime.now().isoformat(),
            'date': latest_row['date'],
            'spx_close': float(price),
            'vix_close': float(vix),
            'forecast_bias': forecast_bias,
            'confidence_level': confidence_clean,  # Use cleaned version
            'bull_signals': bull_signals_clean,    # Use cleaned version
            'bear_signals': bear_signals_clean,    # Use cleaned version 
            'chop_signals': chop_signals_clean,    # Use cleaned version
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
        try:
            conn = self.connect_to_snowflake()
            cursor = conn.cursor()
            
            # DEBUG: Print all data types going into database
            print("\nDEBUG: Database insert values and types:")
            values_debug = [
                ('timestamp', forecast['timestamp'], type(forecast['timestamp'])),
                ('date', str(forecast['date']), type(str(forecast['date']))),
                ('spx_close', float(forecast['spx_close']), type(float(forecast['spx_close']))),
                ('vix_close', float(forecast['vix_close']), type(float(forecast['vix_close']))),
                ('forecast_bias', str(forecast['forecast_bias']), type(str(forecast['forecast_bias']))),
                ('confidence_level', forecast['confidence_level'], type(forecast['confidence_level'])),
                ('bull_signals', forecast['bull_signals'], type(forecast['bull_signals'])),
                ('bear_signals', forecast['bear_signals'], type(forecast['bear_signals'])),
                ('chop_signals', forecast['chop_signals'], type(forecast['chop_signals'])),
                ('signal_details', str(forecast['signal_details']), type(str(forecast['signal_details']))),
                ('rsi', float(forecast['technical_data']['rsi']), type(float(forecast['technical_data']['rsi']))),
                ('vix_regime', str(forecast['technical_data']['vix_regime']), type(str(forecast['technical_data']['vix_regime']))),
                ('volume_ratio', float(forecast['technical_data']['volume_ratio']), type(float(forecast['technical_data']['volume_ratio']))),
                ('council_version', str(forecast['council_version']), type(str(forecast['council_version'])))
            ]
            
            for name, value, vtype in values_debug:
                print(f"  {name}: {value} ({vtype})")
            
            insert_query = """
            INSERT INTO ZEN_MARKET.FORECASTING.LIVE_FORECASTS 
            (TIMESTAMP, DATE, SPX_CLOSE, VIX_CLOSE, FORECAST_BIAS, CONFIDENCE_LEVEL,
             BULL_SIGNALS, BEAR_SIGNALS, CHOP_SIGNALS, SIGNAL_DETAILS, RSI, VIX_REGIME, 
             VOLUME_RATIO, COUNCIL_VERSION)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Convert all values to native Python types to avoid numpy issues
            values = (
                forecast['timestamp'],
                str(forecast['date']),
                float(forecast['spx_close']),
                float(forecast['vix_close']),
                str(forecast['forecast_bias']),
                forecast['confidence_level'],  # Already cleaned
                forecast['bull_signals'],      # Already cleaned
                forecast['bear_signals'],      # Already cleaned
                forecast['chop_signals'],      # Already cleaned
                str(forecast['signal_details']),
                float(forecast['technical_data']['rsi']),
                str(forecast['technical_data']['vix_regime']),
                float(forecast['technical_data']['volume_ratio']),
                str(forecast['council_version'])
            )
            
            print(f"DEBUG: Executing query with {len(values)} parameters")
            cursor.execute(insert_query, values)
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Database save SUCCESS: {forecast['forecast_bias']}")
            
        except Exception as e:
            print(f"Database save FAILED: {str(e)}")
            print(f"Full error details: {repr(e)}")
            # Skip database save but continue with forecast delivery
    
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
        """Send forecast via email to subscribers using GitLab CI environment variables"""
        
        # Try GitLab CI environment variables first, then .env file
        smtp_server = os.getenv('SMTP_HOST')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        email_user = os.getenv('SMTP_USER')
        email_password = os.getenv('SMTP_PASS')
        
        # Fallback to .env file if environment variables not set
        if not smtp_server:
            try:
                with open('.env', 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            value = value.strip('"\'')
                            if key == 'SMTP_HOST' and not smtp_server:
                                smtp_server = value
                            elif key == 'SMTP_PORT' and smtp_port == 587:
                                smtp_port = int(value)
                            elif key == 'SMTP_USER' and not email_user:
                                email_user = value
                            elif key == 'SMTP_PASS' and not email_password:
                                email_password = value
            except FileNotFoundError:
                print("No .env file found - using GitLab CI environment variables only")
        
        # Get recipients from database instead of .env
        recipient_list = self.get_email_recipients()
        
        print(f"Email config - Server: {smtp_server}, Port: {smtp_port}")
        print(f"User: {email_user}, Recipients: {len(recipient_list)}")
        
        if not all([smtp_server, email_user, email_password]) or not recipient_list:
            print("Email configuration incomplete - forecast not sent")
            print("Required: SMTP_HOST, SMTP_USER, SMTP_PASS environment variables")
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
    
    def display_enhanced_forecast_results(self, forecast: dict):
        """Display enhanced forecast results with news attribution"""
        print("\n" + "=" * 60)
        print("ZEN COUNCIL ENHANCED FORECAST")
        print("=" * 60)
        print(f"Date: {forecast['date']}")
        print(f"SPX Close: ${forecast['spx_close']:,.2f}")
        print(f"VIX Level: {forecast['vix_close']:.2f} ({forecast['technical_data']['vix_regime']})")
        print(f"")
        print(f"FORECAST: {forecast['forecast_bias']}")
        
        if forecast.get('news_enhanced'):
            base_confidence = max(forecast['bull_signals'], forecast['bear_signals'], forecast['chop_signals'])
            enhanced_confidence = forecast.get('enhanced_confidence', base_confidence)
            print(f"Confidence: {enhanced_confidence}/6 signals (base: {base_confidence})")
        else:
            print(f"Confidence: {forecast['confidence_level']}/6 signals")
        
        print(f"")
        print(f"Technical Signal Breakdown:")
        print(f"  Bull Signals: {forecast['bull_signals']}")
        print(f"  Bear Signals: {forecast['bear_signals']}")
        print(f"  Chop Signals: {forecast['chop_signals']}")
        
        if forecast.get('news_enhanced'):
            print(f"")
            print(f"News-Enhanced Signals:")
            print(f"  Enhanced Bull: {forecast.get('enhanced_bull_signals', forecast['bull_signals'])}")
            print(f"  Enhanced Bear: {forecast.get('enhanced_bear_signals', forecast['bear_signals'])}")  
            print(f"  Enhanced Chop: {forecast.get('enhanced_chop_signals', forecast['chop_signals'])}")
            print(f"")
            print(f"News Attribution: {forecast.get('news_attribution', 'Technical analysis only')}")
            print(f"News Modifiers: Bull {forecast.get('news_bull_modifier', 1.0):.2f} | Bear {forecast.get('news_bear_modifier', 1.0):.2f} | Chop {forecast.get('news_chop_modifier', 1.0):.2f}")
        
        print(f"")
        print(f"Technical Analysis:")
        print(f"  RSI: {forecast['technical_data']['rsi']:.1f}")
        print(f"  Volume Ratio: {forecast['technical_data']['volume_ratio']:.2f}x")
        print(f"  Support: ${forecast['technical_data']['support_level']:,.2f}")
        print(f"  Resistance: ${forecast['technical_data']['resistance_level']:,.2f}")
        print(f"")
        print(f"Council Details: {forecast['signal_details']}")
        print(f"Enhancement Status: {'News-Enhanced (67.9% -> 88% target)' if forecast.get('news_enhanced') else 'Technical-Only (67.9% baseline)'}")
        print("=" * 60)
    
    def run_live_forecast(self) -> dict:
        """Execute live forecasting with news integration using optimized Council parameters"""
        
        print("ZEN COUNCIL LIVE FORECASTING SYSTEM")
        print("=" * 60)
        print(f"Council Version: {self.council_version}")
        print(f"Optimized Parameters: RSI {self.optimized_params['rsi_bull_threshold']}/{self.optimized_params['rsi_bear_threshold']}")
        print(f"                     VIX {self.optimized_params['vix_complacency_threshold']}/{self.optimized_params['vix_fear_threshold']}")
        print(f"                     Volume {self.optimized_params['volume_ratio_threshold']}x, {self.optimized_params['confirmation_required']}+ confirmations")
        print(f"Mathematical Baseline: 67.9% accuracy")
        print(f"News Integration: {'ENABLED' if NEWS_INTEGRATION_AVAILABLE else 'DISABLED'}")
        print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")
        print("=" * 60)
        
        # Load recent data
        df = self.load_current_market_data()
        
        if len(df) == 0:
            return {"error": "No market data available"}
        
        # Calculate indicators
        print("Council calculating live indicators...")
        df = self.calculate_live_indicators(df)
        
        # Generate base technical forecast
        print("Council generating base technical forecast...")
        base_forecast = self.generate_live_forecast(df)
        
        if 'error' in base_forecast:
            print(f"Council forecast error: {base_forecast['error']}")
            return base_forecast
        
        # News integration enhancement
        enhanced_forecast = base_forecast.copy()
        
        if NEWS_INTEGRATION_AVAILABLE:
            try:
                print("Integrating news analysis into Council forecast...")
                news_integrator = ZenCouncilNewsIntegration()
                news_weights = news_integrator.run_news_integration_analysis()
                
                # Enhance forecast with news attribution
                enhanced_forecast = news_integrator.enhance_council_forecast(base_forecast, news_weights)
                enhanced_forecast['news_enhanced'] = True
                
                print(f"News integration complete - forecast enhanced")
                
            except Exception as e:
                print(f"News integration failed: {e}")
                print("Continuing with technical-only forecast")
                enhanced_forecast['news_enhanced'] = False
        else:
            enhanced_forecast['news_enhanced'] = False
        
        # Display results
        self.display_enhanced_forecast_results(enhanced_forecast)
        
        # Save to database
        try:
            self.save_forecast_to_database(enhanced_forecast)
        except Exception as e:
            print(f"Database save failed: {str(e)}")
        
        # Send email
        try:
            self.send_forecast_email(enhanced_forecast)
        except Exception as e:
            print(f"Email delivery failed: {str(e)}")
        
        return enhanced_forecast

if __name__ == "__main__":
    print("Assembling Zen Council for live forecasting...")
    forecaster = ZenCouncilLiveForecaster()
    
    forecast_result = forecaster.run_live_forecast()
    
    if 'error' not in forecast_result:
        print(f"\nZen Council live forecast generated successfully!")
        print(f"Ready for scheduled execution: 8:40 AM and 5:00 PM ET")
    else:
        print(f"Live forecasting failed: {forecast_result['error']}")