# historical_backtest_generator.py
#!/usr/bin/env python3
"""
Historical Backtesting System
Generate learning data by running your forecasting system on historical data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
import os
import json

class HistoricalBacktester:
    def __init__(self):
        self.backtest_results = []
        self.news_attribution_data = []
        
    def connect_to_snowflake(self):
        return snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
    
    def load_historical_market_data(self, start_date: str = '2020-01-01') -> pd.DataFrame:
        """
        Load your 5 years of historical SPX, VIX, VVIX data
        Assumes you have this data in Snowflake or can import it
        """
        conn = self.connect_to_snowflake()
        
        # Check what historical data tables you actually have
        try:
            query = """
            SELECT 
                DATE,
                SPY_CLOSE as spx_close,
                ES_CLOSE as es_close,
                VIX_CLOSE as vix_close,
                VVIX_CLOSE as vvix_close
            FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL
            WHERE DATE >= %s
            ORDER BY DATE
            """
            df = pd.read_sql(query, conn, params=[start_date])
            
        except Exception as e:
            # Fallback: check available historical tables
            print(f"Primary query failed: {e}")
            print("Checking available historical tables...")
            
            tables_query = "SHOW TABLES LIKE '%HISTORICAL%'"
            tables_df = pd.read_sql(tables_query, conn)
            print("Available historical tables:")
            print(tables_df[['name']].to_string())
            
            # Try alternative table structure
            try:
                alt_query = """
                SELECT 
                    DATE,
                    CLOSE as spx_close,
                    NULL as es_close,
                    NULL as vix_close,
                    NULL as vvix_close
                FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL
                WHERE DATE >= %s
                ORDER BY DATE
                LIMIT 100
                """
                df = pd.read_sql(alt_query, conn, params=[start_date])
            except Exception as e2:
                print(f"Alternative query also failed: {e2}")
                conn.close()
                return pd.DataFrame()
        
        conn.close()
        return df
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply your technical analysis to historical data using actual OHLC data
        """
        df = df.copy()
        
        # Calculate daily returns
        df['daily_return'] = df['spx_close'].pct_change() * 100
        
        # Simple RSI calculation (14-day) 
        delta = df['spx_close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # ATR calculation using actual High/Low data
        df['prev_close'] = df['spx_close'].shift(1)
        
        # Use actual high/low if available, otherwise estimate
        if 'spx_high' in df.columns and 'spx_low' in df.columns:
            df['tr'] = np.maximum(
                df['spx_high'] - df['spx_low'],
                np.maximum(
                    abs(df['spx_high'] - df['prev_close']),
                    abs(df['spx_low'] - df['prev_close'])
                )
            )
        else:
            # Fallback: estimate true range from close prices
            df['tr'] = abs(df['spx_close'] - df['prev_close'])
            
        df['atr'] = df['tr'].rolling(window=14).mean()
        
        # Support/Resistance levels based on ATR
        df['support_1'] = df['spx_close'] - (df['atr'] * 1.5)
        df['support_2'] = df['spx_close'] - (df['atr'] * 2.5) 
        df['resistance_1'] = df['spx_close'] + (df['atr'] * 1.5)
        df['resistance_2'] = df['spx_close'] + (df['atr'] * 2.5)
        
        return df
    
    def generate_forecast_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate forecast bias using your systematic approach
        """
        df = df.copy()
        
        conditions = []
        # Bullish conditions
        bullish_condition = (
            (df['rsi'] < 35) |  # Oversold
            (df['daily_return'] < -1.5)  # Significant down day
        )
        
        # Bearish conditions  
        bearish_condition = (
            (df['rsi'] > 65) |  # Overbought
            (df['daily_return'] > 1.5)   # Significant up day
        )
        
        # Assign bias
        df['forecast_bias'] = 'Neutral'
        df.loc[bullish_condition, 'forecast_bias'] = 'Bullish'
        df.loc[bearish_condition, 'forecast_bias'] = 'Bearish'
        
        return df
    
    def validate_forecast_accuracy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check forecast accuracy against next day's actual performance
        """
        df = df.copy()
        
        # Get next day's return
        df['next_day_return'] = df['daily_return'].shift(-1)
        df['next_day_close'] = df['spx_close'].shift(-1)
        
        # Determine if forecast was correct
        df['forecast_hit'] = False
        
        # Bullish forecast hit if next day positive
        bullish_hits = (df['forecast_bias'] == 'Bullish') & (df['next_day_return'] > 0)
        
        # Bearish forecast hit if next day negative  
        bearish_hits = (df['forecast_bias'] == 'Bearish') & (df['next_day_return'] < 0)
        
        # Neutral forecast hit if next day within +/-0.5%
        neutral_hits = (df['forecast_bias'] == 'Neutral') & (abs(df['next_day_return']) <= 0.5)
        
        df['forecast_hit'] = bullish_hits | bearish_hits | neutral_hits
        
        # Check support/resistance breaches
        df['resistance_breach'] = df['next_day_close'] > df['resistance_1']
        df['support_breach'] = df['next_day_close'] < df['support_1']
        df['level_breach'] = df['resistance_breach'] | df['support_breach']
        
        return df
    
    def run_historical_backtest(self, start_date: str = '2023-01-01', end_date: str = '2025-08-31') -> pd.DataFrame:
        """
        Run complete backtest to generate learning data
        """
        print(f"Starting Historical Backtest: {start_date} to {end_date}")
        print("=" * 60)
        
        # Load historical market data
        print("Loading historical market data...")
        df = self.load_historical_market_data(start_date)
        
        if len(df) == 0:
            print("❌ No historical data found")
            return pd.DataFrame()
        
        print(f"✅ Loaded {len(df)} days of market data")
        
        # Apply technical analysis
        print("Calculating technical indicators...")
        df = self.calculate_technical_indicators(df)
        
        # Generate forecasts
        print("Generating historical forecasts...")
        df = self.generate_forecast_bias(df)
        
        # Validate accuracy
        print("Validating forecast accuracy...")
        df = self.validate_forecast_accuracy(df)
        
        # Filter out incomplete records
        complete_df = df.dropna(subset=['next_day_return', 'forecast_hit']).copy()
        
        if len(complete_df) == 0:
            print("❌ No complete forecast records generated")
            return df
        
        # Calculate performance metrics
        accuracy = complete_df['forecast_hit'].mean()
        total_forecasts = len(complete_df)
        
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        print(f"Total Forecasts: {total_forecasts}")
        print(f"Overall Accuracy: {accuracy:.1%}")
        
        # Accuracy by bias type
        bias_performance = complete_df.groupby('forecast_bias')['forecast_hit'].agg(['count', 'mean'])
        print(f"\nAccuracy by Bias:")
        for bias in bias_performance.index:
            count = bias_performance.loc[bias, 'count']
            acc = bias_performance.loc[bias, 'mean']
            print(f"  {bias}: {acc:.1%} ({count} forecasts)")
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"historical_backtest_results_{timestamp}.csv"
        complete_df.to_csv(output_file, index=False)
        print(f"\n📄 Results saved: {output_file}")
        
        return complete_df
    
    def generate_learning_corpus(self, backtest_df: pd.DataFrame):
        """
        Convert backtest results into format for adaptive learning system
        """
        if len(backtest_df) == 0:
            return
        
        learning_records = []
        
        for _, row in backtest_df.iterrows():
            record = {
                'DATE': row['DATE'],
                'INDEX': 'SPX',
                'FORECAST_BIAS': row['forecast_bias'],
                'ACTUAL_CLOSE': row['next_day_close'],
                'HIT': row['forecast_hit'],
                'PRICE_CHANGE_PCT': row['next_day_return'],
                'LEVEL_BREACH': row['level_breach'],
                'RESISTANCE_BREACH': row['resistance_breach'],
                'SUPPORT_BREACH': row['support_breach'],
                'SUPPORTS': f"{row['support_1']:.2f}, {row['support_2']:.2f}",
                'RESISTANCES': f"{row['resistance_1']:.2f}, {row['resistance_2']:.2f}",
                'RSI': row['rsi'],
                'ATR': row['atr']
            }
            learning_records.append(record)
        
        # Save learning corpus
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        corpus_file = f"learning_corpus_{timestamp}.json"
        
        with open(corpus_file, 'w') as f:
            json.dump(learning_records, f, indent=2, default=str)
        
        print(f"\n🧠 Learning corpus generated: {corpus_file}")
        print(f"   Records: {len(learning_records)}")
        print("   Ready for adaptive learning system")

# Example usage
if __name__ == "__main__":
    backtester = HistoricalBacktester()
    
    # Run backtest on recent period with realistic dates
    results = backtester.run_historical_backtest(
        start_date='2024-01-01',
        end_date='2025-08-22'  # Match your actual data end date
    )
    
    if len(results) > 0:
        # Generate learning corpus
        backtester.generate_learning_corpus(results)
        print(f"\n✅ Generated {len(results)} historical forecasts for learning")
    else:
        print("\n❌ Backtest failed - check your historical data tables")