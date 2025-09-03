# fixed_historical_backtest.py
#!/usr/bin/env python3
"""
Fixed Historical Backtesting System - Working Version
"""

import pandas as pd
import numpy as np
from datetime import datetime
import snowflake.connector
import os
import json

class FixedHistoricalBacktester:
    def __init__(self):
        self.backtest_results = []
        
    def connect_to_snowflake(self):
        return snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
    
    def load_historical_data(self, start_date: str = '2024-01-01') -> pd.DataFrame:
        """Load SPX historical data with actual column names"""
        conn = self.connect_to_snowflake()
        
        query = """
        SELECT 
            DATE,
            OPEN,
            HIGH, 
            LOW,
            CLOSE,
            VOLUME
        FROM ZEN_MARKET.FORECASTING.SPX_HISTORICAL
        WHERE DATE >= %s
        ORDER BY DATE
        """
        
        df = pd.read_sql(query, conn, params=[start_date])
        conn.close()
        
        print(f"Loaded {len(df)} SPX records from {df['DATE'].min() if len(df) > 0 else 'N/A'}")
        return df
    
    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators using actual CLOSE column"""
        df = df.copy()
        
        # Daily returns
        df['daily_return'] = df['CLOSE'].pct_change() * 100
        
        # RSI calculation (14-day)
        delta = df['CLOSE'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # ATR using actual OHLC data
        df['prev_close'] = df['CLOSE'].shift(1)
        df['tr'] = np.maximum(
            df['HIGH'] - df['LOW'],
            np.maximum(
                abs(df['HIGH'] - df['prev_close']),
                abs(df['LOW'] - df['prev_close'])
            )
        )
        df['atr'] = df['tr'].rolling(window=14).mean()
        
        # Support/Resistance levels
        df['support_1'] = df['CLOSE'] - (df['atr'] * 1.5)
        df['support_2'] = df['CLOSE'] - (df['atr'] * 2.5)
        df['resistance_1'] = df['CLOSE'] + (df['atr'] * 1.5)
        df['resistance_2'] = df['CLOSE'] + (df['atr'] * 2.5)
        
        return df
    
    def generate_forecasts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate forecast bias using systematic approach"""
        df = df.copy()
        
        # Initialize bias as Neutral
        df['forecast_bias'] = 'Neutral'
        
        # Bullish conditions
        bullish_condition = (
            (df['rsi'] < 35) |  # Oversold
            (df['daily_return'] < -1.5)  # Big down day
        )
        
        # Bearish conditions  
        bearish_condition = (
            (df['rsi'] > 65) |  # Overbought
            (df['daily_return'] > 1.5)   # Big up day
        )
        
        df.loc[bullish_condition, 'forecast_bias'] = 'Bullish'
        df.loc[bearish_condition, 'forecast_bias'] = 'Bearish'
        
        return df
    
    def validate_accuracy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check forecast accuracy against next day's performance"""
        df = df.copy()
        
        # Get next day's data
        df['next_day_return'] = df['daily_return'].shift(-1)
        df['next_day_close'] = df['CLOSE'].shift(-1)
        
        # Validate forecasts
        df['forecast_hit'] = False
        
        # Bullish hits: next day positive
        bullish_hits = (df['forecast_bias'] == 'Bullish') & (df['next_day_return'] > 0)
        
        # Bearish hits: next day negative
        bearish_hits = (df['forecast_bias'] == 'Bearish') & (df['next_day_return'] < 0)
        
        # Neutral hits: next day within +/-0.5%
        neutral_hits = (df['forecast_bias'] == 'Neutral') & (abs(df['next_day_return']) <= 0.5)
        
        df['forecast_hit'] = bullish_hits | bearish_hits | neutral_hits
        
        # Check level breaches
        df['resistance_breach'] = df['next_day_close'] > df['resistance_1']
        df['support_breach'] = df['next_day_close'] < df['support_1']
        df['level_breach'] = df['resistance_breach'] | df['support_breach']
        
        return df
    
    def run_backtest(self, start_date: str = '2024-01-01', end_date: str = '2025-08-22') -> pd.DataFrame:
        """Run complete historical backtest"""
        
        print(f"Starting Fixed Historical Backtest: {start_date} to {end_date}")
        print("=" * 60)
        
        # Load data
        df = self.load_historical_data(start_date)
        if len(df) == 0:
            print("No historical data loaded")
            return pd.DataFrame()
        
        # Filter by end date if needed
        df = df[df['DATE'] <= pd.to_datetime(end_date).date()]
        print(f"Date range: {df['DATE'].min()} to {df['DATE'].max()}")
        
        # Add technical indicators
        print("Calculating technical indicators...")
        df = self.add_technical_indicators(df)
        
        # Generate forecasts
        print("Generating forecasts...")
        df = self.generate_forecasts(df)
        
        # Validate accuracy
        print("Validating accuracy...")
        df = self.validate_accuracy(df)
        
        # Remove incomplete records
        complete_df = df.dropna(subset=['next_day_return', 'forecast_hit']).copy()
        
        if len(complete_df) == 0:
            print("No complete records for analysis")
            return df
        
        # Calculate performance
        total_forecasts = len(complete_df)
        hits = complete_df['forecast_hit'].sum()
        accuracy = hits / total_forecasts * 100
        
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        print(f"Total Forecasts: {total_forecasts}")
        print(f"Hits: {hits}")
        print(f"Accuracy: {accuracy:.1f}%")
        
        # Performance by bias
        bias_performance = complete_df.groupby('forecast_bias')['forecast_hit'].agg(['count', 'sum', 'mean'])
        bias_performance.columns = ['Total', 'Hits', 'Accuracy']
        bias_performance['Accuracy'] = bias_performance['Accuracy'] * 100
        
        print(f"\nPerformance by Bias:")
        print(bias_performance.round(1))
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"backtest_results_{timestamp}.csv"
        complete_df.to_csv(output_file, index=False)
        print(f"\nResults saved: {output_file}")
        
        # Generate learning corpus
        self.create_learning_corpus(complete_df)
        
        return complete_df
    
    def create_learning_corpus(self, df: pd.DataFrame):
        """Create learning corpus for adaptive system"""
        learning_records = []
        
        for _, row in df.iterrows():
            if pd.isna(row['next_day_close']) or pd.isna(row['forecast_hit']):
                continue
                
            record = {
                'DATE': str(row['DATE']),
                'INDEX': 'SPX',
                'FORECAST_BIAS': row['forecast_bias'],
                'ACTUAL_CLOSE': float(row['next_day_close']),
                'HIT': bool(row['forecast_hit']),
                'PRICE_CHANGE_PCT': float(row['next_day_return']),
                'LEVEL_BREACH': bool(row['level_breach']),
                'RSI': float(row['rsi']) if pd.notna(row['rsi']) else None,
                'ATR': float(row['atr']) if pd.notna(row['atr']) else None,
                'SUPPORTS': f"{row['support_1']:.2f}, {row['support_2']:.2f}",
                'RESISTANCES': f"{row['resistance_1']:.2f}, {row['resistance_2']:.2f}"
            }
            learning_records.append(record)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        corpus_file = f"learning_corpus_{timestamp}.json"
        
        with open(corpus_file, 'w') as f:
            json.dump(learning_records, f, indent=2, default=str)
        
        print(f"Learning corpus saved: {corpus_file}")
        print(f"Records for adaptive learning: {len(learning_records)}")

if __name__ == "__main__":
    backtester = FixedHistoricalBacktester()
    results = backtester.run_backtest(
        start_date='2024-01-01',
        end_date='2025-08-22'
    )
    
    if len(results) > 0:
        print(f"\nBacktest complete! Generated learning data for adaptive system.")
    else:
        print("\nBacktest failed.")