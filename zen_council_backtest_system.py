# zen_council_backtest_system.py
#!/usr/bin/env python3
"""
Zen Council Backtesting System - Mathematical Framework
Implementing 10,000 Council Members + Navy Top Gun + Super Computers + Mathematicians
Target: 70-72% baseline accuracy using Bull/Bear/Chop with multi-confirmation logic
"""

import pandas as pd
import numpy as np
from datetime import datetime
import snowflake.connector
import os
import json

class ZenCouncilBacktester:
    def __init__(self):
        self.council_results = []
        self.regime_performance = {}
        
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
        """Load SPX and VIX data for Council analysis"""
        conn = self.connect_to_snowflake()
        
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
        WHERE s.DATE >= %s
        ORDER BY s.DATE
        """
        
        df = pd.read_sql(query, conn, params=[start_date])
        conn.close()
        
        # Debug: Check actual columns returned from Snowflake
        print(f"Council assembled with {len(df)} market sessions")
        print(f"Original columns: {list(df.columns)}")
        
        # Convert column names to lowercase to handle Snowflake case sensitivity
        df.columns = df.columns.str.lower()
        print(f"Standardized columns: {list(df.columns)}")
        
        return df
    
    def calculate_council_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate indicators with Council mathematical precision"""
        df = df.copy()
        
        # Basic calculations (10,000 Super Computers)
        df['daily_return'] = df['spx_close'].pct_change() * 100
        df['prev_close'] = df['spx_close'].shift(1)
        
        # RSI (14-day) - Options Traders specification
        delta = df['spx_close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # ATR (14-day) - Risk Scientists calculation
        df['tr'] = np.maximum(
            df['spx_high'] - df['spx_low'],
            np.maximum(
                abs(df['spx_high'] - df['prev_close']),
                abs(df['spx_low'] - df['prev_close'])
            )
        )
        df['atr'] = df['tr'].rolling(window=14).mean()
        
        # Support/Resistance levels - Chi Masters flow
        df['support_level'] = df['spx_close'] - (df['atr'] * 1.5)
        df['resistance_level'] = df['spx_close'] + (df['atr'] * 1.5)
        
        # Volume analysis - Behavioral Finance
        df['volume_20ma'] = df['spx_volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['spx_volume'] / df['volume_20ma']
        
        # VIX analysis - Quantitative Risk Scientists
        df['vix_change'] = df['vix_close'].diff()
        df['vix_5ma'] = df['vix_close'].rolling(window=5).mean()
        
        # SPX-VIX Correlation - 10,000 Mathematicians
        df['vix_returns'] = df['vix_close'].pct_change() * 100
        df['spx_vix_correlation'] = df['daily_return'].rolling(window=5).corr(df['vix_returns'])
        
        # Market Regime Classification (Council Consensus)
        df['vix_regime'] = 'NORMAL'
        df.loc[df['vix_close'] < 16, 'vix_regime'] = 'LOW_VOL'
        df.loc[df['vix_close'] > 22, 'vix_regime'] = 'HIGH_VOL'
        
        # Regime breakdown detection
        df['regime_breakdown'] = df['spx_vix_correlation'] > -0.3
        
        return df
    
    def zen_council_forecast_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Implement Council-approved Bull/Bear/Chop logic"""
        df = df.copy()
        
        # Initialize all as Chop (Conservative Council approach)
        df['forecast_bias'] = 'Chop'
        df['bull_signals'] = 0
        df['bear_signals'] = 0
        df['chop_signals'] = 0
        
        for idx in range(len(df)):
            row = df.iloc[idx]
            
            # Skip if insufficient data
            if pd.isna(row['rsi']) or pd.isna(row['vix_close']) or pd.isna(row['atr']):
                continue
            
            # Reset signal counters
            bull_signals = 0
            bear_signals = 0
            chop_signals = 0
            
            # Extract key variables
            rsi = row['rsi']
            vix = row['vix_close']
            vix_change = row['vix_change'] if pd.notna(row['vix_change']) else 0
            volume_ratio = row['volume_ratio'] if pd.notna(row['volume_ratio']) else 1.0
            price = row['spx_close']
            support = row['support_level']
            resistance = row['resistance_level']
            regime = row['vix_regime']
            
            # CHOP CONDITIONS (Chi Masters + Behavioral Finance)
            chop_conditions = [
                40 <= rsi <= 60,                    # No momentum extreme
                support <= price <= resistance,      # Within ATR bands
                volume_ratio < 1.1,                 # No conviction volume
                abs(vix_change) < 1.5               # No fear shift
            ]
            chop_signals = sum(chop_conditions)
            
            # BULL SIGNAL ANALYSIS (Multi-Council Consensus)
            bull_confirmations = []
            
            # RSI oversold (Options Traders)
            if rsi < 32:
                bull_confirmations.append("RSI_OVERSOLD")
                bull_signals += 1
            
            # Technical breach (Chi Masters)
            if price < support:
                bull_confirmations.append("SUPPORT_BREACH")
                bull_signals += 1
            
            # Fear present (Risk Scientists)
            if vix > 20 or vix_change > 2:
                bull_confirmations.append("VIX_FEAR")
                bull_signals += 1
            
            # Volume conviction (Traders)
            if volume_ratio > 1.3:
                bull_confirmations.append("VOLUME_CONVICTION")
                bull_signals += 1
            
            # Additional confirmation for regime
            if regime == 'HIGH_VOL' and rsi < 30:
                bull_confirmations.append("HIGH_VOL_EXTREME")
                bull_signals += 1
            
            # BEAR SIGNAL ANALYSIS (Multi-Council Consensus)
            bear_confirmations = []
            
            # RSI overbought (Options Traders)
            if rsi > 68:
                bear_confirmations.append("RSI_OVERBOUGHT")
                bear_signals += 1
            
            # Technical breach (Chi Masters)
            if price > resistance:
                bear_confirmations.append("RESISTANCE_BREACH")
                bear_signals += 1
            
            # Complacency (Risk Scientists)
            if vix < 15 or vix_change < -2:
                bear_confirmations.append("VIX_COMPLACENCY")
                bear_signals += 1
            
            # Distribution volume (Traders)
            if volume_ratio > 1.3:
                bear_confirmations.append("DISTRIBUTION_VOLUME")
                bear_signals += 1
            
            # Additional confirmation for regime
            if regime == 'LOW_VOL' and rsi > 65:
                bear_confirmations.append("LOW_VOL_EXTREME")
                bear_signals += 1
            
            # COUNCIL DECISION LOGIC (Navy Top Gun Precision)
            
            # Require 3+ confirmations for directional call
            if bull_signals >= 3 and bull_signals > bear_signals:
                df.iloc[idx, df.columns.get_loc('forecast_bias')] = 'Bull'
            elif bear_signals >= 3 and bear_signals > bull_signals:
                df.iloc[idx, df.columns.get_loc('forecast_bias')] = 'Bear'
            elif chop_signals >= 3:
                df.iloc[idx, df.columns.get_loc('forecast_bias')] = 'Chop'
            # Otherwise stay Chop (Conservative default)
            
            # Store signal counts for analysis
            df.iloc[idx, df.columns.get_loc('bull_signals')] = bull_signals
            df.iloc[idx, df.columns.get_loc('bear_signals')] = bear_signals
            df.iloc[idx, df.columns.get_loc('chop_signals')] = chop_signals
        
        return df
    
    def validate_council_accuracy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate forecasts with Council precision standards"""
        df = df.copy()
        
        # Get next day data
        df['next_day_return'] = df['daily_return'].shift(-1)
        df['next_day_close'] = df['spx_close'].shift(-1)
        
        # Council accuracy validation (Top Gun precision)
        df['forecast_hit'] = False
        
        for idx in range(len(df) - 1):
            forecast = df.iloc[idx]['forecast_bias']
            next_return = df.iloc[idx]['next_day_return']
            
            if pd.isna(next_return):
                continue
            
            # Council-approved accuracy thresholds
            if forecast == 'Bull':
                # Bull hits if next day > +0.25% (meaningful bullish move)
                df.iloc[idx, df.columns.get_loc('forecast_hit')] = next_return > 0.25
                
            elif forecast == 'Bear':
                # Bear hits if next day < -0.25% (meaningful bearish move)
                df.iloc[idx, df.columns.get_loc('forecast_hit')] = next_return < -0.25
                
            elif forecast == 'Chop':
                # Chop hits if next day within ±0.75% (range-bound)
                df.iloc[idx, df.columns.get_loc('forecast_hit')] = abs(next_return) <= 0.75
        
        return df
    
    def run_zen_council_backtest(self, start_date: str = '2024-01-01', end_date: str = '2025-08-22') -> pd.DataFrame:
        """Execute Zen Council backtesting system"""
        
        print("ZEN COUNCIL BACKTESTING SYSTEM")
        print("=" * 60)
        print("Council Assembly: 10,000 Chi Masters + 10,000 SPX Traders")
        print("                  10,000 Journalists + Behavioral Analysts")
        print("                  Risk Scientists + Macro Strategists")  
        print("                  10,000 Navy Top Gun + 10,000 Mathematicians")
        print("                  10,000 Super Computers")
        print(f"Target: 70-72% baseline accuracy")
        print(f"Period: {start_date} to {end_date}")
        print("=" * 60)
        
        # Load data
        df = self.load_historical_data(start_date)
        if len(df) == 0:
            return pd.DataFrame()
        
        # Filter by end date
        df = df[df['date'] <= pd.to_datetime(end_date).date()]
        print(f"Council analyzing {len(df)} trading sessions")
        
        # Calculate Council indicators
        print("Council calculating technical indicators...")
        df = self.calculate_council_indicators(df)
        
        # Apply Zen Council forecast logic
        print("Council applying Bull/Bear/Chop logic...")
        df = self.zen_council_forecast_logic(df)
        
        # Validate with Council standards
        print("Council validating forecast accuracy...")
        df = self.validate_council_accuracy(df)
        
        # Analysis
        complete_df = df.dropna(subset=['next_day_return', 'forecast_hit']).copy()
        
        if len(complete_df) == 0:
            print("No complete records for Council analysis")
            return df
        
        # Performance metrics
        total_forecasts = len(complete_df)
        hits = complete_df['forecast_hit'].sum()
        accuracy = hits / total_forecasts * 100
        
        print("\n" + "=" * 60)
        print("ZEN COUNCIL BACKTEST RESULTS")
        print("=" * 60)
        print(f"Total Forecasts: {total_forecasts}")
        print(f"Hits: {hits}")
        print(f"Overall Accuracy: {accuracy:.1f}%")
        
        # Check target achievement
        if accuracy >= 70:
            print(f"🎯 COUNCIL TARGET ACHIEVED: {accuracy:.1f}% >= 70%")
        else:
            print(f"⚠️  BELOW COUNCIL TARGET: {accuracy:.1f}% < 70%")
        
        # Bias performance analysis
        print(f"\nCouncil Performance by Bias:")
        bias_performance = complete_df.groupby('forecast_bias')['forecast_hit'].agg(['count', 'sum', 'mean'])
        bias_performance.columns = ['Total', 'Hits', 'Accuracy']
        bias_performance['Accuracy'] = bias_performance['Accuracy'] * 100
        
        for bias in bias_performance.index:
            total = bias_performance.loc[bias, 'Total']
            acc = bias_performance.loc[bias, 'Accuracy']
            print(f"  {bias}: {acc:.1f}% ({total} forecasts)")
        
        # VIX regime performance
        print(f"\nCouncil Performance by VIX Regime:")
        regime_performance = complete_df.groupby('vix_regime')['forecast_hit'].agg(['count', 'sum', 'mean'])
        regime_performance.columns = ['Total', 'Hits', 'Accuracy']
        regime_performance['Accuracy'] = regime_performance['Accuracy'] * 100
        
        for regime in regime_performance.index:
            total = regime_performance.loc[regime, 'Total']
            acc = regime_performance.loc[regime, 'Accuracy']
            print(f"  {regime}: {acc:.1f}% ({total} forecasts)")
        
        # Signal analysis
        print(f"\nCouncil Signal Distribution:")
        bull_avg = complete_df['bull_signals'].mean()
        bear_avg = complete_df['bear_signals'].mean()
        chop_avg = complete_df['chop_signals'].mean()
        print(f"  Average Bull Signals: {bull_avg:.1f}")
        print(f"  Average Bear Signals: {bear_avg:.1f}")
        print(f"  Average Chop Signals: {chop_avg:.1f}")
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"zen_council_backtest_{timestamp}.csv"
        complete_df.to_csv(output_file, index=False)
        print(f"\nCouncil results saved: {output_file}")
        
        # Generate Council learning corpus
        self.create_council_learning_corpus(complete_df, accuracy)
        
        return complete_df
    
    def create_council_learning_corpus(self, df: pd.DataFrame, accuracy: float):
        """Create learning corpus for Council adaptive system"""
        learning_records = []
        
        for _, row in df.iterrows():
            if pd.isna(row['next_day_close']) or pd.isna(row['forecast_hit']):
                continue
                
            record = {
                'DATE': str(row['date']),
                'INDEX': 'SPX',
                'FORECAST_BIAS': row['forecast_bias'],
                'ACTUAL_CLOSE': float(row['next_day_close']),
                'HIT': bool(row['forecast_hit']),
                'PRICE_CHANGE_PCT': float(row['next_day_return']),
                'VIX_REGIME': row['vix_regime'],
                'VIX_LEVEL': float(row['vix_close']) if pd.notna(row['vix_close']) else None,
                'RSI': float(row['rsi']) if pd.notna(row['rsi']) else None,
                'BULL_SIGNALS': int(row['bull_signals']),
                'BEAR_SIGNALS': int(row['bear_signals']),
                'CHOP_SIGNALS': int(row['chop_signals']),
                'VOLUME_RATIO': float(row['volume_ratio']) if pd.notna(row['volume_ratio']) else None,
                'SPX_VIX_CORRELATION': float(row['spx_vix_correlation']) if pd.notna(row['spx_vix_correlation']) else None
            }
            learning_records.append(record)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        corpus_file = f"zen_council_learning_corpus_{timestamp}.json"
        
        with open(corpus_file, 'w') as f:
            json.dump(learning_records, f, indent=2, default=str)
        
        print(f"Council learning corpus saved: {corpus_file}")
        print(f"Records for Council adaptive system: {len(learning_records)}")
        print(f"Council baseline achieved: {accuracy:.1f}%")
        
        if accuracy >= 70:
            print("Council declares system ready for news attribution integration (target: 88%)")
        else:
            print("Council recommends parameter refinement before proceeding")

if __name__ == "__main__":
    print("Assembling Zen Council for backtesting...")
    backtester = ZenCouncilBacktester()
    
    results = backtester.run_zen_council_backtest(
        start_date='2024-01-01',
        end_date='2025-08-22'
    )
    
    if len(results) > 0:
        print(f"\nZen Council backtesting complete!")
        print("Ready for Phase 2: News attribution + AI enhancement → 88% target")
    else:
        print("Council backtesting failed - review data availability")