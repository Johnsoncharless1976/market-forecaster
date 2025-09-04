# zen_council_regime_optimizer.py
#!/usr/bin/env python3
"""
Zen Council Multi-Regime Parameter Optimization System
Systematic testing of parameter sets for Low Vol, Normal Vol, and High Vol regimes
Target: Optimize each regime independently then combine for overall >70% accuracy
"""

import pandas as pd
import numpy as np
from datetime import datetime
import snowflake.connector
import os
import json

class ZenCouncilRegimeOptimizer:
    def __init__(self):
        self.regime_results = {}
        self.optimization_tests = []
        
    def connect_to_snowflake(self):
        # Try environment variables first, then .env file
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
                print("No .env file found - using environment variables only")
        
        if not account:
            raise Exception("Snowflake credentials not configured")
        
        return snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse
        )
    
    def load_historical_data(self, start_date: str = '2024-01-01') -> pd.DataFrame:
        """Load SPX and VIX data for regime analysis"""
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
        
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()
        
        return df
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators"""
        df = df.copy()
        
        # Basic calculations
        df['daily_return'] = df['spx_close'].pct_change() * 100
        df['prev_close'] = df['spx_close'].shift(1)
        
        # RSI (14-day)
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
        
        # Volume analysis
        df['volume_20ma'] = df['spx_volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['spx_volume'] / df['volume_20ma']
        
        # VIX analysis
        df['vix_change'] = df['vix_close'].diff()
        df['vix_percentile'] = df['vix_close'].rolling(60).rank(pct=True) * 100
        
        # VIX regime classification
        df['vix_regime'] = 'NORMAL'
        df.loc[df['vix_close'] < 17, 'vix_regime'] = 'LOW_VOL'
        df.loc[df['vix_close'] > 21, 'vix_regime'] = 'HIGH_VOL'
        
        # RSI momentum
        df['rsi_momentum'] = df['rsi'].diff().rolling(3).mean()
        
        return df
    
    def generate_forecast_with_params(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Generate forecasts using specific parameter set"""
        df = df.copy()
        
        # Initialize forecast columns
        df['forecast_bias'] = 'Chop'
        df['bull_signals'] = 0
        df['bear_signals'] = 0
        df['chop_signals'] = 0
        df['signal_details'] = ''
        
        # Support/Resistance with parameter-specific multiplier
        atr_multiplier = params.get('atr_multiplier', 1.2)
        df['support_level'] = df['spx_close'] - (df['atr'] * atr_multiplier)
        df['resistance_level'] = df['spx_close'] + (df['atr'] * atr_multiplier)
        
        for idx in range(len(df)):
            if pd.isna(df.iloc[idx]['rsi']) or pd.isna(df.iloc[idx]['vix_close']):
                continue
            
            # Extract variables for this row
            rsi = df.iloc[idx]['rsi']
            vix = df.iloc[idx]['vix_close']
            vix_change = df.iloc[idx]['vix_change'] if pd.notna(df.iloc[idx]['vix_change']) else 0
            volume_ratio = df.iloc[idx]['volume_ratio']
            price = df.iloc[idx]['spx_close']
            support = df.iloc[idx]['support_level']
            resistance = df.iloc[idx]['resistance_level']
            vix_percentile = df.iloc[idx]['vix_percentile'] if pd.notna(df.iloc[idx]['vix_percentile']) else 50
            rsi_momentum = df.iloc[idx]['rsi_momentum'] if pd.notna(df.iloc[idx]['rsi_momentum']) else 0
            
            bull_signals = 0
            bear_signals = 0
            chop_signals = 0
            signal_details = []
            
            # BULL SIGNAL ANALYSIS with regime-specific parameters
            
            # RSI oversold
            if rsi < params['rsi_bull_threshold']:
                bull_signals += 1
                signal_details.append(f"RSI_OVERSOLD({rsi:.1f})")
            
            # Technical breach
            if price < support:
                bull_signals += 1
                signal_details.append("SUPPORT_BREACH")
            
            # VIX fear
            if vix > params['vix_fear_threshold'] or vix_change > 2.5:
                bull_signals += 1
                signal_details.append(f"VIX_FEAR({vix:.1f})")
            
            # VIX percentile fear
            if vix_percentile > 75:
                bull_signals += 1
                signal_details.append(f"VIX_PERCENTILE_HIGH({vix_percentile:.1f})")
            
            # Volume conviction
            if volume_ratio > params['volume_threshold']:
                bull_signals += 1
                signal_details.append(f"VOLUME_CONVICTION({volume_ratio:.2f})")
            
            # RSI momentum
            if rsi < 40 and rsi_momentum > 0.5:
                bull_signals += 1
                signal_details.append("RSI_MOMENTUM_UP")
            
            # BEAR SIGNAL ANALYSIS with regime-specific parameters
            
            # RSI overbought
            if rsi > params['rsi_bear_threshold']:
                bear_signals += 1
                signal_details.append(f"RSI_OVERBOUGHT({rsi:.1f})")
            
            # Technical breach
            if price > resistance:
                bear_signals += 1
                signal_details.append("RESISTANCE_BREACH")
            
            # VIX complacency
            if vix < params['vix_complacency_threshold'] or vix_change < -1.5:
                bear_signals += 1
                signal_details.append(f"VIX_COMPLACENCY({vix:.1f})")
            
            # VIX percentile low
            if vix_percentile < 25:
                bear_signals += 1
                signal_details.append(f"VIX_PERCENTILE_LOW({vix_percentile:.1f})")
            
            # Distribution volume
            if volume_ratio > params['volume_threshold']:
                bear_signals += 1
                signal_details.append(f"DISTRIBUTION_VOLUME({volume_ratio:.2f})")
            
            # RSI momentum
            if rsi > 60 and rsi_momentum < -0.5:
                bear_signals += 1
                signal_details.append("RSI_MOMENTUM_DOWN")
            
            # CHOP CONDITIONS
            chop_conditions = [
                35 <= rsi <= 65,
                support * 0.995 <= price <= resistance * 1.005,
                volume_ratio < 1.15,
                abs(vix_change) < 2.0,
                25 <= vix_percentile <= 75
            ]
            chop_signals = sum(chop_conditions)
            
            # DECISION LOGIC with regime-specific confirmation requirements
            confirmation_required = params['confirmation_required']
            
            if bull_signals >= confirmation_required and bull_signals > bear_signals:
                df.iloc[idx, df.columns.get_loc('forecast_bias')] = 'Bull'
            elif bear_signals >= confirmation_required and bear_signals > bull_signals:
                df.iloc[idx, df.columns.get_loc('forecast_bias')] = 'Bear'
            elif chop_signals >= 4:
                df.iloc[idx, df.columns.get_loc('forecast_bias')] = 'Chop'
            
            # Store signals
            df.iloc[idx, df.columns.get_loc('bull_signals')] = bull_signals
            df.iloc[idx, df.columns.get_loc('bear_signals')] = bear_signals
            df.iloc[idx, df.columns.get_loc('chop_signals')] = chop_signals
            df.iloc[idx, df.columns.get_loc('signal_details')] = '; '.join(signal_details)
        
        return df
    
    def validate_forecasts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate forecasts against next-day performance"""
        df = df.copy()
        
        # Get next day data
        df['next_day_return'] = df['daily_return'].shift(-1)
        df['forecast_hit'] = False
        
        for idx in range(len(df) - 1):
            forecast = df.iloc[idx]['forecast_bias']
            next_return = df.iloc[idx]['next_day_return']
            
            if pd.isna(next_return):
                continue
            
            # Success criteria
            if forecast == 'Bull':
                df.iloc[idx, df.columns.get_loc('forecast_hit')] = next_return > 0.15
            elif forecast == 'Bear':
                df.iloc[idx, df.columns.get_loc('forecast_hit')] = next_return < -0.05
            elif forecast == 'Chop':
                df.iloc[idx, df.columns.get_loc('forecast_hit')] = abs(next_return) <= 0.75
        
        return df
    
    def calculate_regime_performance(self, df: pd.DataFrame, regime: str) -> dict:
        """Calculate performance metrics for a specific regime"""
        regime_data = df[df['vix_regime'] == regime].copy()
        
        if len(regime_data) == 0:
            return {'error': f'No data for regime {regime}'}
        
        # Remove last row (no next day data)
        regime_data = regime_data.iloc[:-1]
        
        # Overall accuracy
        total_forecasts = len(regime_data)
        correct_forecasts = regime_data['forecast_hit'].sum()
        overall_accuracy = (correct_forecasts / total_forecasts * 100) if total_forecasts > 0 else 0
        
        # Accuracy by bias
        results = {
            'regime': regime,
            'total_forecasts': total_forecasts,
            'overall_accuracy': overall_accuracy,
            'correct_forecasts': correct_forecasts
        }
        
        for bias in ['Bull', 'Bear', 'Chop']:
            bias_data = regime_data[regime_data['forecast_bias'] == bias]
            if len(bias_data) > 0:
                bias_correct = bias_data['forecast_hit'].sum()
                bias_accuracy = (bias_correct / len(bias_data) * 100)
                results[f'{bias.lower()}_accuracy'] = bias_accuracy
                results[f'{bias.lower()}_count'] = len(bias_data)
                results[f'{bias.lower()}_correct'] = bias_correct
            else:
                results[f'{bias.lower()}_accuracy'] = 0
                results[f'{bias.lower()}_count'] = 0
                results[f'{bias.lower()}_correct'] = 0
        
        return results
    
    def test_regime_parameters(self, regime: str, parameter_sets: list) -> list:
        """Test multiple parameter sets for a specific regime"""
        print(f"\nTesting {regime} regime optimization...")
        print("=" * 50)
        
        # Load data
        df = self.load_historical_data('2024-01-01')
        df = self.calculate_indicators(df)
        
        # Filter for specific regime
        regime_data = df[df['vix_regime'] == regime].copy()
        
        if len(regime_data) == 0:
            print(f"No data available for {regime} regime")
            return []
        
        print(f"Testing with {len(regime_data)} {regime} sessions")
        
        results = []
        
        for i, params in enumerate(parameter_sets, 1):
            print(f"\nTest {i}/{len(parameter_sets)}: {params}")
            
            # Generate forecasts with these parameters
            forecast_data = self.generate_forecast_with_params(regime_data, params)
            
            # Validate forecasts
            validated_data = self.validate_forecasts(forecast_data)
            
            # Calculate performance
            performance = self.calculate_regime_performance(validated_data, regime)
            performance['parameters'] = params
            performance['test_number'] = i
            
            # Print results
            if 'error' not in performance:
                print(f"Result: {performance['overall_accuracy']:.1f}% accuracy "
                      f"[Bull: {performance['bull_accuracy']:.1f}% ({performance['bull_count']}), "
                      f"Bear: {performance['bear_accuracy']:.1f}% ({performance['bear_count']}), "
                      f"Chop: {performance['chop_accuracy']:.1f}% ({performance['chop_count']})]")
            
            results.append(performance)
        
        return results
    
    def run_regime_optimization(self):
        """Execute systematic regime optimization"""
        
        print("ZEN COUNCIL MULTI-REGIME PARAMETER OPTIMIZATION")
        print("=" * 60)
        print("Mission: Optimize each VIX regime independently")
        print("Current Baseline: Low Vol 73.5%, Normal Vol 60.6%, High Vol 56.9%")
        print("Targets: Low Vol 75%+, Normal Vol 68%+, High Vol 62%+")
        print("=" * 60)
        
        # NORMAL VOL REGIME TESTS (PRIORITY - biggest improvement opportunity)
        normal_vol_params = [
            # Test 1: Lower RSI thresholds, higher confirmation
            {'rsi_bull_threshold': 22, 'rsi_bear_threshold': 78, 'vix_fear_threshold': 22, 'vix_complacency_threshold': 18, 'volume_threshold': 1.0, 'confirmation_required': 4, 'atr_multiplier': 1.3},
            # Test 2: Baseline with higher ATR multiplier
            {'rsi_bull_threshold': 25, 'rsi_bear_threshold': 75, 'vix_fear_threshold': 22, 'vix_complacency_threshold': 20, 'volume_threshold': 1.0, 'confirmation_required': 3, 'atr_multiplier': 1.5},
            # Test 3: More sensitive RSI, tighter VIX ranges
            {'rsi_bull_threshold': 28, 'rsi_bear_threshold': 72, 'vix_fear_threshold': 20, 'vix_complacency_threshold': 22, 'volume_threshold': 0.9, 'confirmation_required': 3, 'atr_multiplier': 1.3},
            # Test 4: Conservative approach - higher confirmation needed
            {'rsi_bull_threshold': 25, 'rsi_bear_threshold': 75, 'vix_fear_threshold': 23, 'vix_complacency_threshold': 18, 'volume_threshold': 1.1, 'confirmation_required': 5, 'atr_multiplier': 1.2},
            # Test 5: Very sensitive RSI
            {'rsi_bull_threshold': 30, 'rsi_bear_threshold': 70, 'vix_fear_threshold': 22, 'vix_complacency_threshold': 20, 'volume_threshold': 1.0, 'confirmation_required': 3, 'atr_multiplier': 1.4}
        ]
        
        # LOW VOL REGIME TESTS (Fine-tuning from strong 73.5% baseline)
        low_vol_params = [
            # Test 1: More sensitive to capture additional moves
            {'rsi_bull_threshold': 23, 'rsi_bear_threshold': 77, 'vix_fear_threshold': 25, 'vix_complacency_threshold': 15, 'volume_threshold': 0.9, 'confirmation_required': 3, 'atr_multiplier': 1.2},
            # Test 2: Lower volume threshold for calmer markets
            {'rsi_bull_threshold': 20, 'rsi_bear_threshold': 80, 'vix_fear_threshold': 26, 'vix_complacency_threshold': 16, 'volume_threshold': 0.8, 'confirmation_required': 3, 'atr_multiplier': 1.1},
            # Test 3: Reduced confirmation requirements
            {'rsi_bull_threshold': 25, 'rsi_bear_threshold': 75, 'vix_fear_threshold': 24, 'vix_complacency_threshold': 15, 'volume_threshold': 1.0, 'confirmation_required': 2, 'atr_multiplier': 1.3},
            # Test 4: Extended VIX ranges
            {'rsi_bull_threshold': 27, 'rsi_bear_threshold': 73, 'vix_fear_threshold': 28, 'vix_complacency_threshold': 14, 'volume_threshold': 1.1, 'confirmation_required': 3, 'atr_multiplier': 1.2}
        ]
        
        # HIGH VOL REGIME TESTS (Conservative approach for challenging environment)
        high_vol_params = [
            # Test 1: Very conservative - high confirmation
            {'rsi_bull_threshold': 25, 'rsi_bear_threshold': 75, 'vix_fear_threshold': 25, 'vix_complacency_threshold': 10, 'volume_threshold': 1.2, 'confirmation_required': 5, 'atr_multiplier': 1.5},
            # Test 2: Wider RSI ranges for volatility
            {'rsi_bull_threshold': 20, 'rsi_bear_threshold': 80, 'vix_fear_threshold': 28, 'vix_complacency_threshold': 8, 'volume_threshold': 1.0, 'confirmation_required': 4, 'atr_multiplier': 1.4},
            # Test 3: Focus on extreme VIX levels
            {'rsi_bull_threshold': 30, 'rsi_bear_threshold': 70, 'vix_fear_threshold': 30, 'vix_complacency_threshold': 12, 'volume_threshold': 1.3, 'confirmation_required': 6, 'atr_multiplier': 1.6},
            # Test 4: Moderate approach
            {'rsi_bull_threshold': 25, 'rsi_bear_threshold': 75, 'vix_fear_threshold': 27, 'vix_complacency_threshold': 10, 'volume_threshold': 1.1, 'confirmation_required': 4, 'atr_multiplier': 1.3}
        ]
        
        # Execute tests
        all_results = {}
        
        # Test NORMAL VOL first (priority)
        all_results['NORMAL'] = self.test_regime_parameters('NORMAL', normal_vol_params)
        
        # Test LOW VOL
        all_results['LOW_VOL'] = self.test_regime_parameters('LOW_VOL', low_vol_params)
        
        # Test HIGH VOL
        all_results['HIGH_VOL'] = self.test_regime_parameters('HIGH_VOL', high_vol_params)
        
        return all_results
    
    def analyze_best_results(self, all_results: dict):
        """Analyze and report best performing parameters for each regime"""
        
        print("\n" + "=" * 70)
        print("ZEN COUNCIL REGIME OPTIMIZATION RESULTS")
        print("=" * 70)
        
        best_params = {}
        
        for regime, results in all_results.items():
            if not results or 'error' in results[0]:
                print(f"\n{regime}: No valid results")
                continue
            
            # Find best performing parameter set
            valid_results = [r for r in results if 'error' not in r and r['total_forecasts'] > 10]
            if not valid_results:
                print(f"\n{regime}: No valid results with sufficient data")
                continue
            
            best_result = max(valid_results, key=lambda x: x['overall_accuracy'])
            best_params[regime] = best_result
            
            print(f"\n{regime} REGIME BEST RESULT:")
            print(f"  Accuracy: {best_result['overall_accuracy']:.1f}% ({best_result['correct_forecasts']}/{best_result['total_forecasts']})")
            print(f"  Bull: {best_result['bull_accuracy']:.1f}% ({best_result['bull_correct']}/{best_result['bull_count']})")
            print(f"  Bear: {best_result['bear_accuracy']:.1f}% ({best_result['bear_correct']}/{best_result['bear_count']})")
            print(f"  Chop: {best_result['chop_accuracy']:.1f}% ({best_result['chop_correct']}/{best_result['chop_count']})")
            print(f"  Parameters: {best_result['parameters']}")
        
        return best_params

if __name__ == "__main__":
    print("Assembling Zen Council for Multi-Regime Optimization...")
    optimizer = ZenCouncilRegimeOptimizer()
    
    # Run optimization
    results = optimizer.run_regime_optimization()
    
    # Analyze best results
    best_params = optimizer.analyze_best_results(results)
    
    print(f"\nZen Council Multi-Regime Optimization Complete!")
    print(f"Best parameters identified for each regime")