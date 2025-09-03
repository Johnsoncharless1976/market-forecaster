# load_backtest_corpus.py
#!/usr/bin/env python3
"""
Load backtesting results into adaptive learning system
"""

import pandas as pd
import numpy as np
import json
import glob
from datetime import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

def load_latest_learning_corpus():
    """Load the most recent learning corpus file"""
    corpus_files = glob.glob("learning_corpus_*.json")
    if not corpus_files:
        print("No learning corpus files found")
        return None
    
    latest_file = max(corpus_files, key=lambda x: x.split('_')[-1])
    print(f"Loading corpus: {latest_file}")
    
    with open(latest_file, 'r') as f:
        return json.load(f)

def analyze_backtest_patterns(corpus_data):
    """Analyze the backtest corpus to discover patterns"""
    
    df = pd.DataFrame(corpus_data)
    print(f"\nAnalyzing {len(df)} backtest records...")
    
    # Basic performance metrics
    total_forecasts = len(df)
    hits = df['HIT'].sum()
    accuracy = hits / total_forecasts * 100
    
    print(f"Backtest Performance:")
    print(f"  Total Forecasts: {total_forecasts}")
    print(f"  Hits: {hits}")
    print(f"  Accuracy: {accuracy:.1f}%")
    
    # Analyze meaningful move thresholds
    df['abs_move'] = abs(df['PRICE_CHANGE_PCT'])
    
    # Find percentiles where accuracy changes
    percentiles = [50, 75, 90, 95]
    thresholds = {}
    
    for p in percentiles:
        threshold = np.percentile(df['abs_move'], p)
        above_threshold = df[df['abs_move'] > threshold]
        below_threshold = df[df['abs_move'] <= threshold]
        
        if len(above_threshold) > 10 and len(below_threshold) > 10:
            accuracy_above = above_threshold['HIT'].mean()
            accuracy_below = below_threshold['HIT'].mean()
            accuracy_diff = abs(accuracy_above - accuracy_below)
            
            thresholds[f"p{p}"] = {
                "threshold": round(threshold, 3),
                "accuracy_above": round(accuracy_above, 3),
                "accuracy_below": round(accuracy_below, 3),
                "significance": round(accuracy_diff, 3),
                "sample_above": len(above_threshold),
                "sample_below": len(below_threshold)
            }
    
    print(f"\nMove Significance Analysis:")
    for percentile, data in thresholds.items():
        print(f"  {percentile}: {data['threshold']}% threshold")
        print(f"    Above: {data['accuracy_above']:.1%} accuracy ({data['sample_above']} samples)")
        print(f"    Below: {data['accuracy_below']:.1%} accuracy ({data['sample_below']} samples)")
        print(f"    Difference: {data['significance']:.1%}")
    
    # Find best threshold
    if thresholds:
        best_threshold = max(thresholds.values(), key=lambda x: x['significance'])
        print(f"\nOptimal Move Threshold: {best_threshold['threshold']}%")
        print(f"  Creates {best_threshold['significance']:.1%} accuracy difference")
    
    # Bias performance analysis
    print(f"\nForecast Bias Performance:")
    bias_performance = df.groupby('FORECAST_BIAS')['HIT'].agg(['count', 'sum', 'mean'])
    for bias in bias_performance.index:
        count = bias_performance.loc[bias, 'count']
        accuracy = bias_performance.loc[bias, 'mean']
        print(f"  {bias}: {accuracy:.1%} ({count} forecasts)")
    
    # Level breach analysis
    if 'LEVEL_BREACH' in df.columns:
        breach_accuracy = df[df['LEVEL_BREACH'] == True]['HIT'].mean()
        no_breach_accuracy = df[df['LEVEL_BREACH'] == False]['HIT'].mean()
        print(f"\nLevel Breach Analysis:")
        print(f"  Breach accuracy: {breach_accuracy:.1%}")
        print(f"  No breach accuracy: {no_breach_accuracy:.1%}")
        print(f"  Difference: {abs(breach_accuracy - no_breach_accuracy):.1%}")
    
    # Machine learning model training
    print(f"\nTraining ML Model...")
    
    # Prepare features (only use available numeric columns)
    feature_columns = []
    if 'PRICE_CHANGE_PCT' in df.columns:
        df['abs_price_change'] = abs(df['PRICE_CHANGE_PCT'])
        feature_columns.append('abs_price_change')
    
    if 'RSI' in df.columns and df['RSI'].notna().any():
        df['RSI'] = df['RSI'].fillna(50)  # Fill NaN with neutral RSI
        feature_columns.append('RSI')
    
    if 'ATR' in df.columns and df['ATR'].notna().any():
        df['ATR'] = df['ATR'].fillna(df['ATR'].mean())
        feature_columns.append('ATR')
    
    # Add bias encoding
    df['bias_bullish'] = (df['FORECAST_BIAS'] == 'Bullish').astype(int)
    df['bias_bearish'] = (df['FORECAST_BIAS'] == 'Bearish').astype(int)
    feature_columns.extend(['bias_bullish', 'bias_bearish'])
    
    if len(feature_columns) >= 2:
        X = df[feature_columns].values
        y = df['HIT'].astype(int).values
        
        # Train model
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        model = LogisticRegression(random_state=42)
        model.fit(X_train_scaled, y_train)
        
        train_accuracy = model.score(X_train_scaled, y_train)
        test_accuracy = model.score(X_test_scaled, y_test)
        
        print(f"  Train Accuracy: {train_accuracy:.1%}")
        print(f"  Test Accuracy: {test_accuracy:.1%}")
        print(f"  Feature Importance: {dict(zip(feature_columns, model.coef_[0]))}")
    
    # Save learned parameters
    learned_params = {
        "move_thresholds": thresholds,
        "optimal_threshold": best_threshold if thresholds else None,
        "bias_performance": bias_performance.to_dict() if not bias_performance.empty else {},
        "overall_accuracy": accuracy,
        "total_samples": total_forecasts,
        "learning_date": datetime.now().isoformat(),
        "corpus_file": latest_file
    }
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    params_file = f"learned_parameters_{timestamp}.json"
    
    with open(params_file, 'w') as f:
        json.dump(learned_params, f, indent=2, default=str)
    
    print(f"\nLearned parameters saved: {params_file}")
    return learned_params

if __name__ == "__main__":
    print("Loading Backtest Learning Corpus...")
    print("=" * 50)
    
    corpus_data = load_latest_learning_corpus()
    if corpus_data:
        # Get the filename for the learned parameters
        corpus_files = glob.glob("learning_corpus_*.json")
        latest_file = max(corpus_files, key=lambda x: x.split('_')[-1])
        
        learned_params = analyze_backtest_patterns(corpus_data, latest_file)
        print(f"\nAdaptive learning complete!")
        print(f"Discovered patterns from {len(corpus_data)} historical forecasts")
    else:
        print("No learning corpus found. Run fixed_historical_backtest.py first.")