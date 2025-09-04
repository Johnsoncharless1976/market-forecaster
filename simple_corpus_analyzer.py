# simple_corpus_analyzer.py
#!/usr/bin/env python3
"""
Simple analysis of backtest learning corpus
"""

import pandas as pd
import numpy as np
import json
import glob
from datetime import datetime

def main():
    print("Analyzing Backtest Learning Corpus...")
    print("=" * 50)
    
    # Find latest corpus file
    corpus_files = glob.glob("learning_corpus_*.json")
    if not corpus_files:
        print("No learning corpus found. Run fixed_historical_backtest.py first.")
        return
    
    latest_file = max(corpus_files, key=lambda x: x.split('_')[-1])
    print(f"Loading: {latest_file}")
    
    # Load data
    with open(latest_file, 'r') as f:
        corpus_data = json.load(f)
    
    df = pd.DataFrame(corpus_data)
    print(f"Records loaded: {len(df)}")
    
    # Basic analysis
    total_forecasts = len(df)
    hits = df['HIT'].sum()
    accuracy = hits / total_forecasts * 100
    
    print(f"\nBacktest Performance:")
    print(f"  Total Forecasts: {total_forecasts}")
    print(f"  Hits: {hits}")
    print(f"  Accuracy: {accuracy:.1f}%")
    
    # Bias performance
    print(f"\nForecast Bias Performance:")
    bias_performance = df.groupby('FORECAST_BIAS')['HIT'].agg(['count', 'mean'])
    for bias in bias_performance.index:
        count = bias_performance.loc[bias, 'count']
        acc = bias_performance.loc[bias, 'mean']
        print(f"  {bias}: {acc:.1%} ({count} forecasts)")
    
    # Move size analysis
    df['abs_move'] = abs(df['PRICE_CHANGE_PCT'])
    small_moves = df[df['abs_move'] <= 0.5]['HIT'].mean()
    medium_moves = df[(df['abs_move'] > 0.5) & (df['abs_move'] <= 1.5)]['HIT'].mean()
    large_moves = df[df['abs_move'] > 1.5]['HIT'].mean()
    
    print(f"\nMove Size Analysis:")
    print(f"  Small moves (≤0.5%): {small_moves:.1%}")
    print(f"  Medium moves (0.5-1.5%): {medium_moves:.1%}")
    print(f"  Large moves (>1.5%): {large_moves:.1%}")
    
    # Level breach analysis
    if 'LEVEL_BREACH' in df.columns:
        breach_accuracy = df[df['LEVEL_BREACH'] == True]['HIT'].mean()
        no_breach_accuracy = df[df['LEVEL_BREACH'] == False]['HIT'].mean()
        breach_count = df['LEVEL_BREACH'].sum()
        
        print(f"\nLevel Breach Analysis:")
        print(f"  Level breaches: {breach_count} ({breach_count/len(df):.1%})")
        print(f"  Breach accuracy: {breach_accuracy:.1%}")
        print(f"  No breach accuracy: {no_breach_accuracy:.1%}")
    
    # Save simple learned parameters
    learned_params = {
        "overall_accuracy": accuracy,
        "total_samples": total_forecasts,
        "bias_performance": {
            bias: {
                "count": int(bias_performance.loc[bias, 'count']),
                "accuracy": float(bias_performance.loc[bias, 'mean'])
            } for bias in bias_performance.index
        },
        "move_analysis": {
            "small_moves_accuracy": float(small_moves),
            "medium_moves_accuracy": float(medium_moves),
            "large_moves_accuracy": float(large_moves)
        },
        "learning_date": datetime.now().isoformat(),
        "source_file": latest_file
    }
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"learned_parameters_{timestamp}.json"
    
    with open(output_file, 'w') as f:
        json.dump(learned_params, f, indent=2, default=str)
    
    print(f"\nLearned parameters saved: {output_file}")
    print(f"\nKey Insights:")
    print(f"- Your backtesting shows {accuracy:.1f}% baseline accuracy")
    print(f"- Bullish forecasts perform best") if 'Bullish' in bias_performance.index and bias_performance.loc['Bullish', 'mean'] > 0.5 else print(f"- Neutral forecasts most reliable")
    print(f"- System ready for adaptive parameter optimization")

if __name__ == "__main__":
    main()