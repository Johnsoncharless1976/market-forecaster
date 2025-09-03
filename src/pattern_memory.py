#!/usr/bin/env python3
"""
Pattern Memory System - Council's Playbook
Tracks recurring patterns in forecast performance
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List

class PatternMemory:
    def __init__(self):
        self.patterns = {}
        self.pattern_file = "output/patterns/zen_patterns.json"
        
    def identify_patterns(self, forecast_df: pd.DataFrame, miss_df: pd.DataFrame) -> Dict:
        """Identify recurring patterns in forecasts and misses"""
        
        patterns = {
            'accuracy_patterns': self._find_accuracy_patterns(forecast_df),
            'miss_patterns': self._find_miss_patterns(miss_df),
            'confidence_patterns': self._find_confidence_patterns(forecast_df),
            'temporal_patterns': self._find_temporal_patterns(forecast_df)
        }
        
        return patterns
    
    def _find_accuracy_patterns(self, df: pd.DataFrame) -> Dict:
        """Find patterns in accuracy over time"""
        
        if df.empty:
            return {}
        
        # Weekly accuracy patterns
        df['weekday'] = df['FORECAST_DATE'].dt.day_name()
        weekday_accuracy = df.groupby('weekday')['ACCURACY'].mean().to_dict()
        
        # Confidence level accuracy
        df['confidence_bucket'] = pd.cut(df['CONFIDENCE'], 
                                       bins=[0, 0.7, 0.8, 0.9, 1.0], 
                                       labels=['Low', 'Med', 'High', 'Very High'])
        confidence_accuracy = df.groupby('confidence_bucket')['ACCURACY'].mean().to_dict()
        
        return {
            'weekday_performance': weekday_accuracy,
            'confidence_performance': {str(k): v for k, v in confidence_accuracy.items()}
        }
    
    def _find_miss_patterns(self, miss_df: pd.DataFrame) -> Dict:
        """Find patterns in forecast misses"""
        
        if miss_df.empty:
            return {}
        
        # Miss cause patterns
        cause_patterns = miss_df['primary_cause'].value_counts().to_dict() if 'primary_cause' in miss_df.columns else {}
        
        # Temporal miss patterns
        miss_df['weekday'] = pd.to_datetime(miss_df['forecast_date']).dt.day_name()
        weekday_misses = miss_df['weekday'].value_counts().to_dict()
        
        return {
            'cause_frequency': cause_patterns,
            'weekday_misses': weekday_misses
        }
    
    def _find_confidence_patterns(self, df: pd.DataFrame) -> Dict:
        """Find patterns in confidence vs performance"""
        
        if df.empty:
            return {}
        
        # Overconfidence detection
        high_conf_misses = df[(df['CONFIDENCE'] > 0.85) & (df['ACCURACY'] == 0)]
        overconfidence_rate = len(high_conf_misses) / len(df[df['CONFIDENCE'] > 0.85]) if len(df[df['CONFIDENCE'] > 0.85]) > 0 else 0
        
        return {
            'overconfidence_rate': overconfidence_rate,
            'high_confidence_accuracy': df[df['CONFIDENCE'] > 0.85]['ACCURACY'].mean()
        }
    
    def _find_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Find time-based patterns"""
        
        if df.empty:
            return {}
        
        # Month-over-month trends
        df['month'] = df['FORECAST_DATE'].dt.month
        monthly_accuracy = df.groupby('month')['ACCURACY'].mean().to_dict()
        
        return {
            'monthly_performance': monthly_accuracy
        }
    
    def save_patterns(self, patterns: Dict):
        """Save identified patterns to file"""
        
        os.makedirs("output/patterns", exist_ok=True)
        
        # Add timestamp
        patterns['analysis_date'] = datetime.now().isoformat()
        patterns['version'] = '1.0'
        
        with open(self.pattern_file, 'w') as f:
            json.dump(patterns, f, indent=2, default=str)
        
        print(f"Patterns saved to {self.pattern_file}")
    
    def load_patterns(self) -> Dict:
        """Load previously saved patterns"""
        
        if os.path.exists(self.pattern_file):
            with open(self.pattern_file, 'r') as f:
                return json.load(f)
        return {}

def main():
    """Run pattern analysis"""
    
    print("=== PATTERN MEMORY ANALYSIS ===")
    
    memory = PatternMemory()
    
    # This would load actual data
    print("Pattern memory system initialized")
    print("Ready to analyze forecast patterns when connected to data")
    
    return 0

if __name__ == "__main__":
    exit(main())
