# adaptive_news_attribution_learning.py
#!/usr/bin/env python3
"""
Adaptive News Attribution Learning System
Self-learning system that discovers optimal parameters from historical performance
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json
import snowflake.connector
import os
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

class AdaptiveNewsLearning:
    def __init__(self):
        self.minimum_learning_samples = 50  # Need at least 50 data points to learn
        self.learned_parameters = {
            "thresholds": {},
            "weights": {},
            "correlations": {},
            "last_updated": None,
            "sample_size": 0,
            "confidence_level": 0.0
        }
        # AI Model Configuration for cost efficiency
        self.ai_config = {
            "primary_model": "claude-3-haiku",     # For daily forecasting work
            "analysis_model": "claude-3-haiku",    # For news attribution analysis
            "consensus_model": "claude-3-haiku",   # For Zen Council consensus
            "fallback_model": "claude-3-sonnet",   # Only for complex paradigm shifts
            "max_tokens": 1000,                    # Keep responses focused
            "temperature": 0.1                     # Low temperature for consistency
        }
        
    def connect_to_snowflake(self):
        """Connect to Snowflake using environment variables"""
        return snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
    
    def extract_historical_performance(self) -> pd.DataFrame:
        """
        Extract and analyze historical forecast performance data
        """
        conn = self.connect_to_snowflake()
        
        # Query to get forecast performance with market data
        query = """
        WITH market_moves AS (
            SELECT 
                DATE,
                SPY_CLOSE as current_price,
                LAG(SPY_CLOSE) OVER (ORDER BY DATE) as prev_close,
                ((SPY_CLOSE - LAG(SPY_CLOSE) OVER (ORDER BY DATE)) / 
                 LAG(SPY_CLOSE) OVER (ORDER BY DATE)) * 100 as price_change_pct
            FROM ZEN_MARKET.FORECASTING.DAILY_MARKET_DATA
        ),
        forecast_performance AS (
            SELECT 
                fp.DATE,
                fp.INDEX as symbol,
                fp.FORECAST_BIAS,
                fp.ACTUAL_CLOSE,
                fp.HIT,
                mm.price_change_pct,
                mm.current_price
            FROM ZEN_MARKET.FORECASTING.FORECAST_POSTMORTEM fp
            JOIN market_moves mm ON fp.DATE = mm.DATE
        ),
        forecast_levels AS (
            SELECT 
                DATE,
                SUPPORTS,
                RESISTANCES
            FROM ZEN_MARKET.FORECASTING.FORECAST_SUMMARY
        )
        SELECT 
            fp.*,
            fl.SUPPORTS,
            fl.RESISTANCES
        FROM forecast_performance fp
        LEFT JOIN forecast_levels fl ON fp.DATE = fl.DATE
        ORDER BY fp.DATE DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    
    def analyze_meaningful_move_thresholds(self, df: pd.DataFrame) -> Dict:
        """
        Learn what percentage moves actually correlate with forecast hits/misses
        """
        if len(df) < self.minimum_learning_samples:
            return {"error": f"Insufficient data: {len(df)} samples, need {self.minimum_learning_samples}"}
        
        # Analyze relationship between move size and forecast accuracy
        df['abs_move'] = df['price_change_pct'].abs()
        
        # Find percentile thresholds where forecast accuracy changes
        percentiles = [10, 25, 50, 75, 90, 95]
        thresholds = {}
        
        for p in percentiles:
            threshold = np.percentile(df['abs_move'].dropna(), p)
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
                    "significance": round(accuracy_diff, 3)
                }
        
        # Find the threshold with maximum accuracy difference
        best_threshold = max(thresholds.values(), key=lambda x: x['significance'])
        
        return {
            "learned_move_threshold": best_threshold['threshold'],
            "confidence": best_threshold['significance'],
            "all_thresholds": thresholds,
            "sample_size": len(df)
        }
    
    def learn_support_resistance_breach_patterns(self, df: pd.DataFrame) -> Dict:
        """
        Analyze when support/resistance breaches actually matter
        """
        breach_analysis = {
            "resistance_breaches": [],
            "support_breaches": [],
            "learned_parameters": {}
        }
        
        for _, row in df.iterrows():
            if pd.isna(row['RESISTANCES']) or pd.isna(row['SUPPORTS']):
                continue
                
            current_price = row['current_price']
            
            # Parse resistance levels (assuming comma-separated)
            try:
                resistances = [float(x.strip()) for x in str(row['RESISTANCES']).split(',')]
                supports = [float(x.strip()) for x in str(row['SUPPORTS']).split(',')]
            except:
                continue
            
            # Check resistance breaches
            for resistance in resistances:
                if current_price > resistance:
                    breach_pct = ((current_price - resistance) / resistance) * 100
                    breach_analysis["resistance_breaches"].append({
                        'date': row['DATE'],
                        'breach_percent': breach_pct,
                        'forecast_hit': row['HIT'],
                        'market_move': row['price_change_pct']
                    })
            
            # Check support breaches
            for support in supports:
                if current_price < support:
                    breach_pct = ((support - current_price) / support) * 100
                    breach_analysis["support_breaches"].append({
                        'date': row['DATE'],
                        'breach_percent': breach_pct,
                        'forecast_hit': row['HIT'],
                        'market_move': row['price_change_pct']
                    })
        
        # Analyze meaningful breach thresholds
        all_breaches = breach_analysis["resistance_breaches"] + breach_analysis["support_breaches"]
        
        if len(all_breaches) >= 20:
            breach_df = pd.DataFrame(all_breaches)
            
            # Find breach percentage that correlates with forecast accuracy
            median_breach = breach_df['breach_percent'].median()
            significant_breaches = breach_df[breach_df['breach_percent'] > median_breach]
            minor_breaches = breach_df[breach_df['breach_percent'] <= median_breach]
            
            if len(significant_breaches) > 5 and len(minor_breaches) > 5:
                sig_accuracy = significant_breaches['forecast_hit'].mean()
                minor_accuracy = minor_breaches['forecast_hit'].mean()
                
                breach_analysis["learned_parameters"] = {
                    "meaningful_breach_threshold": round(median_breach, 2),
                    "significant_breach_accuracy": round(sig_accuracy, 3),
                    "minor_breach_accuracy": round(minor_accuracy, 3),
                    "total_breaches": len(all_breaches)
                }
        
        return breach_analysis
    
    def discover_news_timing_correlations(self, df: pd.DataFrame) -> Dict:
        """
        Future: Analyze news timing vs forecast accuracy
        This would require historical news data to be meaningful
        """
        return {
            "status": "pending_news_data_integration",
            "note": "Requires historical news corpus to learn timing correlations"
        }
    
    def train_adaptive_model(self, df: pd.DataFrame) -> Dict:
        """
        Train machine learning model to predict when moves are significant
        """
        if len(df) < self.minimum_learning_samples:
            return {"error": "Insufficient training data"}
        
        # Prepare features
        features = []
        targets = []
        
        for _, row in df.iterrows():
            if pd.isna(row['price_change_pct']):
                continue
                
            feature_vector = [
                abs(row['price_change_pct']),  # Absolute move size
                1 if row['FORECAST_BIAS'] == 'Bullish' else 0,  # Bias direction
                1 if row['HIT'] else 0  # Previous forecast accuracy
            ]
            
            # Target: was this a "significant" move (forecast miss correlates with large moves)
            target = 1 if abs(row['price_change_pct']) > 0.5 else 0
            
            features.append(feature_vector)
            targets.append(target)
        
        if len(features) < 30:
            return {"error": "Insufficient features for ML training"}
        
        # Train model
        X = np.array(features)
        y = np.array(targets)
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        model = LogisticRegression(random_state=42)
        model.fit(X_train_scaled, y_train)
        
        # Evaluate
        train_accuracy = model.score(X_train_scaled, y_train)
        test_accuracy = model.score(X_test_scaled, y_test)
        
        return {
            "model_trained": True,
            "train_accuracy": round(train_accuracy, 3),
            "test_accuracy": round(test_accuracy, 3),
            "feature_importance": model.coef_[0].tolist(),
            "sample_size": len(features)
        }
    
    def run_adaptive_learning(self) -> Dict:
        """
        Master function: Learn all parameters from historical data
        """
        print("Starting Adaptive Learning Process...")
        print("=" * 50)
        
        # Extract historical data
        print("Extracting historical performance data...")
        try:
            df = self.extract_historical_performance()
            print(f"Loaded {len(df)} historical records")
        except Exception as e:
            return {"error": f"Data extraction failed: {str(e)}"}
        
        if len(df) < self.minimum_learning_samples:
            return {
                "error": f"Insufficient data for learning",
                "records_found": len(df),
                "minimum_required": self.minimum_learning_samples
            }
        
        # Learn meaningful move thresholds
        print("Learning meaningful move thresholds...")
        move_analysis = self.analyze_meaningful_move_thresholds(df)
        
        # Learn support/resistance patterns
        print("Learning support/resistance breach patterns...")
        breach_analysis = self.learn_support_resistance_breach_patterns(df)
        
        # Train adaptive model
        print("Training adaptive ML model...")
        ml_results = self.train_adaptive_model(df)
        
        # News timing analysis (placeholder)
        news_analysis = self.discover_news_timing_correlations(df)
        
        # Compile learned parameters
        self.learned_parameters = {
            "move_thresholds": move_analysis,
            "breach_patterns": breach_analysis,
            "ml_model": ml_results,
            "news_timing": news_analysis,
            "last_updated": datetime.now().isoformat(),
            "total_samples": len(df),
            "learning_confidence": "HIGH" if len(df) > 100 else "MODERATE"
        }
        
        # Save learned parameters
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"learned_parameters_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(self.learned_parameters, f, indent=2, default=str)
        
        print("\n" + "=" * 50)
        print("ADAPTIVE LEARNING RESULTS")
        print("=" * 50)
        
        if "learned_move_threshold" in move_analysis:
            threshold = move_analysis["learned_move_threshold"]
            confidence = move_analysis["confidence"]
            print(f"✅ LEARNED MOVE THRESHOLD: {threshold:.3f}% (confidence: {confidence:.3f})")
        
        if "learned_parameters" in breach_analysis and breach_analysis["learned_parameters"]:
            breach_threshold = breach_analysis["learned_parameters"]["meaningful_breach_threshold"]
            print(f"✅ LEARNED BREACH THRESHOLD: {breach_threshold:.2f}%")
        
        if ml_results.get("model_trained"):
            accuracy = ml_results["test_accuracy"]
            print(f"✅ ML MODEL TRAINED: {accuracy:.1%} accuracy")
        
        print(f"\n📄 Parameters saved: {output_file}")
        print(f"📊 Total learning samples: {len(df)}")
        print(f"🎯 System confidence: {self.learned_parameters['learning_confidence']}")
        
        return self.learned_parameters

# Example usage
if __name__ == "__main__":
    learner = AdaptiveNewsLearning()
    results = learner.run_adaptive_learning()
    
    if "error" not in results:
        print("\n🧠 SYSTEM IS NOW LEARNING FROM YOUR DATA")
        print("Parameters will self-adjust based on actual performance patterns")
    else:
        print(f"\n❌ Learning failed: {results['error']}")