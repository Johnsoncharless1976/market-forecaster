#!/usr/bin/env python3
"""
Post-Mortem Learning System Builder
Automates analysis of forecast misses for systematic learning
"""

import os
from datetime import datetime, timedelta

class PostMortemLearningBuilder:
    def __init__(self):
        pass
        
    def create_miss_analyzer(self):
        """Generate system to analyze forecast misses and categorize them"""
        
        analyzer_code = '''#!/usr/bin/env python3
"""
Post-Mortem Miss Analyzer
Systematically analyzes forecast misses and categorizes causes
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from snowflake_conn import get_snowflake_connection
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    print("Warning: Snowflake connection not available")
    SNOWFLAKE_AVAILABLE = False

class PostMortemAnalyzer:
    def __init__(self):
        self.miss_categories = {
            'NEWS_EVENT': 'Unexpected news caused market reaction',
            'MACRO_RELEASE': 'Economic data release impact',
            'VOLATILITY_SHIFT': 'Regime change in market volatility',
            'TECHNICAL_BREAK': 'Key technical level breakthrough',
            'CORRELATION_BREAK': 'SPX-VIX correlation breakdown',
            'OPEX_EFFECT': 'Options expiration impact',
            'LIQUIDITY_SHOCK': 'Market liquidity disruption',
            'UNKNOWN': 'Cause not immediately identifiable'
        }
        
    def get_forecast_performance(self, days_back: int = 30) -> pd.DataFrame:
        """Get forecast performance data"""
        
        if not SNOWFLAKE_AVAILABLE:
            return self._generate_sample_forecasts()
            
        try:
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                f.FORECAST_DATE,
                f.BULL_TARGET,
                f.BEAR_TARGET,
                f.CHOP_LOWER,
                f.CHOP_UPPER,
                f.CONFIDENCE,
                f.PREDICTED_DIRECTION,
                f.ACTUAL_DIRECTION,
                f.ACTUAL_CLOSE,
                f.FORECAST_ACCURACY,
                f.CREATED_AT
            FROM FORECAST_DAILY f
            WHERE f.FORECAST_DATE >= CURRENT_DATE() - %s
            AND f.ACTUAL_DIRECTION IS NOT NULL
            ORDER BY f.FORECAST_DATE DESC
            """
            
            cursor.execute(query, (days_back,))
            results = cursor.fetchall()
            
            if results:
                columns = ['FORECAST_DATE', 'BULL_TARGET', 'BEAR_TARGET', 'CHOP_LOWER', 'CHOP_UPPER',
                          'CONFIDENCE', 'PREDICTED_DIRECTION', 'ACTUAL_DIRECTION', 'ACTUAL_CLOSE', 
                          'FORECAST_ACCURACY', 'CREATED_AT']
                df = pd.DataFrame(results, columns=columns)
                df['FORECAST_DATE'] = pd.to_datetime(df['FORECAST_DATE'])
                return df
            else:
                print("No forecast data found")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error retrieving forecast data: {e}")
            return pd.DataFrame()
    
    def identify_misses(self, forecast_df: pd.DataFrame) -> pd.DataFrame:
        """Identify forecast misses for analysis"""
        
        if forecast_df.empty:
            return pd.DataFrame()
        
        # Define miss criteria
        misses = []
        
        for _, row in forecast_df.iterrows():
            is_miss = False
            miss_type = None
            miss_details = {}
            
            predicted = str(row.get('PREDICTED_DIRECTION', '')).upper()
            actual = str(row.get('ACTUAL_DIRECTION', '')).upper()
            actual_close = row.get('ACTUAL_CLOSE', 0)
            
            # Check directional miss
            if predicted != actual and predicted in ['BULL', 'BEAR', 'CHOP'] and actual in ['BULL', 'BEAR', 'CHOP']:
                is_miss = True
                miss_type = 'DIRECTIONAL_MISS'
                miss_details['predicted'] = predicted
                miss_details['actual'] = actual
            
            # Check band breach (if actual close is available)
            elif actual_close > 0:
                bull_target = row.get('BULL_TARGET', 0)
                bear_target = row.get('BEAR_TARGET', 0)
                
                if predicted == 'CHOP':
                    chop_upper = row.get('CHOP_UPPER', 0)
                    chop_lower = row.get('CHOP_LOWER', 0)
                    
                    if actual_close > chop_upper or actual_close < chop_lower:
                        is_miss = True
                        miss_type = 'BAND_BREACH'
                        miss_details['breached_level'] = 'upper' if actual_close > chop_upper else 'lower'
                        miss_details['target'] = chop_upper if actual_close > chop_upper else chop_lower
                        miss_details['actual'] = actual_close
            
            # Check confidence vs accuracy
            confidence = row.get('CONFIDENCE', 0)
            if confidence > 0.85 and is_miss:
                miss_details['high_confidence_miss'] = True
            
            if is_miss:
                miss_record = {
                    'forecast_date': row['FORECAST_DATE'],
                    'miss_type': miss_type,
                    'confidence': confidence,
                    'miss_details': miss_details,
                    'bull_target': row.get('BULL_TARGET', 0),
                    'bear_target': row.get('BEAR_TARGET', 0),
                    'chop_upper': row.get('CHOP_UPPER', 0),
                    'chop_lower': row.get('CHOP_LOWER', 0),
                    'predicted_direction': predicted,
                    'actual_direction': actual,
                    'actual_close': actual_close
                }
                misses.append(miss_record)
        
        return pd.DataFrame(misses) if misses else pd.DataFrame()
    
    def categorize_miss_causes(self, miss_df: pd.DataFrame) -> List[Dict]:
        """Analyze and categorize the causes of forecast misses"""
        
        if miss_df.empty:
            return []
        
        categorized_misses = []
        
        for _, miss in miss_df.iterrows():
            forecast_date = miss['forecast_date']
            
            # Get correlations for this date
            correlations = self._get_correlations_for_date(forecast_date)
            
            # Get market data for context
            market_context = self._get_market_context(forecast_date)
            
            # Determine most likely cause
            primary_cause = self._determine_primary_cause(miss, correlations, market_context)
            
            categorized_miss = {
                'forecast_date': forecast_date,
                'miss_type': miss['miss_type'],
                'confidence': miss['confidence'],
                'predicted_direction': miss['predicted_direction'],
                'actual_direction': miss['actual_direction'],
                'primary_cause': primary_cause['category'],
                'cause_confidence': primary_cause['confidence'],
                'cause_details': primary_cause['details'],
                'correlations_found': len(correlations),
                'market_context': market_context,
                'miss_details': miss['miss_details'],
                'analysis_date': datetime.now()
            }
            
            categorized_misses.append(categorized_miss)
        
        return categorized_misses
    
    def _get_correlations_for_date(self, forecast_date: datetime) -> List[Dict]:
        """Get news correlations for a specific date"""
        
        # Try to load from correlation results
        correlation_files = []
        
        # Check for correlation files
        if os.path.exists("output/correlations"):
            import glob
            correlation_files = glob.glob("output/correlations/correlations_*.json")
        
        correlations = []
        
        for file_path in correlation_files:
            try:
                import json
                with open(file_path, 'r') as f:
                    file_correlations = json.load(f)
                
                # Filter for the forecast date (within 1 day)
                date_correlations = []
                for corr in file_correlations:
                    if 'news_time' in corr:
                        news_time = datetime.fromisoformat(corr['news_time'].replace('Z', '+00:00'))
                        if abs((news_time.date() - forecast_date.date()).days) <= 1:
                            date_correlations.append(corr)
                
                correlations.extend(date_correlations)
                
            except Exception as e:
                print(f"Error reading correlation file {file_path}: {e}")
        
        return correlations
    
    def _get_market_context(self, forecast_date: datetime) -> Dict:
        """Get market context for the forecast date"""
        
        # This would normally query market data
        # For now, return basic structure
        return {
            'vix_level': 'normal',  # Would calculate: low (<15), normal (15-25), high (>25)
            'market_regime': 'normal',  # calm, volatile, trending, choppy
            'volume_profile': 'average',  # light, average, heavy
            'correlation_regime': 'normal'  # normal, breakdown, extreme
        }
    
    def _determine_primary_cause(self, miss: pd.Series, correlations: List[Dict], market_context: Dict) -> Dict:
        """Determine the most likely cause of the forecast miss"""
        
        # Analyze correlations
        if correlations:
            # High correlation strength suggests news event
            strong_correlations = [c for c in correlations if c.get('correlation_strength', 0) > 0.8]
            
            if strong_correlations:
                strongest = max(strong_correlations, key=lambda x: x.get('correlation_strength', 0))
                
                # Categorize based on news source/category
                if 'fed' in strongest.get('news_source', '').lower():
                    return {
                        'category': 'MACRO_RELEASE',
                        'confidence': 0.9,
                        'details': f"Fed news: {strongest.get('news_title', '')[:100]}"
                    }
                elif strongest.get('news_category') == 'economic_data':
                    return {
                        'category': 'MACRO_RELEASE', 
                        'confidence': 0.85,
                        'details': f"Economic data: {strongest.get('news_title', '')[:100]}"
                    }
                else:
                    return {
                        'category': 'NEWS_EVENT',
                        'confidence': 0.8,
                        'details': f"News event: {strongest.get('news_title', '')[:100]}"
                    }
        
        # Check for technical factors
        miss_details = miss.get('miss_details', {})
        
        if miss_details.get('high_confidence_miss'):
            # High confidence miss suggests external shock
            return {
                'category': 'VOLATILITY_SHIFT',
                'confidence': 0.7,
                'details': 'High confidence forecast missed - possible regime change'
            }
        
        # Check miss type
        if miss.get('miss_type') == 'BAND_BREACH':
            return {
                'category': 'TECHNICAL_BREAK',
                'confidence': 0.6,
                'details': f"Price breached {miss_details.get('breached_level', 'unknown')} level"
            }
        
        # Default to unknown
        return {
            'category': 'UNKNOWN',
            'confidence': 0.3,
            'details': 'Insufficient data to determine cause'
        }
    
    def _generate_sample_forecasts(self) -> pd.DataFrame:
        """Generate sample forecast data for testing"""
        
        dates = pd.date_range(start=datetime.now() - timedelta(days=7), periods=7, freq='D')
        sample_data = []
        
        for i, date in enumerate(dates):
            # Mix of hits and misses
            predicted = ['BULL', 'CHOP', 'BEAR', 'BULL', 'CHOP', 'BULL', 'BEAR'][i]
            actual = ['BULL', 'BEAR', 'BEAR', 'BULL', 'CHOP', 'CHOP', 'BEAR'][i]  # Some misses
            
            sample_data.append({
                'FORECAST_DATE': date,
                'BULL_TARGET': 5500 + i*5,
                'BEAR_TARGET': 5400 + i*5,
                'CHOP_LOWER': 5440 + i*5,
                'CHOP_UPPER': 5460 + i*5,
                'CONFIDENCE': 0.85 + (i * 0.02),
                'PREDICTED_DIRECTION': predicted,
                'ACTUAL_DIRECTION': actual,
                'ACTUAL_CLOSE': 5450 + i*10,
                'FORECAST_ACCURACY': 1.0 if predicted == actual else 0.0,
                'CREATED_AT': datetime.now() - timedelta(days=7-i)
            })
        
        return pd.DataFrame(sample_data)
    
    def store_miss_analysis(self, categorized_misses: List[Dict]) -> bool:
        """Store miss analysis results"""
        
        if not categorized_misses:
            print("No misses to analyze")
            return True
        
        if SNOWFLAKE_AVAILABLE:
            try:
                conn = get_snowflake_connection()
                cursor = conn.cursor()
                
                # Create/update ZEN_AUDIT_LIBRARY table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS ZEN_AUDIT_LIBRARY (
                    AUDIT_ID VARCHAR(50) PRIMARY KEY,
                    FORECAST_DATE DATE,
                    MISS_TYPE VARCHAR(50),
                    CONFIDENCE DECIMAL(6,4),
                    PREDICTED_DIRECTION VARCHAR(10),
                    ACTUAL_DIRECTION VARCHAR(10),
                    PRIMARY_CAUSE VARCHAR(50),
                    CAUSE_CONFIDENCE DECIMAL(6,4),
                    CAUSE_DETAILS VARCHAR(2000),
                    CORRELATIONS_FOUND INTEGER,
                    MARKET_CONTEXT VARCHAR(1000),
                    ANALYSIS_DATE TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """)
                
                # Insert miss analyses
                for miss in categorized_misses:
                    audit_id = f"audit_{miss['forecast_date'].strftime('%Y%m%d')}_{miss['miss_type']}"
                    
                    cursor.execute("""
                    MERGE INTO ZEN_AUDIT_LIBRARY z
                    USING (SELECT %s as AUDIT_ID) s
                    ON z.AUDIT_ID = s.AUDIT_ID
                    WHEN NOT MATCHED THEN INSERT (
                        AUDIT_ID, FORECAST_DATE, MISS_TYPE, CONFIDENCE,
                        PREDICTED_DIRECTION, ACTUAL_DIRECTION, PRIMARY_CAUSE,
                        CAUSE_CONFIDENCE, CAUSE_DETAILS, CORRELATIONS_FOUND,
                        MARKET_CONTEXT, ANALYSIS_DATE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        audit_id, audit_id, miss['forecast_date'], miss['miss_type'],
                        miss['confidence'], miss['predicted_direction'], miss['actual_direction'],
                        miss['primary_cause'], miss['cause_confidence'], miss['cause_details'],
                        miss['correlations_found'], str(miss['market_context']), miss['analysis_date']
                    ))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                print(f"Stored {len(categorized_misses)} miss analyses to ZEN_AUDIT_LIBRARY")
                return True
                
            except Exception as e:
                print(f"Error storing miss analyses: {e}")
                return False
        else:
            # Fallback: Save to JSON
            import json
            
            os.makedirs("output/postmortem", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"output/postmortem/miss_analysis_{timestamp}.json"
            
            # Convert datetime objects for JSON
            json_misses = []
            for miss in categorized_misses:
                json_miss = miss.copy()
                for key, value in json_miss.items():
                    if isinstance(value, datetime):
                        json_miss[key] = value.isoformat()
                json_misses.append(json_miss)
            
            with open(filename, 'w') as f:
                json.dump(json_misses, f, indent=2)
            
            print(f"Stored {len(categorized_misses)} miss analyses to {filename}")
            return True
    
    def run_postmortem_analysis(self, days_back: int = 30) -> Dict:
        """Run complete post-mortem analysis"""
        
        print("=== POST-MORTEM LEARNING ANALYSIS ===")
        
        # Get forecast performance
        print(f"Loading forecast performance for last {days_back} days...")
        forecast_df = self.get_forecast_performance(days_back)
        print(f"Found {len(forecast_df)} completed forecasts")
        
        # Identify misses
        print("Identifying forecast misses...")
        miss_df = self.identify_misses(forecast_df)
        print(f"Found {len(miss_df)} forecast misses")
        
        if miss_df.empty:
            return {
                'success': True,
                'total_forecasts': len(forecast_df),
                'misses_found': 0,
                'accuracy_rate': 1.0 if len(forecast_df) > 0 else 0,
                'categorized_misses': []
            }
        
        # Categorize miss causes
        print("Categorizing miss causes...")
        categorized_misses = self.categorize_miss_causes(miss_df)
        
        # Store results
        success = self.store_miss_analysis(categorized_misses)
        
        # Calculate metrics
        total_forecasts = len(forecast_df)
        accuracy_rate = (total_forecasts - len(miss_df)) / total_forecasts if total_forecasts > 0 else 0
        
        # Display summary
        if categorized_misses:
            print("\\nMISS ANALYSIS SUMMARY:")
            cause_counts = {}
            for miss in categorized_misses:
                cause = miss['primary_cause']
                cause_counts[cause] = cause_counts.get(cause, 0) + 1
            
            for cause, count in sorted(cause_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"- {cause}: {count} occurrences")
        
        return {
            'success': success,
            'total_forecasts': total_forecasts,
            'misses_found': len(miss_df),
            'accuracy_rate': accuracy_rate,
            'categorized_misses': categorized_misses
        }

def main():
    """Main post-mortem analysis entry point"""
    
    analyzer = PostMortemAnalyzer()
    results = analyzer.run_postmortem_analysis(days_back=14)
    
    print("=== POST-MORTEM ANALYSIS COMPLETE ===")
    print(f"Success: {results['success']}")
    print(f"Total forecasts: {results['total_forecasts']}")
    print(f"Misses found: {results['misses_found']}")
    print(f"Accuracy rate: {results['accuracy_rate']:.1%}")
    
    return 0 if results['success'] else 1

if __name__ == "__main__":
    exit(main())
'''

        with open("src/postmortem_analyzer.py", "w", encoding='utf-8') as f:
            f.write(analyzer_code)
            
        print("Generated post-mortem analyzer: src/postmortem_analyzer.py")

def main():
    """Build post-mortem learning system"""
    print("=== POST-MORTEM LEARNING SYSTEM BUILDER ===")
    print("Building automated forecast miss analysis...")
    
    builder = PostMortemLearningBuilder()
    builder.create_miss_analyzer()
    
    print("=== POST-MORTEM LEARNING BUILD COMPLETE ===")
    print("Generated files:")
    print("- src/postmortem_analyzer.py (automated miss analysis)")
    
    print("\\nTo run post-mortem analysis:")
    print("python src/postmortem_analyzer.py")
    
    print("\\nSystem capabilities:")
    print("1. Identifies forecast misses (directional, band breaches)")
    print("2. Correlates misses with news events")
    print("3. Categorizes miss causes (NEWS_EVENT, MACRO_RELEASE, etc.)")
    print("4. Stores results in ZEN_AUDIT_LIBRARY for learning")
    print("5. Provides accuracy metrics and cause analysis")
    
    return 0

if __name__ == "__main__":
    exit(main())