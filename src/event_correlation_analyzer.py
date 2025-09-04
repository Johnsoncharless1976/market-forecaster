#!/usr/bin/env python3
"""
Event Correlation Analyzer
Analyzes correlation between news events and market movements
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Tuple, Optional

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from snowflake_conn import get_snowflake_connection
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    print("Warning: Snowflake connection not available")
    SNOWFLAKE_AVAILABLE = False

class EventCorrelationAnalyzer:
    def __init__(self):
        self.correlation_window_hours = 4  # Look for market moves within 4 hours of news
        
    def get_news_events(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Retrieve news events from database"""
        
        if not SNOWFLAKE_AVAILABLE:
            print("No database connection - using sample data")
            return self._generate_sample_news()
            
        try:
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                ARTICLE_ID,
                SOURCE,
                CATEGORY,
                TITLE,
                PUBLISHED_DATE,
                INGESTED_DATE,
                RELEVANCE_SCORE
            FROM NEWS_ARTICLES 
            WHERE PUBLISHED_DATE >= %s 
            AND PUBLISHED_DATE <= %s
            AND RELEVANCE_SCORE > 50
            ORDER BY PUBLISHED_DATE DESC
            """
            
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            if results:
                columns = ['ARTICLE_ID', 'SOURCE', 'CATEGORY', 'TITLE', 'PUBLISHED_DATE', 'INGESTED_DATE', 'RELEVANCE_SCORE']
                df = pd.DataFrame(results, columns=columns)
                df['PUBLISHED_DATE'] = pd.to_datetime(df['PUBLISHED_DATE'])
                df['INGESTED_DATE'] = pd.to_datetime(df['INGESTED_DATE'])
                return df
            else:
                print("No news events found in date range")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error retrieving news events: {e}")
            return pd.DataFrame()
    
    def get_market_moves(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Retrieve market price movements"""
        
        if not SNOWFLAKE_AVAILABLE:
            print("No database connection - using sample data")
            return self._generate_sample_market_data()
            
        try:
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            
            # Get hourly price data (if available) or daily data
            query = """
            SELECT 
                DATE,
                SYMBOL,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME
            FROM MARKET_OHLCV 
            WHERE DATE >= %s 
            AND DATE <= %s
            AND SYMBOL IN ('^GSPC', '^VIX')
            ORDER BY DATE, SYMBOL
            """
            
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            if results:
                columns = ['DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
                df = pd.DataFrame(results, columns=columns)
                df['DATE'] = pd.to_datetime(df['DATE'])
                
                # Calculate percentage moves
                for symbol in df['SYMBOL'].unique():
                    symbol_data = df[df['SYMBOL'] == symbol].copy()
                    symbol_data = symbol_data.sort_values('DATE')
                    symbol_data['PREV_CLOSE'] = symbol_data['CLOSE'].shift(1)
                    symbol_data['PCT_CHANGE'] = ((symbol_data['CLOSE'] - symbol_data['PREV_CLOSE']) / symbol_data['PREV_CLOSE'] * 100)
                    symbol_data['INTRADAY_RANGE'] = ((symbol_data['HIGH'] - symbol_data['LOW']) / symbol_data['OPEN'] * 100)
                    
                    df.loc[df['SYMBOL'] == symbol, 'PCT_CHANGE'] = symbol_data['PCT_CHANGE']
                    df.loc[df['SYMBOL'] == symbol, 'INTRADAY_RANGE'] = symbol_data['INTRADAY_RANGE']
                
                return df
            else:
                print("No market data found in date range")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error retrieving market data: {e}")
            return pd.DataFrame()
    
    def find_correlations(self, news_df: pd.DataFrame, market_df: pd.DataFrame) -> List[Dict]:
        """Find correlations between news events and market moves"""
        
        if news_df.empty or market_df.empty:
            return []
        
        correlations = []
        
        # Define significant move thresholds
        thresholds = {
            '^GSPC': {'significant_move': 1.0, 'large_move': 2.0},  # 1% and 2% moves
            '^VIX': {'significant_move': 10.0, 'large_move': 20.0}  # 10% and 20% moves
        }
        
        for _, news_event in news_df.iterrows():
            news_time = news_event['PUBLISHED_DATE']
            
            # Look for market moves within correlation window
            window_start = news_time - timedelta(hours=1)  # 1 hour before news
            window_end = news_time + timedelta(hours=self.correlation_window_hours)
            
            # Find market moves in this window
            for symbol in ['^GSPC', '^VIX']:
                symbol_data = market_df[
                    (market_df['SYMBOL'] == symbol) &
                    (market_df['DATE'] >= window_start) &
                    (market_df['DATE'] <= window_end)
                ].copy()
                
                if not symbol_data.empty:
                    for _, market_move in symbol_data.iterrows():
                        pct_change = abs(market_move.get('PCT_CHANGE', 0))
                        intraday_range = market_move.get('INTRADAY_RANGE', 0)
                        
                        # Check if move is significant
                        threshold = thresholds[symbol]
                        if pct_change >= threshold['significant_move'] or intraday_range >= threshold['significant_move']:
                            
                            # Determine correlation strength
                            correlation_strength = self._calculate_correlation_strength(
                                news_event, market_move, pct_change, intraday_range, threshold
                            )
                            
                            correlation = {
                                'news_id': news_event['ARTICLE_ID'],
                                'news_title': news_event['TITLE'][:100] + "..." if len(news_event['TITLE']) > 100 else news_event['TITLE'],
                                'news_source': news_event['SOURCE'],
                                'news_category': news_event['CATEGORY'],
                                'news_time': news_time,
                                'news_relevance': news_event['RELEVANCE_SCORE'],
                                'symbol': symbol,
                                'market_time': market_move['DATE'],
                                'time_diff_hours': (market_move['DATE'] - news_time).total_seconds() / 3600,
                                'pct_change': pct_change,
                                'intraday_range': intraday_range,
                                'move_direction': 'up' if market_move.get('PCT_CHANGE', 0) > 0 else 'down',
                                'correlation_strength': correlation_strength,
                                'market_open': market_move['OPEN'],
                                'market_close': market_move['CLOSE'],
                                'market_high': market_move['HIGH'],
                                'market_low': market_move['LOW']
                            }
                            
                            correlations.append(correlation)
        
        # Sort by correlation strength
        correlations.sort(key=lambda x: x['correlation_strength'], reverse=True)
        return correlations
    
    def _calculate_correlation_strength(self, news_event, market_move, pct_change, intraday_range, threshold):
        """Calculate correlation strength score"""
        
        # Base score from move magnitude
        move_score = min((pct_change / threshold['significant_move']) * 0.4, 1.0)
        range_score = min((intraday_range / threshold['significant_move']) * 0.3, 1.0)
        
        # News relevance factor
        relevance_score = (news_event['RELEVANCE_SCORE'] / 100) * 0.2
        
        # Time proximity factor (closer = higher score)
        time_diff = abs((market_move['DATE'] - news_event['PUBLISHED_DATE']).total_seconds() / 3600)
        time_score = max(0, (self.correlation_window_hours - time_diff) / self.correlation_window_hours) * 0.1
        
        return move_score + range_score + relevance_score + time_score
    
    def _generate_sample_news(self) -> pd.DataFrame:
        """Generate sample news data for testing"""
        
        sample_data = [
            {
                'ARTICLE_ID': 'news_001',
                'SOURCE': 'fed_news',
                'CATEGORY': 'macro_policy',
                'TITLE': 'Fed Officials Signal Potential Rate Change',
                'PUBLISHED_DATE': datetime.now() - timedelta(days=1),
                'INGESTED_DATE': datetime.now() - timedelta(days=1),
                'RELEVANCE_SCORE': 95
            },
            {
                'ARTICLE_ID': 'news_002',
                'SOURCE': 'cnbc_economy',
                'CATEGORY': 'economic_data',
                'TITLE': 'Jobs Report Shows Unexpected Strength',
                'PUBLISHED_DATE': datetime.now() - timedelta(days=2),
                'INGESTED_DATE': datetime.now() - timedelta(days=2),
                'RELEVANCE_SCORE': 85
            }
        ]
        
        return pd.DataFrame(sample_data)
    
    def _generate_sample_market_data(self) -> pd.DataFrame:
        """Generate sample market data for testing"""
        
        dates = pd.date_range(start=datetime.now() - timedelta(days=3), periods=3, freq='D')
        sample_data = []
        
        for i, date in enumerate(dates):
            # SPX data
            spx_close = 5500 + i * 10
            spx_open = spx_close - 5
            sample_data.append({
                'DATE': date,
                'SYMBOL': '^GSPC',
                'OPEN': spx_open,
                'HIGH': spx_close + 15,
                'LOW': spx_close - 20,
                'CLOSE': spx_close,
                'VOLUME': 1000000,
                'PCT_CHANGE': (spx_close - spx_open) / spx_open * 100,
                'INTRADAY_RANGE': 35 / spx_open * 100
            })
            
            # VIX data
            vix_close = 18 - i * 0.5
            vix_open = vix_close + 1
            sample_data.append({
                'DATE': date,
                'SYMBOL': '^VIX',
                'OPEN': vix_open,
                'HIGH': vix_close + 2,
                'LOW': vix_close - 1,
                'CLOSE': vix_close,
                'VOLUME': 500000,
                'PCT_CHANGE': (vix_close - vix_open) / vix_open * 100,
                'INTRADAY_RANGE': 3 / vix_open * 100
            })
        
        return pd.DataFrame(sample_data)
    
    def store_correlations(self, correlations: List[Dict]) -> bool:
        """Store correlation analysis results"""
        
        if not correlations:
            print("No correlations to store")
            return True
        
        if SNOWFLAKE_AVAILABLE:
            try:
                conn = get_snowflake_connection()
                cursor = conn.cursor()
                
                # Create correlation table if not exists
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS NEWS_MARKET_CORRELATIONS (
                    CORRELATION_ID VARCHAR(50) PRIMARY KEY,
                    NEWS_ID VARCHAR(50),
                    NEWS_TITLE VARCHAR(1000),
                    NEWS_SOURCE VARCHAR(100),
                    NEWS_CATEGORY VARCHAR(100),
                    NEWS_TIME TIMESTAMP_NTZ,
                    SYMBOL VARCHAR(20),
                    MARKET_TIME TIMESTAMP_NTZ,
                    TIME_DIFF_HOURS DECIMAL(6,2),
                    PCT_CHANGE DECIMAL(8,4),
                    INTRADAY_RANGE DECIMAL(8,4),
                    MOVE_DIRECTION VARCHAR(10),
                    CORRELATION_STRENGTH DECIMAL(6,4),
                    ANALYSIS_DATE TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """)
                
                # Insert correlations
                for correlation in correlations:
                    correlation_id = f"{correlation['news_id']}_{correlation['symbol']}_{correlation['market_time'].strftime('%Y%m%d_%H%M')}"
                    
                    cursor.execute("""
                    MERGE INTO NEWS_MARKET_CORRELATIONS c
                    USING (SELECT %s as CORRELATION_ID) s
                    ON c.CORRELATION_ID = s.CORRELATION_ID
                    WHEN NOT MATCHED THEN INSERT (
                        CORRELATION_ID, NEWS_ID, NEWS_TITLE, NEWS_SOURCE, NEWS_CATEGORY,
                        NEWS_TIME, SYMBOL, MARKET_TIME, TIME_DIFF_HOURS, PCT_CHANGE,
                        INTRADAY_RANGE, MOVE_DIRECTION, CORRELATION_STRENGTH, ANALYSIS_DATE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        correlation_id, correlation_id, correlation['news_id'],
                        correlation['news_title'], correlation['news_source'], correlation['news_category'],
                        correlation['news_time'], correlation['symbol'], correlation['market_time'],
                        correlation['time_diff_hours'], correlation['pct_change'],
                        correlation['intraday_range'], correlation['move_direction'],
                        correlation['correlation_strength'], datetime.now()
                    ))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                print(f"Stored {len(correlations)} correlations to database")
                return True
                
            except Exception as e:
                print(f"Error storing correlations: {e}")
                return False
        else:
            # Fallback: Save to JSON file
            import json
            
            # Convert datetime objects to strings for JSON serialization
            json_correlations = []
            for corr in correlations:
                json_corr = corr.copy()
                for key, value in json_corr.items():
                    if isinstance(value, datetime):
                        json_corr[key] = value.isoformat()
                json_correlations.append(json_corr)
            
            os.makedirs("output/correlations", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"output/correlations/correlations_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(json_correlations, f, indent=2)
            
            print(f"Stored {len(correlations)} correlations to {filename}")
            return True
    
    def run_analysis(self, days_back: int = 7) -> Dict:
        """Run complete correlation analysis"""
        
        print("=== EVENT CORRELATION ANALYSIS ===")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"Analyzing period: {start_date.date()} to {end_date.date()}")
        
        # Get data
        print("Loading news events...")
        news_df = self.get_news_events(start_date, end_date)
        print(f"Found {len(news_df)} news events")
        
        print("Loading market data...")
        market_df = self.get_market_moves(start_date, end_date)
        print(f"Found {len(market_df)} market data points")
        
        # Find correlations
        print("Analyzing correlations...")
        correlations = self.find_correlations(news_df, market_df)
        print(f"Found {len(correlations)} potential correlations")
        
        # Store results
        if correlations:
            success = self.store_correlations(correlations)
            
            # Display top correlations
            print("\nTOP CORRELATIONS:")
            for i, corr in enumerate(correlations[:5]):
                print(f"{i+1}. {corr['news_source']} - {corr['news_title']}")
                print(f"   {corr['symbol']}: {corr['pct_change']:.2f}% move, {corr['time_diff_hours']:.1f}h after news")
                print(f"   Correlation strength: {corr['correlation_strength']:.3f}")
                print()
        else:
            print("No significant correlations found")
            success = True
        
        return {
            'success': success,
            'correlations_found': len(correlations),
            'news_events': len(news_df),
            'market_data_points': len(market_df),
            'analysis_period': f"{start_date.date()} to {end_date.date()}"
        }

def main():
    """Main analysis entry point"""
    
    analyzer = EventCorrelationAnalyzer()
    
    # Run analysis for past 7 days
    results = analyzer.run_analysis(days_back=7)
    
    print("=== ANALYSIS COMPLETE ===")
    print(f"Success: {results['success']}")
    print(f"Correlations found: {results['correlations_found']}")
    print(f"Period analyzed: {results['analysis_period']}")
    
    return 0 if results['success'] else 1

if __name__ == "__main__":
    exit(main())
