# zen_council_news_integration.py
#!/usr/bin/env python3
"""
Zen Council News Integration System
Enhances 67.9% mathematical baseline with news attribution analysis
Target: Bridge toward 88% accuracy by weighting Council signals with breaking news
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
import os
import json
import requests
import feedparser
from collections import defaultdict
import re

class ZenCouncilNewsIntegration:
    def __init__(self):
        self.news_sources = {
            'high_impact': [
                'https://feeds.a.dj.com/rss/RSSMarketsMain.xml',  # WSJ Markets
                'https://www.federalreserve.gov/feeds/press_all.xml',  # Fed announcements
                'https://feeds.reuters.com/reuters/businessNews'  # Reuters Business
            ],
            'medium_impact': [
                'https://feeds.finance.yahoo.com/rss/2.0/headline',  # Yahoo Finance
                'https://www.cnbc.com/id/100003114/device/rss/rss.html',  # CNBC Markets
                'https://feeds.bloomberg.com/markets/news.rss'  # Bloomberg Markets
            ],
            'sentiment_tracking': [
                'https://feeds.marketwatch.com/marketwatch/marketpulse/',  # MarketWatch
                'https://seekingalpha.com/api/sa/combined/DJIA.xml',  # Seeking Alpha
                'https://www.zerohedge.com/fullrss2.xml'  # ZeroHedge
            ]
        }
        
        self.market_keywords = {
            'bullish': ['rally', 'surge', 'gains', 'optimism', 'breakthrough', 'positive', 'upbeat', 'strong', 'beat', 'exceed'],
            'bearish': ['crash', 'decline', 'falls', 'concerns', 'warning', 'negative', 'weak', 'miss', 'disappointing', 'uncertainty'],
            'volatility': ['volatility', 'swing', 'uncertainty', 'instability', 'fluctuation', 'choppy'],
            'fed_related': ['federal reserve', 'fed', 'interest rates', 'monetary policy', 'powell', 'fomc'],
            'earnings': ['earnings', 'quarterly', 'guidance', 'revenue', 'profit'],
            'geopolitical': ['trade', 'china', 'tariff', 'sanctions', 'election', 'politics']
        }
        
    def connect_to_snowflake(self):
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        if not account:
            try:
                with open('.env', 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            if key == 'SNOWFLAKE_ACCOUNT':
                                account = value.strip('"\'')
                                break
            except FileNotFoundError:
                pass
        
        return snowflake.connector.connect(
            account=account or os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'ZEN_MARKET'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'FORECASTING'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
    
    def fetch_recent_news(self, hours_back: int = 24) -> list:
        """Fetch news from all RSS sources within specified timeframe"""
        news_items = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        print(f"Fetching news from last {hours_back} hours...")
        
        for impact_level, feeds in self.news_sources.items():
            for feed_url in feeds:
                try:
                    feed = feedparser.parse(feed_url)
                    for entry in feed.entries:
                        # Parse publication date
                        pub_date = datetime.now()  # Default fallback
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            pub_date = datetime(*entry.published_parsed[:6])
                        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                            pub_date = datetime(*entry.updated_parsed[:6])
                        
                        # Only include recent news
                        if pub_date >= cutoff_time:
                            news_items.append({
                                'source': feed_url,
                                'impact_level': impact_level,
                                'title': entry.get('title', ''),
                                'description': entry.get('description', ''),
                                'published': pub_date,
                                'link': entry.get('link', '')
                            })
                            
                except Exception as e:
                    print(f"Error fetching from {feed_url}: {e}")
                    continue
        
        print(f"Fetched {len(news_items)} recent news items")
        return news_items
    
    def analyze_news_sentiment(self, news_items: list) -> dict:
        """Analyze news sentiment and categorize market impact"""
        sentiment_analysis = {
            'bullish_score': 0,
            'bearish_score': 0,
            'volatility_score': 0,
            'fed_impact': 0,
            'earnings_impact': 0,
            'geopolitical_impact': 0,
            'high_impact_count': 0,
            'total_articles': len(news_items)
        }
        
        for item in news_items:
            text = (item['title'] + ' ' + item['description']).lower()
            
            # Weight by impact level
            weight = 3 if item['impact_level'] == 'high_impact' else 2 if item['impact_level'] == 'medium_impact' else 1
            
            if item['impact_level'] == 'high_impact':
                sentiment_analysis['high_impact_count'] += 1
            
            # Keyword scoring
            for category, keywords in self.market_keywords.items():
                keyword_count = sum(text.count(keyword) for keyword in keywords)
                
                if category == 'bullish':
                    sentiment_analysis['bullish_score'] += keyword_count * weight
                elif category == 'bearish':
                    sentiment_analysis['bearish_score'] += keyword_count * weight
                elif category == 'volatility':
                    sentiment_analysis['volatility_score'] += keyword_count * weight
                elif category == 'fed_related':
                    sentiment_analysis['fed_impact'] += keyword_count * weight
                elif category == 'earnings':
                    sentiment_analysis['earnings_impact'] += keyword_count * weight
                elif category == 'geopolitical':
                    sentiment_analysis['geopolitical_impact'] += keyword_count * weight
        
        # Normalize scores
        if sentiment_analysis['total_articles'] > 0:
            for key in ['bullish_score', 'bearish_score', 'volatility_score']:
                sentiment_analysis[key] = sentiment_analysis[key] / sentiment_analysis['total_articles']
        
        return sentiment_analysis
    
    def calculate_news_council_weights(self, sentiment_analysis: dict) -> dict:
        """Calculate how news should weight Council signals"""
        weights = {
            'bull_signal_modifier': 1.0,
            'bear_signal_modifier': 1.0,
            'chop_signal_modifier': 1.0,
            'confidence_boost': 0,
            'news_override': None
        }
        
        bullish_net = sentiment_analysis['bullish_score'] - sentiment_analysis['bearish_score']
        volatility = sentiment_analysis['volatility_score']
        
        # High impact news override logic
        if sentiment_analysis['high_impact_count'] >= 3:
            if bullish_net > 2:
                weights['news_override'] = 'STRONG_BULL'
                weights['bull_signal_modifier'] = 1.5
                weights['confidence_boost'] = 2
            elif bullish_net < -2:
                weights['news_override'] = 'STRONG_BEAR'  
                weights['bear_signal_modifier'] = 1.5
                weights['confidence_boost'] = 2
        
        # Fed impact amplification
        if sentiment_analysis['fed_impact'] > 1:
            if bullish_net > 0:
                weights['bull_signal_modifier'] += 0.3
            else:
                weights['bear_signal_modifier'] += 0.3
            weights['confidence_boost'] += 1
        
        # Volatility impact
        if volatility > 1:
            weights['chop_signal_modifier'] += 0.2
            weights['bull_signal_modifier'] -= 0.1
            weights['bear_signal_modifier'] -= 0.1
        
        # Earnings season impact
        if sentiment_analysis['earnings_impact'] > 1:
            weights['volatility_expectation'] = True
            weights['chop_signal_modifier'] += 0.1
        
        return weights
    
    def enhance_council_forecast(self, base_forecast: dict, news_weights: dict) -> dict:
        """Enhance Council forecast with news attribution"""
        enhanced_forecast = base_forecast.copy()
        
        # Apply news weighting to signal counts
        enhanced_bull = int(base_forecast['bull_signals'] * news_weights['bull_signal_modifier'])
        enhanced_bear = int(base_forecast['bear_signals'] * news_weights['bear_signal_modifier'])  
        enhanced_chop = int(base_forecast['chop_signals'] * news_weights['chop_signal_modifier'])
        
        # News override logic
        if news_weights.get('news_override') == 'STRONG_BULL':
            enhanced_forecast['forecast_bias'] = 'Bull'
            enhanced_forecast['news_attribution'] = 'High-impact bullish news override'
        elif news_weights.get('news_override') == 'STRONG_BEAR':
            enhanced_forecast['forecast_bias'] = 'Bear'  
            enhanced_forecast['news_attribution'] = 'High-impact bearish news override'
        else:
            # Re-evaluate with enhanced signals
            if enhanced_bull >= 3 and enhanced_bull > enhanced_bear:
                enhanced_forecast['forecast_bias'] = 'Bull'
            elif enhanced_bear >= 3 and enhanced_bear > enhanced_bull:
                enhanced_forecast['forecast_bias'] = 'Bear'
            elif enhanced_chop >= 3:
                enhanced_forecast['forecast_bias'] = 'Chop'
            
            enhanced_forecast['news_attribution'] = 'News-weighted technical analysis'
        
        # Update confidence with news boost
        base_confidence = max(base_forecast['bull_signals'], base_forecast['bear_signals'], base_forecast['chop_signals'])
        enhanced_confidence = min(6, base_confidence + news_weights['confidence_boost'])
        
        enhanced_forecast.update({
            'enhanced_bull_signals': enhanced_bull,
            'enhanced_bear_signals': enhanced_bear,
            'enhanced_chop_signals': enhanced_chop,
            'enhanced_confidence': enhanced_confidence,
            'news_bull_modifier': round(news_weights['bull_signal_modifier'], 2),
            'news_bear_modifier': round(news_weights['bear_signal_modifier'], 2),
            'news_chop_modifier': round(news_weights['chop_signal_modifier'], 2)
        })
        
        return enhanced_forecast
    
    def save_news_analysis_to_database(self, sentiment_analysis: dict, news_weights: dict):
        """Save news analysis results to Snowflake"""
        try:
            conn = self.connect_to_snowflake()
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO ZEN_MARKET.FORECASTING.NEWS_ANALYSIS
            (TIMESTAMP, BULLISH_SCORE, BEARISH_SCORE, VOLATILITY_SCORE, FED_IMPACT,
             EARNINGS_IMPACT, GEOPOLITICAL_IMPACT, HIGH_IMPACT_COUNT, TOTAL_ARTICLES,
             BULL_MODIFIER, BEAR_MODIFIER, CHOP_MODIFIER, CONFIDENCE_BOOST, NEWS_OVERRIDE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                datetime.now().isoformat(),
                float(sentiment_analysis['bullish_score']),
                float(sentiment_analysis['bearish_score']),
                float(sentiment_analysis['volatility_score']),
                int(sentiment_analysis['fed_impact']),
                int(sentiment_analysis['earnings_impact']),
                int(sentiment_analysis['geopolitical_impact']),
                int(sentiment_analysis['high_impact_count']),
                int(sentiment_analysis['total_articles']),
                float(news_weights['bull_signal_modifier']),
                float(news_weights['bear_signal_modifier']),
                float(news_weights['chop_signal_modifier']),
                int(news_weights['confidence_boost']),
                str(news_weights.get('news_override', ''))
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print("News analysis saved to database")
            
        except Exception as e:
            print(f"Failed to save news analysis: {e}")
    
    def run_news_integration_analysis(self) -> dict:
        """Complete news integration analysis pipeline"""
        
        print("ZEN COUNCIL NEWS INTEGRATION ANALYSIS")
        print("=" * 55)
        
        # Fetch recent news
        news_items = self.fetch_recent_news(24)
        
        if not news_items:
            print("No recent news found - returning neutral weights")
            return {
                'bull_signal_modifier': 1.0,
                'bear_signal_modifier': 1.0,
                'chop_signal_modifier': 1.0,
                'confidence_boost': 0,
                'news_override': None
            }
        
        # Analyze sentiment
        sentiment_analysis = self.analyze_news_sentiment(news_items)
        
        print(f"News Sentiment Analysis:")
        print(f"  Articles analyzed: {sentiment_analysis['total_articles']}")
        print(f"  High impact articles: {sentiment_analysis['high_impact_count']}")
        print(f"  Bullish score: {sentiment_analysis['bullish_score']:.2f}")
        print(f"  Bearish score: {sentiment_analysis['bearish_score']:.2f}")
        print(f"  Volatility score: {sentiment_analysis['volatility_score']:.2f}")
        print(f"  Fed impact: {sentiment_analysis['fed_impact']}")
        
        # Calculate weights
        news_weights = self.calculate_news_council_weights(sentiment_analysis)
        
        print(f"\nNews Council Weighting:")
        print(f"  Bull signal modifier: {news_weights['bull_signal_modifier']:.2f}")
        print(f"  Bear signal modifier: {news_weights['bear_signal_modifier']:.2f}")
        print(f"  Chop signal modifier: {news_weights['chop_signal_modifier']:.2f}")
        print(f"  Confidence boost: +{news_weights['confidence_boost']}")
        if news_weights['news_override']:
            print(f"  News override: {news_weights['news_override']}")
        
        # Save to database
        self.save_news_analysis_to_database(sentiment_analysis, news_weights)
        
        return news_weights

if __name__ == "__main__":
    print("Assembling Zen Council News Integration...")
    news_integrator = ZenCouncilNewsIntegration()
    
    news_weights = news_integrator.run_news_integration_analysis()
    
    print(f"\nNews integration analysis complete!")
    print("Ready to enhance Council forecasts with news attribution")