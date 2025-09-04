#!/usr/bin/env python3
"""
News Attribution Weighting System
Prevents false attribution by weighting news impact against market timing and magnitude
"""

import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Tuple
import re

class NewsAttributionWeighting:
    def __init__(self):
        # Source credibility weights (0-1.0)
        self.source_credibility = {
            # TIER 1 - OFFICIAL/PRIMARY SOURCES (1.0)
            "fed_news": 1.0,
            "sec_news": 1.0, 
            "treasury_releases": 1.0,
            "usgs_earthquakes": 1.0,
            "hurricane_center": 1.0,
            
            # TIER 2 - ESTABLISHED FINANCIAL MEDIA (0.8-0.9)
            "wsj_markets": 0.9,
            "reuters_business": 0.9,
            "bloomberg_economics": 0.9,
            "financial_times": 0.8,
            
            # TIER 3 - MAINSTREAM FINANCIAL (0.6-0.7)
            "cnbc_economy": 0.7,
            "marketwatch_breaking": 0.6,
            "seeking_alpha": 0.6,
            
            # TIER 4 - ALTERNATIVE/OPINION (0.4-0.5)
            "zerohedge": 0.4,
            "wolf_street": 0.5,
            "naked_capitalism": 0.4,
            
            # TIER 5 - SPECIALIZED/NICHE (0.6-0.8)
            "defense_news": 0.7,
            "energy_news": 0.6,
            "trade_gov": 0.8
        }
        
        # Market move magnitude thresholds
        self.magnitude_thresholds = {
            "EXTREME": 3.0,    # >3% SPX move
            "MAJOR": 2.0,      # 2-3% SPX move
            "SIGNIFICANT": 1.0, # 1-2% SPX move
            "MODERATE": 0.5,   # 0.5-1% SPX move
            "MINOR": 0.0       # <0.5% SPX move
        }
        
        # Time decay factors (how news impact decreases over time)
        self.time_decay = {
            "IMMEDIATE": 1.0,   # 0-2 hours
            "SAME_DAY": 0.8,    # 2-8 hours
            "NEXT_DAY": 0.5,    # 8-24 hours
            "STALE": 0.2        # >24 hours
        }
        
        # Event type likelihood of causing market moves
        self.event_market_probability = {
            "MONETARY_POLICY": 0.95,      # Fed moves almost always market-moving
            "GEOPOLITICAL": 0.7,          # Wars/conflicts often move markets
            "NATURAL_DISASTERS": 0.6,     # Depends on location/severity
            "CORPORATE_CRISIS": 0.5,      # Depends on company size
            "MACRO_DATA": 0.8,            # Economic data usually matters
            "ENERGY": 0.7,                # Oil/gas events often significant
            "BANKING": 0.8,               # Financial sector very sensitive
            "TECH_DISRUPTION": 0.4,       # Often overhyped
            "MARKET_STRUCTURE": 0.6,      # Regulatory changes matter
            "TRADE": 0.8,                 # Trade wars/deals highly impactful
            "CYBER_WARFARE": 0.3,         # Often initial panic, then recovery
            "PANDEMIC": 0.9,              # Health crises extremely impactful
            "CURRENCY_CRISIS": 0.85       # Currency events highly correlated
        }

    def calculate_timing_weight(self, news_timestamp: datetime, market_move_timestamp: datetime) -> float:
        """Calculate how much news timing correlates with market move"""
        time_diff = abs((market_move_timestamp - news_timestamp).total_seconds() / 3600)  # Hours
        
        if time_diff <= 2:
            return self.time_decay["IMMEDIATE"]
        elif time_diff <= 8:
            return self.time_decay["SAME_DAY"] 
        elif time_diff <= 24:
            return self.time_decay["NEXT_DAY"]
        else:
            return self.time_decay["STALE"]

    def calculate_magnitude_requirement(self, market_move_pct: float) -> str:
        """Determine what level of news would justify this market move"""
        abs_move = abs(market_move_pct)
        
        if abs_move >= 3.0:
            return "EXTREME"
        elif abs_move >= 2.0:
            return "MAJOR"
        elif abs_move >= 1.0:
            return "SIGNIFICANT"
        elif abs_move >= 0.5:
            return "MODERATE"
        else:
            return "MINOR"

    def calculate_news_attribution_score(self, news_item: Dict, market_move: Dict) -> float:
        """
        Calculate final attribution score for a news item explaining a market move
        Returns 0-100 score indicating confidence this news caused the move
        """
        
        # Base impact score from content analysis (0-50 from your existing system)
        content_score = news_item.get('impact_score', 0)
        
        # Source credibility multiplier
        source_name = news_item.get('source', 'unknown')
        credibility = self.source_credibility.get(source_name, 0.3)  # Default low credibility
        
        # Event type probability of market impact
        categories = news_item.get('categories', ['GENERAL_MARKET'])
        max_probability = max([self.event_market_probability.get(cat, 0.3) for cat in categories])
        
        # Timing correlation
        news_time = news_item.get('timestamp')
        move_time = market_move.get('timestamp')
        timing_weight = self.calculate_timing_weight(news_time, move_time)
        
        # Market move magnitude vs news severity matching
        market_move_pct = market_move.get('percentage_change', 0)
        required_magnitude = self.calculate_magnitude_requirement(market_move_pct)
        
        # Magnitude matching score
        magnitude_match = 1.0
        if required_magnitude == "EXTREME" and content_score < 20:
            magnitude_match = 0.3  # Unlikely small news caused huge move
        elif required_magnitude == "MINOR" and content_score > 30:
            magnitude_match = 0.5  # Overkill - big news for small move
        
        # Calculate final weighted score
        final_score = (
            content_score *           # Base news impact (0-50)
            credibility *            # Source credibility (0-1.0)
            max_probability *        # Event type likelihood (0-1.0)  
            timing_weight *          # Timing correlation (0-1.0)
            magnitude_match          # Magnitude matching (0-1.0)
        )
        
        # Cap at 100 and add confidence threshold
        final_score = min(final_score, 100)
        
        return {
            'attribution_score': round(final_score, 2),
            'confidence_level': self._get_confidence_level(final_score),
            'components': {
                'content_impact': content_score,
                'source_credibility': credibility,
                'event_probability': max_probability,
                'timing_correlation': timing_weight,
                'magnitude_match': magnitude_match
            }
        }

    def _get_confidence_level(self, score: float) -> str:
        """Convert numeric score to confidence level"""
        if score >= 70:
            return "HIGH_CONFIDENCE"
        elif score >= 40:
            return "MODERATE_CONFIDENCE"
        elif score >= 20:
            return "LOW_CONFIDENCE"
        else:
            return "UNLIKELY_ATTRIBUTION"

    def rank_news_attribution(self, news_items: List[Dict], market_move: Dict) -> List[Dict]:
        """
        Rank all news items by their likelihood of causing the market move
        Returns sorted list with attribution scores
        """
        attributed_news = []
        
        for news_item in news_items:
            attribution = self.calculate_news_attribution_score(news_item, market_move)
            
            news_with_attribution = {
                **news_item,
                **attribution
            }
            attributed_news.append(news_with_attribution)
        
        # Sort by attribution score (highest first)
        attributed_news.sort(key=lambda x: x['attribution_score'], reverse=True)
        
        return attributed_news

    def generate_attribution_analysis(self, top_news: Dict, market_move: Dict) -> str:
        """Generate human-readable attribution analysis"""
        
        score = top_news['attribution_score']
        confidence = top_news['confidence_level']
        title = top_news.get('title', 'Unknown headline')
        move_pct = market_move.get('percentage_change', 0)
        
        if confidence == "HIGH_CONFIDENCE":
            return f"HIGH CONFIDENCE: Market moved {move_pct:.2f}% likely due to '{title}' (Attribution Score: {score})"
        
        elif confidence == "MODERATE_CONFIDENCE":
            return f"MODERATE CONFIDENCE: Market moved {move_pct:.2f}% possibly due to '{title}' (Attribution Score: {score})"
        
        elif confidence == "LOW_CONFIDENCE":
            return f"LOW CONFIDENCE: Market moved {move_pct:.2f}% with weak correlation to '{title}' (Attribution Score: {score})"
        
        else:
            return f"UNCLEAR ATTRIBUTION: Market moved {move_pct:.2f}% but no clear news catalyst identified (Highest Score: {score})"

# Example usage for testing
if __name__ == "__main__":
    weighter = NewsAttributionWeighting()
    
    # Test case: Major market move with corresponding news
    market_move = {
        'timestamp': datetime(2025, 9, 3, 14, 30),  # 2:30 PM
        'percentage_change': -2.1,  # Major down move
        'symbol': 'SPX'
    }
    
    sample_news = [
        {
            'title': 'Fed Chair Powell Signals Emergency Rate Hike',
            'impact_score': 45,
            'categories': ['MONETARY_POLICY'],
            'source': 'fed_news',
            'timestamp': datetime(2025, 9, 3, 14, 15)  # 15 minutes before move
        },
        {
            'title': 'Celebrity Tweets About Stock Market',
            'impact_score': 8,
            'categories': ['GENERAL_MARKET'],
            'source': 'zerohedge',
            'timestamp': datetime(2025, 9, 3, 14, 25)  # 5 minutes before move
        }
    ]
    
    attributed_news = weighter.rank_news_attribution(sample_news, market_move)
    
    print("NEWS ATTRIBUTION ANALYSIS:")
    print("=" * 50)
    for news in attributed_news:
        analysis = weighter.generate_attribution_analysis(news, market_move)
        print(analysis)
        print(f"Components: {news['components']}")
        print()