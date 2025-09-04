#!/usr/bin/env python3
"""
News Feed Discovery and Testing System
Tests RSS feeds for financial news quality and reliability
"""

import feedparser
import requests
from datetime import datetime, timedelta
import pandas as pd
import re
from typing import Dict, List, Tuple
import time

class NewsSourceTester:
    def __init__(self):
        # Candidate RSS feeds for financial news
        self.rss_feeds = {
            "yahoo_finance": "https://feeds.finance.yahoo.com/rss/2.0/headline",
            "marketwatch_breaking": "http://feeds.marketwatch.com/marketwatch/realtimeheadlines/",
            "marketwatch_markets": "http://feeds.marketwatch.com/marketwatch/marketpulse/",
            "cnbc_markets": "https://www.cnbc.com/id/100727362/device/rss/rss.html",
            "cnbc_economy": "https://www.cnbc.com/id/20910258/device/rss/rss.html",
            "reuters_markets": "https://feeds.reuters.com/reuters/businessNews",
            "fed_news": "https://www.federalreserve.gov/feeds/press_all.xml",
            "treasury_releases": "https://home.treasury.gov/rss/press-releases",
            "seeking_alpha": "https://seekingalpha.com/market_currents.xml",
            "bloomberg_markets": "https://feeds.bloomberg.com/markets/news.rss"
        }
        
        # Keywords that indicate market-moving news
        self.market_keywords = [
            "fed", "federal reserve", "interest rate", "inflation", "cpi", "ppi",
            "employment", "jobless", "unemployment", "gdp", "recession",
            "s&p", "dow", "nasdaq", "vix", "volatility", "earnings",
            "fomc", "jerome powell", "treasury", "yield", "dollar"
        ]
        
    def test_feed_accessibility(self, feed_name: str, url: str) -> Dict:
        """Test if RSS feed is accessible and parse basic info"""
        try:
            print(f"Testing {feed_name}: {url}")
            
            # Try to fetch with timeout
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            if response.status_code != 200:
                return {
                    "name": feed_name,
                    "status": "FAILED",
                    "error": f"HTTP {response.status_code}",
                    "entries": 0,
                    "latest_date": None,
                    "market_relevance": 0
                }
            
            # Parse RSS feed
            feed = feedparser.parse(response.content)
            
            if feed.bozo:
                return {
                    "name": feed_name,
                    "status": "FAILED",
                    "error": "Invalid RSS format",
                    "entries": 0,
                    "latest_date": None,
                    "market_relevance": 0
                }
            
            # Analyze entries
            entries = feed.entries[:10]  # Test first 10 entries
            market_relevant_count = 0
            latest_date = None
            
            for entry in entries:
                # Check for market relevance
                title_text = entry.get('title', '').lower()
                summary_text = entry.get('summary', '').lower()
                combined_text = f"{title_text} {summary_text}"
                
                if any(keyword in combined_text for keyword in self.market_keywords):
                    market_relevant_count += 1
                
                # Get latest publish date
                if 'published_parsed' in entry:
                    entry_date = datetime(*entry.published_parsed[:6])
                    if latest_date is None or entry_date > latest_date:
                        latest_date = entry_date
            
            market_relevance = (market_relevant_count / len(entries) * 100) if entries else 0
            
            return {
                "name": feed_name,
                "status": "SUCCESS",
                "error": None,
                "entries": len(entries),
                "latest_date": latest_date,
                "market_relevance": round(market_relevance, 1),
                "sample_titles": [entry.get('title', '')[:100] for entry in entries[:3]]
            }
            
        except Exception as e:
            return {
                "name": feed_name,
                "status": "FAILED", 
                "error": str(e),
                "entries": 0,
                "latest_date": None,
                "market_relevance": 0
            }
    
    def test_all_feeds(self) -> pd.DataFrame:
        """Test all RSS feeds and return results"""
        results = []
        
        for feed_name, url in self.rss_feeds.items():
            result = self.test_feed_accessibility(feed_name, url)
            results.append(result)
            
            # Be respectful with requests
            time.sleep(1)
        
        return pd.DataFrame(results)
    
    def generate_report(self, results_df: pd.DataFrame) -> str:
        """Generate a comprehensive report of RSS feed testing"""
        
        working_feeds = results_df[results_df['status'] == 'SUCCESS']
        failed_feeds = results_df[results_df['status'] == 'FAILED']
        
        report = f"""
# RSS Feed Testing Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}

## Summary
- **Total feeds tested**: {len(results_df)}
- **Working feeds**: {len(working_feeds)}
- **Failed feeds**: {len(failed_feeds)}
- **Success rate**: {len(working_feeds)/len(results_df)*100:.1f}%

## Working Feeds (Recommended for Implementation)
"""
        
        # Sort by market relevance
        working_sorted = working_feeds.sort_values('market_relevance', ascending=False)
        
        for _, feed in working_sorted.iterrows():
            freshness = "Recent" if feed['latest_date'] and feed['latest_date'] > datetime.now() - timedelta(hours=24) else "Stale"
            
            report += f"""
### {feed['name']}
- **Market Relevance**: {feed['market_relevance']}% of articles
- **Entry Count**: {feed['entries']} articles
- **Data Freshness**: {freshness}
- **Latest Article**: {feed['latest_date'].strftime('%Y-%m-%d %H:%M') if feed['latest_date'] else 'Unknown'}
- **Sample Headlines**:
"""
            for title in feed.get('sample_titles', []):
                if title:
                    report += f"  - {title}\n"
        
        if len(failed_feeds) > 0:
            report += f"""
## Failed Feeds (Need Alternative Sources)
"""
            for _, feed in failed_feeds.iterrows():
                report += f"- **{feed['name']}**: {feed['error']}\n"
        
        report += f"""
## Recommendations

**High Priority Implementation** (Market Relevance >50%):
{chr(10).join([f"- {row['name']}" for _, row in working_sorted[working_sorted['market_relevance'] > 50].iterrows()])}

**Secondary Sources** (Market Relevance 20-50%):
{chr(10).join([f"- {row['name']}" for _, row in working_sorted[(working_sorted['market_relevance'] >= 20) & (working_sorted['market_relevance'] <= 50)].iterrows()])}

**Next Steps**:
1. Implement RSS parsing for high-priority feeds
2. Create news categorization system
3. Build Snowflake integration for news storage
4. Develop news-forecast correlation analysis
"""
        
        return report

def main():
    """Run RSS feed testing and generate report"""
    print("Starting RSS Feed Discovery and Testing...")
    print("=" * 50)
    
    tester = NewsSourceTester()
    results = tester.test_all_feeds()
    
    # Display results
    print("\n" + "=" * 50)
    print("RESULTS SUMMARY")
    print("=" * 50)
    
    for _, result in results.iterrows():
        status_symbol = "✅" if result['status'] == 'SUCCESS' else "❌"
        relevance = f"{result['market_relevance']}%" if result['status'] == 'SUCCESS' else "N/A"
        print(f"{status_symbol} {result['name']:<20} | Relevance: {relevance:<6} | Entries: {result['entries']}")
    
    # Generate detailed report
    report = tester.generate_report(results)
    
    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"news_feed_analysis_{timestamp}.md"
    
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"\n📄 Detailed report saved: {report_file}")
    print("\n" + "=" * 50)
    print("RSS FEED TESTING COMPLETE")
    print("=" * 50)
    
    return results, report

if __name__ == "__main__":
    results, report = main()