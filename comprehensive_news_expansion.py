# comprehensive_news_expansion.py
#!/usr/bin/env python3
"""
Comprehensive News System Expansion
Adds broader news sources with impact scoring and event categorization
"""

import feedparser
import requests
from datetime import datetime, timedelta
import pandas as pd
import re
from typing import Dict, List, Tuple
import time
import json

class ComprehensiveNewsAnalyzer:
    def __init__(self):
        # Expanded RSS feeds covering all market-moving categories
        self.rss_feeds = {
            # EXISTING WORKING FEEDS (from your previous tests)
            "fed_news": "https://www.federalreserve.gov/feeds/press_all.xml",
            "cnbc_economy": "https://www.cnbc.com/id/20910258/device/rss/rss.html",
            "seeking_alpha": "https://seekingalpha.com/market_currents.xml",
            "marketwatch_breaking": "http://feeds.marketwatch.com/marketwatch/realtimeheadlines/",
            
            # PREMIUM SOURCES (alternative URLs/methods)
            "wsj_markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
            "reuters_business": "http://feeds.reuters.com/reuters/businessNews",
            "bloomberg_economics": "https://feeds.bloomberg.com/economics/news.rss",
            "financial_times": "https://www.ft.com/rss/home/us",
            
            # ALTERNATIVE/CONTRARIAN SOURCES
            "zerohedge": "https://feeds.feedburner.com/zerohedge/feed",
            "naked_capitalism": "https://www.nakedcapitalism.com/feed",
            "wolf_street": "https://wolfstreet.com/feed/",
            
            # GEOPOLITICAL/WAR THREAT SOURCES
            "defense_news": "https://www.defensenews.com/arc/outboundfeeds/rss/category/pentagon/?outputType=xml",
            "stratfor": "https://worldview.stratfor.com/rss.xml",
            "council_foreign_relations": "https://www.cfr.org/feeds/rss.xml",
            
            # CORPORATE/BANKRUPTCY/CRISIS SOURCES
            "bankruptcy_news": "https://www.abi.org/feed",
            "sec_news": "https://www.sec.gov/news/pressreleases.rss",
            "corporate_distress": "https://www.reuters.com/business/finance",
            
            # TRADE/TARIFF SOURCES
            "trade_gov": "https://www.trade.gov/rss-feeds/trade-news.xml",
            "ustr_releases": "https://ustr.gov/rss.xml",
            "customs_news": "https://www.cbp.gov/newsroom/news-releases/rss",
            
            # SECTOR-SPECIFIC SOURCES
            "energy_news": "https://www.energy.gov/rss-feeds/news",
            "tech_crunch_finance": "https://techcrunch.com/category/fintech/feed/",
            "bank_news": "https://www.americanbanker.com/feed",
            
            # INTERNATIONAL SOURCES
            "bbc_business": "http://feeds.bbci.co.uk/news/business/rss.xml",
            "ecb_news": "https://www.ecb.europa.eu/rss/news.html",
            "bank_japan": "https://www.boj.or.jp/en/rss/whatsnew.xml"
        }
        
        # Market impact keywords with scoring weights
        self.impact_keywords = {
            # IMMEDIATE HIGH IMPACT (Score: 10)
            "war": 10, "invasion": 10, "nuclear": 10, "attack": 10, "crisis": 10,
            "bankruptcy": 10, "collapse": 10, "crash": 10, "emergency": 10,
            "federal reserve": 10, "interest rate": 10, "fomc": 10,
            
            # HIGH IMPACT (Score: 8)
            "tariff": 8, "trade war": 8, "sanctions": 8, "embargo": 8,
            "recession": 8, "inflation": 8, "unemployment": 8, "gdp": 8,
            "earnings miss": 8, "profit warning": 8, "downgrade": 8,
            
            # MEDIUM IMPACT (Score: 6)
            "merger": 6, "acquisition": 6, "ipo": 6, "dividend": 6,
            "oil price": 6, "commodities": 6, "currency": 6, "dollar": 6,
            "china": 6, "europe": 6, "geopolitical": 6,
            
            # MODERATE IMPACT (Score: 4)
            "earnings": 4, "revenue": 4, "guidance": 4, "outlook": 4,
            "analyst": 4, "upgrade": 4, "target price": 4,
            "regulatory": 4, "policy": 4, "legislation": 4,
            
            # LOW IMPACT (Score: 2)
            "market": 2, "stocks": 2, "trading": 2, "investment": 2,
            "company": 2, "business": 2, "financial": 2
        }
        
        # Event categorization
        self.event_categories = {
            "MONETARY_POLICY": ["fed", "federal reserve", "interest rate", "fomc", "powell"],
            "GEOPOLITICAL": ["war", "china", "russia", "ukraine", "taiwan", "sanctions", "military"],
            "TRADE": ["tariff", "trade war", "wto", "nafta", "usmca", "trade deal"],
            "CORPORATE_CRISIS": ["bankruptcy", "collapse", "fraud", "scandal", "investigation"],
            "MACRO_DATA": ["gdp", "inflation", "cpi", "ppi", "employment", "unemployment", "jobless"],
            "ENERGY": ["oil", "gas", "energy", "crude", "petroleum", "opec"],
            "BANKING": ["bank", "credit", "lending", "mortgage", "financial institution"],
            "TECH_DISRUPTION": ["ai", "technology", "cyber", "data breach", "regulation"],
            "MARKET_STRUCTURE": ["sec", "regulation", "compliance", "etf", "derivatives"],
            "NATURAL_DISASTERS": ["hurricane", "earthquake", "tsunami", "wildfire", "flooding", "tornado", "drought", "storm", "natural disaster", "weather emergency", "climate"]
        }

    def test_feed_accessibility(self, name: str, url: str) -> Dict:
        """Test if RSS feed is accessible and parse basic info"""
        try:
            feed = feedparser.parse(url)
            if feed.bozo and not feed.entries:
                return {"accessible": False, "error": "Feed parsing failed", "entries": 0}
            
            return {
                "accessible": True,
                "entries": len(feed.entries),
                "title": feed.feed.get('title', 'Unknown'),
                "description": feed.feed.get('description', ''),
                "last_updated": feed.feed.get('updated', 'Unknown')
            }
        except Exception as e:
            return {"accessible": False, "error": str(e), "entries": 0}

    def calculate_impact_score(self, title: str, description: str) -> int:
        """Calculate market impact score for a news item"""
        text = (title + " " + description).lower()
        score = 0
        
        for keyword, weight in self.impact_keywords.items():
            if keyword in text:
                score += weight
        
        return min(score, 50)  # Cap at 50 to prevent extreme scores

    def categorize_event(self, title: str, description: str) -> List[str]:
        """Categorize news event into impact categories"""
        text = (title + " " + description).lower()
        categories = []
        
        for category, keywords in self.event_categories.items():
            if any(keyword in text for keyword in keywords):
                categories.append(category)
        
        return categories if categories else ["GENERAL_MARKET"]

    def analyze_news_feed(self, name: str, url: str) -> Dict:
        """Comprehensive analysis of a news feed"""
        print(f"Analyzing {name}: {url}")
        
        # Test accessibility
        feed_info = self.test_feed_accessibility(name, url)
        if not feed_info["accessible"]:
            return {
                "name": name,
                "url": url,
                "status": "FAILED",
                "error": feed_info["error"],
                "analysis": None
            }
        
        # Parse feed entries
        feed = feedparser.parse(url)
        articles = []
        
        for entry in feed.entries[:20]:  # Analyze up to 20 recent entries
            title = entry.get('title', '')
            description = entry.get('description', '') or entry.get('summary', '')
            published = entry.get('published', '')
            
            impact_score = self.calculate_impact_score(title, description)
            categories = self.categorize_event(title, description)
            
            articles.append({
                "title": title,
                "description": description[:200],  # Truncate for analysis
                "published": published,
                "impact_score": impact_score,
                "categories": categories
            })
        
        # Calculate feed quality metrics
        avg_impact = sum(article["impact_score"] for article in articles) / len(articles) if articles else 0
        high_impact_count = sum(1 for article in articles if article["impact_score"] >= 8)
        category_distribution = {}
        
        for article in articles:
            for category in article["categories"]:
                category_distribution[category] = category_distribution.get(category, 0) + 1
        
        return {
            "name": name,
            "url": url,
            "status": "SUCCESS",
            "feed_info": feed_info,
            "analysis": {
                "total_articles": len(articles),
                "avg_impact_score": round(avg_impact, 2),
                "high_impact_articles": high_impact_count,
                "category_distribution": category_distribution,
                "sample_articles": articles[:3]  # Show top 3 for review
            }
        }

    def run_comprehensive_analysis(self) -> Dict:
        """Run analysis on all news feeds"""
        print("Starting Comprehensive News Feed Analysis...")
        print("=" * 60)
        
        results = []
        for name, url in self.rss_feeds.items():
            try:
                result = self.analyze_news_feed(name, url)
                results.append(result)
                time.sleep(1)  # Be respectful to servers
            except Exception as e:
                print(f"Error analyzing {name}: {str(e)}")
                results.append({
                    "name": name,
                    "url": url,
                    "status": "ERROR",
                    "error": str(e)
                })
        
        # Generate summary report
        successful_feeds = [r for r in results if r["status"] == "SUCCESS"]
        failed_feeds = [r for r in results if r["status"] != "SUCCESS"]
        
        print("\n" + "=" * 60)
        print("COMPREHENSIVE NEWS ANALYSIS RESULTS")
        print("=" * 60)
        
        # Sort successful feeds by average impact score
        successful_feeds.sort(key=lambda x: x["analysis"]["avg_impact_score"], reverse=True)
        
        print(f"\n✅ SUCCESSFUL FEEDS ({len(successful_feeds)}):")
        for feed in successful_feeds:
            analysis = feed["analysis"]
            print(f"{feed['name']:25} | Avg Impact: {analysis['avg_impact_score']:4.1f} | High Impact: {analysis['high_impact_articles']:2d} | Articles: {analysis['total_articles']:2d}")
        
        if failed_feeds:
            print(f"\n❌ FAILED FEEDS ({len(failed_feeds)}):")
            for feed in failed_feeds:
                print(f"{feed['name']:25} | Error: {feed.get('error', 'Unknown')}")
        
        # Save detailed report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"comprehensive_news_analysis_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved: {report_file}")
        print("\nTop recommendations for integration:")
        
        # Recommend best feeds for integration
        top_feeds = successful_feeds[:5]
        for i, feed in enumerate(top_feeds, 1):
            analysis = feed["analysis"]
            print(f"{i}. {feed['name']} - Avg Impact: {analysis['avg_impact_score']}, Categories: {list(analysis['category_distribution'].keys())[:3]}")
        
        return {
            "successful_feeds": successful_feeds,
            "failed_feeds": failed_feeds,
            "report_file": report_file
        }

if __name__ == "__main__":
    analyzer = ComprehensiveNewsAnalyzer()
    results = analyzer.run_comprehensive_analysis()