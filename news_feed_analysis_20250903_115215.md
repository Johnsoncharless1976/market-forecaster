
# RSS Feed Testing Report - 2025-09-03 11:52

## Summary
- **Total feeds tested**: 10
- **Working feeds**: 7
- **Failed feeds**: 3
- **Success rate**: 70.0%

## Working Feeds (Recommended for Implementation)

### fed_news
- **Market Relevance**: 90.0% of articles
- **Entry Count**: 10 articles
- **Data Freshness**: Recent
- **Latest Article**: 2025-09-03 15:00
- **Sample Headlines**:
  - Federal Reserve Board announces it will host a conference on payments innovation on Tuesday, October
  - Federal Reserve Board announces final individual capital requirements for large banks, effective on 
  - Minutes of the Board's discount rate meetings on July 21 and July 30, 2025

### cnbc_economy
- **Market Relevance**: 60.0% of articles
- **Entry Count**: 10 articles
- **Data Freshness**: Recent
- **Latest Article**: 2025-09-03 14:57
- **Sample Headlines**:
  - Job opening data falls to levels rarely seen since pandemic
  - Euro zone inflation rises to hotter-than-expected 2.1% in August
  - Trump keeps threatening to punish Putin. Here's what's holding him back

### seeking_alpha
- **Market Relevance**: 57.1% of articles
- **Entry Count**: 7 articles
- **Data Freshness**: Recent
- **Latest Article**: 2025-09-03 15:35
- **Sample Headlines**:
  - Amgen dips after trial update for Zai Lab partnered gastric cancer therapy
  - Dollar Tree outlines 4% to 6% comp sales growth target amid multi-price expansion and strategic cost
  - Wall Street is mixed as Google skirts tougher penalties and job openings data declines

### marketwatch_breaking
- **Market Relevance**: 50.0% of articles
- **Entry Count**: 10 articles
- **Data Freshness**: Stale
- **Latest Article**: 2025-06-11 07:09
- **Sample Headlines**:
  - Elon Musk in a post on X says some of his Trump posts ‘went too far’ and that he regrets them
  - Bank of England decision on Thursday will be at 12:02 p.m. local time instead of 12 p.m., due to VE 
  - Swiss National Bank cuts interest rates by a half point to 0.5%

### marketwatch_markets
- **Market Relevance**: 40.0% of articles
- **Entry Count**: 10 articles
- **Data Freshness**: Stale
- **Latest Article**: 2025-07-03 12:36
- **Sample Headlines**:
  - Jobless claims fall to lowest level since mid-May
  - Jobless claims stay low in latest week
  - Consumer credit growth soars in December

### bloomberg_markets
- **Market Relevance**: 40.0% of articles
- **Entry Count**: 10 articles
- **Data Freshness**: Recent
- **Latest Article**: 2025-09-03 15:32
- **Sample Headlines**:
  - ConocoPhillips Plans to Cut Up to 25% of Its Workforce
  - M&amp;G Shares Briefly Spike After FT Report Apollo Examined a Deal
  - Soy Eases With Rains Helping US Fields After Conditions Weaken

### cnbc_markets
- **Market Relevance**: 0.0% of articles
- **Entry Count**: 10 articles
- **Data Freshness**: Recent
- **Latest Article**: 2025-09-03 14:05
- **Sample Headlines**:
  - China's Xi says the world faces 'peace or war' as Trump claims Beijing conspiring against U.S.
  - World’s largest sovereign wealth fund invests $543 million in Manhattan office building
  - Sen. Rand Paul blasts Trump's stake in Intel as 'a step towards socialism'

## Failed Feeds (Need Alternative Sources)
- **yahoo_finance**: HTTP 400
- **reuters_markets**: HTTPSConnectionPool(host='feeds.reuters.com', port=443): Max retries exceeded with url: /reuters/businessNews (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x000001D4C41A3ED0>: Failed to resolve 'feeds.reuters.com' ([Errno 11001] getaddrinfo failed)"))
- **treasury_releases**: HTTP 404

## Recommendations

**High Priority Implementation** (Market Relevance >50%):
- fed_news
- cnbc_economy
- seeking_alpha

**Secondary Sources** (Market Relevance 20-50%):
- marketwatch_breaking
- marketwatch_markets
- bloomberg_markets

**Next Steps**:
1. Implement RSS parsing for high-priority feeds
2. Create news categorization system
3. Build Snowflake integration for news storage
4. Develop news-forecast correlation analysis
