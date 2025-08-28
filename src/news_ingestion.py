#!/usr/bin/env python3
"""
News & Macro Ingestion Engine v0.1
Lightweight RSS/official page pull for daily artifacts
"""

import os
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import re
import hashlib
from urllib.parse import urlparse
import time

# Add src to path
sys.path.append(str(Path(__file__).parent))


class NewsIngestionEngine:
    """Lightweight news ingestion with whitelist and weight enforcement"""
    
    def __init__(self, whitelist_path="NEWS_WHITELIST.md", weights_path="NEWS_SOURCE_WEIGHTS.yaml"):
        self.whitelist_path = Path(whitelist_path)
        self.weights_path = Path(weights_path)
        self.enabled = os.getenv('NEWS_ENABLED', 'true').lower() == 'true'
        
        self.whitelist_domains = self._load_whitelist()
        self.source_weights = self._load_weights()
        
        # Quality control settings
        self.min_headline_length = 10
        self.max_headline_length = 500
        self.dedup_threshold = 0.8
        
        # Rate limiting
        self.request_delay = 1.0  # 1 second between requests
        
    def _load_whitelist(self):
        """Load approved domains from whitelist"""
        if not self.whitelist_path.exists():
            return set()
        
        domains = set()
        with open(self.whitelist_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Extract domains from markdown (look for domain.com patterns)
            domain_pattern = r'([a-zA-Z0-9-]+\.(?:gov|com|org|eu|uk|jp|int))'
            matches = re.findall(domain_pattern, content)
            domains.update(matches)
        
        return domains
    
    def _load_weights(self):
        """Load source weights configuration"""
        if not self.weights_path.exists():
            return {}
        
        with open(self.weights_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _is_domain_allowed(self, url):
        """Check if URL domain is in whitelist"""
        try:
            domain = urlparse(url).netloc.lower()
            # Remove www. prefix if present
            domain = re.sub(r'^www\.', '', domain)
            return domain in self.whitelist_domains
        except:
            return False
    
    def _get_source_weight(self, domain):
        """Get weight for source domain"""
        domain = re.sub(r'^www\.', '', domain.lower())
        
        # Check each category for the domain
        for category, config in self.source_weights.items():
            if isinstance(config, dict) and 'sources' in config:
                if domain in config['sources']:
                    return config['weight'], category
        
        return 0.0, 'unknown'
    
    def _simulate_news_fetch(self, domain, category):
        """Simulate news fetching for testing (replace with actual RSS/API calls)"""
        # Simulate different types of news based on category
        news_items = []
        
        if category == 'official':
            items = [
                {"headline": f"Fed releases monthly economic indicators", "severity": "HIGH"},
                {"headline": f"Treasury announces new bond issuance schedule", "severity": "MEDIUM"},
            ]
        elif category == 'tier1_press':
            items = [
                {"headline": f"Market volatility increases amid uncertainty", "severity": "MEDIUM"},
                {"headline": f"Central bank signals policy shift", "severity": "HIGH"},
            ]
        elif category == 'low_trust':
            items = [
                {"headline": f"Market manipulation alleged in trading scandal", "severity": "HIGH"},
                {"headline": f"Insider sources claim major policy change", "severity": "MEDIUM"},
            ]
        else:
            items = [
                {"headline": f"Economic data shows mixed signals", "severity": "LOW"},
            ]
        
        # Add metadata to each item
        for item in items:
            item.update({
                "source": domain,
                "category": category,
                "timestamp": datetime.now(),
                "link": f"https://{domain}/article-{hash(item['headline']) % 1000}",
                "requires_corroboration": category == 'low_trust'
            })
            news_items.append(item)
        
        return news_items
    
    def _simulate_macro_data(self):
        """Simulate macro calendar data (replace with ForexFactory API)"""
        macro_events = []
        
        # Simulate some economic events with consensus/actual
        events = [
            {
                "event": "Non-Farm Payrolls",
                "time": "08:30 ET",
                "consensus": 180000,
                "actual": 195000,
                "prior": 175000,
                "severity": "HIGH"
            },
            {
                "event": "Unemployment Rate",
                "time": "08:30 ET", 
                "consensus": 3.9,
                "actual": 3.8,
                "prior": 4.0,
                "severity": "HIGH"
            },
            {
                "event": "Consumer Confidence",
                "time": "10:00 ET",
                "consensus": 102.5,
                "actual": 104.2,
                "prior": 101.8,
                "severity": "MEDIUM"
            }
        ]
        
        for event in events:
            # Calculate z-score if consensus and actual exist
            if event['consensus'] and event['actual']:
                # Simplified z-score calculation
                surprise = event['actual'] - event['consensus']
                # Use historical volatility proxy
                vol_proxy = abs(event['consensus']) * 0.02  # 2% of consensus as vol proxy
                z_score = surprise / vol_proxy if vol_proxy > 0 else 0.0
                event['z_score'] = z_score
                event['surprise_direction'] = 'positive' if surprise > 0 else 'negative' if surprise < 0 else 'neutral'
            
            event['timestamp'] = datetime.now()
            event['source'] = 'forexfactory.com'
            macro_events.append(event)
        
        return macro_events
    
    def _check_corroboration(self, news_items):
        """Check corroboration for low-trust sources"""
        corroborated_items = []
        
        # Group by headline similarity (simplified)
        headline_groups = {}
        
        for item in news_items:
            # Simple grouping by first 50 characters
            key = item['headline'][:50].lower()
            if key not in headline_groups:
                headline_groups[key] = []
            headline_groups[key].append(item)
        
        for group in headline_groups.values():
            low_trust_items = [item for item in group if item['requires_corroboration']]
            other_items = [item for item in group if not item['requires_corroboration']]
            
            # If low-trust items have corroboration from other sources
            for low_trust_item in low_trust_items:
                if len(other_items) >= 1:  # At least 1 independent source
                    low_trust_item['corroborated'] = True
                    corroborated_items.append(low_trust_item)
                else:
                    low_trust_item['corroborated'] = False
                    low_trust_item['status'] = 'MUTED'
                    corroborated_items.append(low_trust_item)
            
            # Add all other items
            corroborated_items.extend(other_items)
        
        return corroborated_items
    
    def ingest_daily_news(self, target_date=None):
        """Main ingestion pipeline for daily news"""
        if not self.enabled:
            return {
                'news_items': [],
                'macro_events': [],
                'sources_used': 0,
                'enabled': False
            }
        
        if target_date is None:
            target_date = datetime.now().date()
        
        print(f"Starting news ingestion for {target_date}...")
        
        all_news_items = []
        sources_used = set()
        
        # Simulate fetching from different source categories
        for category, config in self.source_weights.items():
            if isinstance(config, dict) and 'sources' in config and config['weight'] > 0:
                for domain in config['sources']:
                    if self._is_domain_allowed(f"https://{domain}"):
                        print(f"Fetching from {domain} (category: {category})")
                        
                        # Simulate fetch (replace with actual implementation)
                        items = self._simulate_news_fetch(domain, category)
                        all_news_items.extend(items)
                        sources_used.add(domain)
                        
                        time.sleep(self.request_delay)  # Rate limiting
        
        # Check corroboration for low-trust sources
        all_news_items = self._check_corroboration(all_news_items)
        
        # Fetch macro events
        macro_events = self._simulate_macro_data()
        
        print(f"Ingestion complete. {len(all_news_items)} news items, {len(macro_events)} macro events from {len(sources_used)} sources")
        
        return {
            'news_items': all_news_items,
            'macro_events': macro_events,
            'sources_used': len(sources_used),
            'enabled': True,
            'target_date': target_date
        }
    
    def write_daily_artifacts(self, ingestion_result, output_dir='audit_exports'):
        """Write daily artifacts: NEWS_FEED_LOG.md, MACRO_EVENTS.md"""
        if not ingestion_result['enabled']:
            return {}
        
        target_date = ingestion_result['target_date']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        audit_dir = Path(output_dir) / 'daily' / timestamp
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        artifacts = {}
        
        # Write NEWS_FEED_LOG.md
        news_log = self._write_news_feed_log(ingestion_result['news_items'], audit_dir)
        artifacts['news_feed_log'] = news_log
        
        # Write MACRO_EVENTS.md
        macro_log = self._write_macro_events_log(ingestion_result['macro_events'], audit_dir)
        artifacts['macro_events_log'] = macro_log
        
        return artifacts
    
    def _write_news_feed_log(self, news_items, audit_dir):
        """Write NEWS_FEED_LOG.md"""
        log_file = audit_dir / 'NEWS_FEED_LOG.md'
        
        content = f"""# News Feed Log

**Date**: {datetime.now().date()}
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Items Processed**: {len(news_items)}

## News Items by Source

"""
        
        # Group by source
        by_source = {}
        for item in news_items:
            source = item['source']
            if source not in by_source:
                by_source[source] = []
            by_source[source].append(item)
        
        for source, items in sorted(by_source.items()):
            weight, category = self._get_source_weight(source)
            content += f"### {source} (Category: {category}, Weight: {weight})\n\n"
            
            for item in items:
                status = ""
                if item.get('status') == 'MUTED':
                    status = " ⚠️ MUTED (No corroboration)"
                elif item.get('corroborated') is False:
                    status = " ❌ UNCORROBORATED"
                elif item.get('corroborated') is True:
                    status = " ✅ CORROBORATED"
                
                content += f"- **{item['severity']}**: {item['headline']}{status}\n"
                content += f"  - Time: {item['timestamp'].strftime('%H:%M:%S')}\n"
                content += f"  - Link: {item['link']}\n\n"
        
        # Summary statistics
        content += f"""## Summary

### By Category
"""
        category_counts = {}
        muted_counts = {}
        
        for item in news_items:
            weight, category = self._get_source_weight(item['source'])
            category_counts[category] = category_counts.get(category, 0) + 1
            if item.get('status') == 'MUTED':
                muted_counts[category] = muted_counts.get(category, 0) + 1
        
        for category, count in sorted(category_counts.items()):
            muted = muted_counts.get(category, 0)
            content += f"- **{category}**: {count} items"
            if muted > 0:
                content += f" ({muted} muted)"
            content += f"\n"
        
        content += f"""
### Quality Control
- Items requiring corroboration: {len([item for item in news_items if item.get('requires_corroboration')])}
- Muted items (uncorroborated): {len([item for item in news_items if item.get('status') == 'MUTED'])}
- Total sources used: {len(set(item['source'] for item in news_items))}

---
Generated by News Ingestion Engine v0.1
"""
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(log_file)
    
    def _write_macro_events_log(self, macro_events, audit_dir):
        """Write MACRO_EVENTS.md"""
        log_file = audit_dir / 'MACRO_EVENTS.md'
        
        content = f"""# Macro Events Log

**Date**: {datetime.now().date()}
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
**Events Processed**: {len(macro_events)}

## Economic Calendar Events

"""
        
        for event in macro_events:
            content += f"### {event['event']} ({event['time']})\n\n"
            content += f"- **Severity**: {event['severity']}\n"
            
            if 'consensus' in event and event['consensus']:
                content += f"- **Consensus**: {event['consensus']}\n"
            if 'actual' in event and event['actual']:
                content += f"- **Actual**: {event['actual']}\n"
            if 'prior' in event and event['prior']:
                content += f"- **Prior**: {event['prior']}\n"
            
            if 'z_score' in event:
                surprise_text = "📈 Positive" if event['z_score'] > 0 else "📉 Negative" if event['z_score'] < 0 else "➖ Neutral"
                content += f"- **Surprise**: {surprise_text} (z={event['z_score']:.2f})\n"
                content += f"- **Market Impact**: {'Significant' if abs(event['z_score']) >= 1.0 else 'Moderate'}\n"
            
            content += f"- **Source**: {event['source']}\n\n"
        
        content += f"""## Summary

### Surprise Analysis
"""
        
        high_surprises = [e for e in macro_events if e.get('z_score') and abs(e['z_score']) >= 1.0]
        positive_surprises = [e for e in macro_events if e.get('z_score') and e['z_score'] > 0]
        negative_surprises = [e for e in macro_events if e.get('z_score') and e['z_score'] < 0]
        
        content += f"- **High Impact Events** (|z| >= 1.0): {len(high_surprises)}\n"
        content += f"- **Positive Surprises**: {len(positive_surprises)}\n"
        content += f"- **Negative Surprises**: {len(negative_surprises)}\n"
        
        if high_surprises:
            content += f"\n### High Impact Events Detail\n"
            for event in high_surprises:
                content += f"- {event['event']}: z={event['z_score']:.2f} ({event['surprise_direction']})\n"
        
        content += f"""

---
Generated by News Ingestion Engine v0.1
"""
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(log_file)


def main():
    """Test news ingestion"""
    ingestion = NewsIngestionEngine()
    
    # Run daily ingestion
    result = ingestion.ingest_daily_news()
    
    # Write artifacts
    artifacts = ingestion.write_daily_artifacts(result)
    
    print(f"\nIngestion complete!")
    print(f"Sources used: {result['sources_used']}")
    print(f"News items: {len(result['news_items'])}")
    print(f"Macro events: {len(result['macro_events'])}")
    print(f"Artifacts: {list(artifacts.values())}")
    
    return result, artifacts


if __name__ == '__main__':
    main()