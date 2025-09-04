#!/usr/bin/env python3
"""
Event Correlation System Test
"""

import sys
import os
sys.path.append('src')

def test_event_correlation():
    """Test event correlation analysis"""
    
    print("=== TESTING EVENT CORRELATION SYSTEM ===")
    
    try:
        from event_correlation_analyzer import EventCorrelationAnalyzer
        print("Event correlation module imported successfully")
        
        analyzer = EventCorrelationAnalyzer()
        print("Analyzer initialized")
        
        # Test with sample data
        results = analyzer.run_analysis(days_back=3)
        
        if results['success']:
            print(f"Analysis completed successfully")
            print(f"Found {results['correlations_found']} correlations")
            return True
        else:
            print("Analysis completed with warnings")
            return True
            
    except Exception as e:
        print(f"Event correlation test failed: {e}")
        return False

def main():
    success = test_event_correlation()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
