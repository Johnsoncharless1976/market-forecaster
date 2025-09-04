#!/usr/bin/env python3
"""
Simple Dashboard Launcher - No API Costs
"""

import subprocess
import sys
import webbrowser
import time

def main():
    print("🎯 ZenMarket Forecaster - Local Dashboard")
    print("=" * 50)
    print("💰 No API costs - Pure data visualization")
    print("🚀 Starting dashboard...")
    
    try:
        # Start streamlit
        process = subprocess.Popen([
            sys.executable, "-m", "streamlit", "run", 
            "local_forecast_dashboard.py", "--server.port", "8504"
        ])
        
        # Wait a moment then open browser
        time.sleep(3)
        webbrowser.open("http://localhost:8504")
        
        print("✅ Dashboard running at http://localhost:8504")
        print("Press Ctrl+C to stop")
        
        # Wait for process
        process.wait()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping dashboard...")
        process.terminate()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
