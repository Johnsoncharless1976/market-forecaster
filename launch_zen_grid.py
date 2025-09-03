#!/usr/bin/env python3
"""
Zen Grid Dashboard Launcher
"""

import subprocess
import sys
import webbrowser
import time

def main():
    print("Zen Grid - Pattern Analysis Dashboard")
    print("=" * 50)
    print("Visual forecast analysis and pattern recognition")
    print("Starting dashboard...")
    
    try:
        # Start streamlit dashboard
        process = subprocess.Popen([
            sys.executable, "-m", "streamlit", "run", 
            "zen_grid_dashboard.py", "--server.port", "8506"
        ])
        
        # Wait then open browser
        time.sleep(3)
        webbrowser.open("http://localhost:8506")
        
        print("Dashboard running at http://localhost:8506")
        print("Press Ctrl+C to stop")
        
        # Wait for process
        process.wait()
        
    except KeyboardInterrupt:
        print("\nStopping dashboard...")
        process.terminate()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
