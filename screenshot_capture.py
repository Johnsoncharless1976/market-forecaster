#!/usr/bin/env python3
"""
Screenshot capture utility for ZenMarket AI Streamlit app
Captures all 4 pages and saves as PNG artifacts
"""

import os
import time
import subprocess
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def setup_driver():
    """Setup Chrome driver for headless screenshot capture"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def wait_for_streamlit():
    """Wait for Streamlit app to be ready"""
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:8501/_stcore/health", timeout=5)
            if response.status_code == 200:
                print(f"✅ Streamlit ready after {i+1} attempts")
                return True
        except:
            pass
        time.sleep(2)
    
    print("❌ Streamlit failed to start")
    return False

def capture_page(driver, page_name, output_path):
    """Capture screenshot of a specific page"""
    try:
        # Navigate to the page
        base_url = "http://localhost:8501"
        driver.get(base_url)
        
        # Wait for page to load
        time.sleep(3)
        
        # Select the page from sidebar
        if page_name != "Overview":
            try:
                # Find and click the selectbox
                selectbox = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='selectbox'] select"))
                )
                
                # Select the desired page
                for option in selectbox.find_elements(By.TAG_NAME, "option"):
                    if option.text == page_name:
                        option.click()
                        break
                
                time.sleep(5)  # Wait for page transition
            except Exception as e:
                print(f"⚠️  Could not navigate to {page_name}: {e}")
        
        # Capture screenshot
        driver.save_screenshot(output_path)
        
        # Get file size
        if os.path.exists(output_path):
            size = os.path.getsize(output_path)
            print(f"📸 {page_name}.png captured - {size} bytes")
            return size
        else:
            print(f"❌ Failed to capture {page_name}")
            return 0
            
    except Exception as e:
        print(f"❌ Error capturing {page_name}: {e}")
        return 0

def main():
    """Main screenshot capture process"""
    print("🚀 Starting Streamlit app for screenshots...")
    
    # Start Streamlit in background
    streamlit_process = subprocess.Popen([
        "streamlit", "run", "app.py",
        "--server.port=8501",
        "--server.address=127.0.0.1",
        "--server.headless=true"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    try:
        # Wait for Streamlit to be ready
        if not wait_for_streamlit():
            return 1
        
        # Setup screenshot directory
        screenshot_dir = "artifacts/viz/screenshots"
        os.makedirs(screenshot_dir, exist_ok=True)
        
        # Setup Chrome driver
        print("🌐 Setting up Chrome driver...")
        driver = setup_driver()
        
        # Capture all pages
        pages = ["Overview", "Zen Grid", "Forecast vs Actual", "Evidence"]
        total_size = 0
        
        for page in pages:
            filename = page.lower().replace(" ", "_").replace("vs", "vs") + ".png"
            output_path = os.path.join(screenshot_dir, filename)
            size = capture_page(driver, page, output_path)
            total_size += size
        
        driver.quit()
        print(f"📊 Total screenshot size: {total_size} bytes")
        
        # List all captured files
        print("\n📁 Screenshot artifacts:")
        for file in os.listdir(screenshot_dir):
            if file.endswith('.png'):
                filepath = os.path.join(screenshot_dir, file)
                size = os.path.getsize(filepath)
                print(f"  - {file}: {size} bytes")
        
        return 0
        
    finally:
        # Clean up Streamlit process
        streamlit_process.terminate()
        streamlit_process.wait()

if __name__ == "__main__":
    exit(main())