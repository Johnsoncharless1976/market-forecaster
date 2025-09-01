# File: claude_forecast_scorer.py
# Title: Claude Forecast Scoring Script
# Commit Notes: Efficient daily forecast post-mortem scorer using Claude API

import os
import requests
from datetime import date
from claude_runner.claude_audit_writer import insert_prompt_session

# Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL_NAME = os.getenv("ANTHROPIC_MODEL", "claude-3-sonnet-20240229")

def load_forecast_bundle(target_date: str) -> str:
    """Mock loader - replace with actual implementation"""
    return f"""
SPX Forecast for {target_date}:
- Expected direction: UP
- Target range: 4950-5000
- RSI expectation: 45-55 range
- Key levels: Support at 4920, Resistance at 5010
"""

def load_actual_outcome(target_date: str) -> str:
    """Mock loader - replace with actual implementation"""
    return f"""
SPX Actual for {target_date}:
- Price movement: +1.2% (UP)
- Close: 4987 (within predicted range)
- RSI: 52 (within expected range)
- Support/Resistance: Held above 4920, didn't test 5010
"""

def build_scoring_prompt(forecast_text: str, outcome_text: str) -> str:
    return f"""You are a market forecaster analyst.

Below is the original forecast text:
---
{forecast_text}
---

Below is the actual outcome (price movement, RSI, straddle hits, etc):
---
{outcome_text}
---

Evaluate the forecast's accuracy using the following criteria:
- Forecast Direction Correct (Yes/No)
- Range Hit (Yes/No) 
- RSI Aligned (Yes/No)
- Overall Accuracy Grade (A-F)
- 1-sentence Feedback Summary

Return in this markdown format:

## Forecast Evaluation Result
- Direction Correct: <Yes/No>
- Range Hit: <Yes/No>
- RSI Aligned: <Yes/No>
- Grade: <A-F>
- Feedback: <1-sentence>"""

def run_claude_scoring(prompt: str) -> str:
    """Call Claude API via Anthropic's messages endpoint"""
    if not ANTHROPIC_API_KEY:
        return "ERROR: ANTHROPIC_API_KEY not configured"
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    data = {
        "model": MODEL_NAME,
        "max_tokens": 600,
        "temperature": 0.4,
        "messages": [{"role": "user", "content": prompt}]
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=60)
        if response.status_code == 200:
            result = response.json()
            return result['content'][0]['text']
        else:
            return f"ERROR: API returned {response.status_code}: {response.text}"
    except Exception as e:
        return f"ERROR: Request failed: {e}"

def main():
    today = date.today().isoformat()
    
    # Load data
    forecast_text = load_forecast_bundle(today)
    outcome_text = load_actual_outcome(today)
    
    # Build prompt and get Claude response
    prompt = build_scoring_prompt(forecast_text, outcome_text)
    response = run_claude_scoring(prompt)
    
    print(f"Forecast Scoring for {today}")
    print("-" * 40)
    print(response)
    
    # Log to Snowflake audit
    try:
        insert_prompt_session(
            title=f"Forecast Scoring {today}",
            tags=["zenmarket", "scoring", "postmortem"],
            prompt=prompt,
            response=response
        )
        print("\nSUCCESS: Logged to audit system")
    except Exception as e:
        print(f"WARNING: Audit logging failed: {e}")

if __name__ == "__main__":
    main()