#!/usr/bin/env python3
"""
Claude Runner - A resilient Claude API client with model fallback
Sends prompts from prompt.md to Anthropic's Claude API with automatic model fallback.
"""

import os
import sys
import dotenv
import requests
from pathlib import Path


def load_environment():
    """Load environment variables with error handling."""
    try:
        dotenv.load_dotenv()
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            print("ERROR: ANTHROPIC_API_KEY not found in .env file")
            print("   Please add: ANTHROPIC_API_KEY=your_key_here")
            sys.exit(1)
        
        # Get model from environment or use default
        model = os.getenv("ANTHROPIC_MODEL", "claude-3-sonnet-20240229")
        print("SUCCESS: Environment loaded successfully")
        print(f"SUCCESS: Using model: {model}")
        
        return api_key, model
    except Exception as e:
        print(f"ERROR: Loading environment: {e}")
        sys.exit(1)


def load_prompt():
    """Load prompt from prompt.md with error handling."""
    prompt_file = Path("prompt.md")
    
    if not prompt_file.exists():
        print("ERROR: prompt.md file not found in current directory")
        print("   Please create prompt.md with your Claude prompt")
        sys.exit(1)
    
    try:
        with open(prompt_file, "r", encoding="utf-8") as file:
            content = file.read().strip()
        
        if not content:
            print("ERROR: prompt.md is empty")
            print("   Please add your prompt to prompt.md")
            sys.exit(1)
        
        print("SUCCESS: Prompt loaded successfully")
        return content
    except Exception as e:
        print(f"ERROR: Reading prompt.md: {e}")
        sys.exit(1)


def try_model(api_key, model, user_prompt):
    """Try to call Claude API with a specific model."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    json_data = {
        "model": model,
        "max_tokens": 4000,
        "temperature": 0.5,
        "messages": [
            {"role": "user", "content": user_prompt}
        ],
    }
    
    try:
        print(f"TRYING: Model {model}")
        response = requests.post(url, headers=headers, json=json_data, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            print(f"SUCCESS: Model {model} worked!")
            return result
        elif response.status_code == 404:
            print(f"ERROR: Model {model} not available (404)")
            return None
        elif response.status_code == 401:
            print("ERROR: Authentication failed (401)")
            print("   Check your ANTHROPIC_API_KEY in .env file")
            return "auth_error"
        else:
            print(f"ERROR: API Error {response.status_code}: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"ERROR: Timeout with model: {model}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Request failed with model {model}: {e}")
        return None


def call_claude_with_fallback(api_key, preferred_model, user_prompt):
    """Call Claude API with automatic model fallback."""
    
    # Define fallback chain
    models = [
        preferred_model,
        "claude-3-sonnet-20240229", 
        "claude-3-haiku-20240307"
    ]
    
    # Remove duplicates while preserving order
    models = list(dict.fromkeys(models))
    
    print("STARTING: Claude API call with model fallback")
    
    for i, model in enumerate(models):
        result = try_model(api_key, model, user_prompt)
        
        if result == "auth_error":
            sys.exit(1)
        elif result is not None:
            return result
        elif i < len(models) - 1:
            print("WARNING: Falling back to next model...")
    
    print("ERROR: All models failed. Please check:")
    print("   1. Your API key is valid")
    print("   2. You have access to Claude models")
    print("   3. Your internet connection")
    sys.exit(1)


def format_output(result):
    """Format and display the Claude response."""
    try:
        response_text = result['content'][0]['text']
        
        print("\n" + "="*60)
        print("CLAUDE RESPONSE")
        print("="*60)
        print(response_text)
        print("="*60 + "\n")
        
    except KeyError as e:
        print(f"ERROR: Unexpected response format: {e}")
        print("Raw response:", result)


def main():
    """Main execution function."""
    print("Claude Runner v2.0 - Resilient API Client")
    print("-" * 50)
    
    # Load environment and prompt
    api_key, model = load_environment()
    user_prompt = load_prompt()
    
    # Call Claude with fallback
    result = call_claude_with_fallback(api_key, model, user_prompt)
    
    # Format and display output
    format_output(result)
    
    print("SUCCESS: Claude Runner completed successfully")


if __name__ == "__main__":
    main()