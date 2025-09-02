#!/usr/bin/env python3
"""
Claude Runner - resilient Anthropic client with model fallback
- Default model: claude-3-haiku-20240307 (cheapest)
- max_tokens is clamped to each model's true limit
- Prompt sources: CLI arg -> ANTHROPIC_PROMPT -> ANTHROPIC_PROMPT_FILE
                  -> prompt.md next to script -> prompt.md in CWD
"""

import os
import sys
import json
from pathlib import Path
import requests
import dotenv

# ---------- Model limits (output token caps) ----------
MODEL_LIMITS = {
    "claude-3-haiku-20240307": 4096,
    "claude-3-sonnet-20240229": 4096,
    "claude-3-5-sonnet-20241022": 8192,   # if you have access
    "claude-3-opus-20240229": 4096,
}

# ---------- Environment ----------
def load_environment():
    dotenv.load_dotenv()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not found in environment or .env")
        sys.exit(1)

    # Default to cheaper model unless you override
    model = os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307").strip()

    # Requested max tokens; will be clamped to the model’s limit
    try:
        requested_max = int(os.getenv("ANTHROPIC_MAX_TOKENS", "1024"))
    except ValueError:
        requested_max = 1024

    try:
        temperature = float(os.getenv("ANTHROPIC_TEMPERATURE", "0.5"))
    except ValueError:
        temperature = 0.5

    print(f"ENV OK  : Using model: {model}")
    print(f"ENV OK  : Requested max_tokens: {requested_max}")
    print(f"ENV OK  : Temperature: {temperature}")
    return api_key, model, requested_max, temperature

# ---------- Prompt resolution ----------
def resolve_prompt_from_sources(argv) -> str:
    """
    Priority:
      1) CLI arg: python claude_runner.py "your prompt"
      2) ANTHROPIC_PROMPT (env)
      3) ANTHROPIC_PROMPT_FILE (env; abs or relative path)
      4) prompt.md next to this script
      5) prompt.md in current working directory
    """
    # 1) CLI arg
    if len(argv) > 1 and argv[1].strip():
        print("PROMPT  : Using CLI argument")
        return argv[1].strip()

    # 2) env text
    env_prompt = os.getenv("ANTHROPIC_PROMPT")
    if env_prompt and env_prompt.strip():
        print("PROMPT  : Using ANTHROPIC_PROMPT (env)")
        return env_prompt.strip()

    # 3) env file path
    env_prompt_file = os.getenv("ANTHROPIC_PROMPT_FILE")
    if env_prompt_file:
        p = Path(env_prompt_file).expanduser()
        if not p.is_absolute():
            p = Path.cwd() / p
        if p.exists():
            print(f"PROMPT  : Using ANTHROPIC_PROMPT_FILE: {p}")
            content = p.read_text(encoding="utf-8").strip()
            if content:
                return content
            print("ERROR   : Prompt file is empty:", p)

    # 4) prompt.md next to script
    script_dir = Path(__file__).resolve().parent
    near_script = script_dir / "prompt.md"
    if near_script.exists():
        print(f"PROMPT  : Using prompt.md next to script: {near_script}")
        content = near_script.read_text(encoding="utf-8").strip()
        if content:
            return content
        print("ERROR   : prompt.md next to script is empty.")

    # 5) prompt.md in CWD
    cwd_file = Path.cwd() / "prompt.md"
    if cwd_file.exists():
        print(f"PROMPT  : Using prompt.md in CWD: {cwd_file}")
        content = cwd_file.read_text(encoding="utf-8").strip()
        if content:
            return content
        print("ERROR   : prompt.md in CWD is empty.")

    print("ERROR   : No prompt found.")
    print('         Provide one of:')
    print('           • CLI: python claude_runner.py "your prompt"')
    print("           • Env text: ANTHROPIC_PROMPT")
    print("           • Env path: ANTHROPIC_PROMPT_FILE=/path/to/prompt.md")
    print("           • File: prompt.md (next to script or in CWD)")
    sys.exit(1)

# ---------- Anthropic call ----------
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

def clamp_max_tokens(model: str, requested: int) -> int:
    limit = MODEL_LIMITS.get(model)
    if limit is None:
        # Unknown model; be conservative
        print(f"WARNING : Unknown model limit for {model}. Clamping to 4096.")
        limit = 4096
    if requested > limit:
        print(f"INFO    : Clamping max_tokens {requested} -> {limit} for {model}")
        return limit
    return requested

def post_message(api_key: str, model: str, prompt: str, max_tokens: int, temperature: float):
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": clamp_max_tokens(model, max_tokens),
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }
    resp = requests.post(ANTHROPIC_URL, headers=headers, json=body, timeout=90)
    return resp

def try_model(api_key: str, model: str, prompt: str, max_tokens: int, temperature: float):
    print(f"TRY     : {model}")
    try:
        r = post_message(api_key, model, prompt, max_tokens, temperature)
    except requests.exceptions.Timeout:
        print("ERROR   : Timeout")
        return None
    except requests.exceptions.RequestException as e:
        print(f"ERROR   : Request error: {e}")
        return None

    if r.status_code == 200:
        print(f"SUCCESS : {model}")
        return r.json()

    # Common errors
    if r.status_code == 401:
        print("ERROR   : 401 Unauthorized — check ANTHROPIC_API_KEY")
        return "auth_error"
    if r.status_code == 404:
        print(f"ERROR   : 404 Model not found: {model}")
        return None

    # Token limit or quota style messages
    try:
        err = r.json()
    except Exception:
        err = {"text": r.text}
    print(f"ERROR   : {r.status_code} {err}")
    return None

# ---------- Fallback orchestrator ----------
def call_with_fallback(api_key: str, preferred_model: str, prompt: str, max_tokens: int, temperature: float):
    chain = [
        preferred_model,
        "claude-3-haiku-20240307",    # cheapest default
        "claude-3-sonnet-20240229",   # reliable alt
    ]
    # de-dup while preserving order
    seen, models = set(), []
    for m in chain:
        if m not in seen:
            seen.add(m); models.append(m)

    print("START   : Calling Anthropic with fallback")
    for idx, model in enumerate(models):
        res = try_model(api_key, model, prompt, max_tokens, temperature)
        if res == "auth_error":
            sys.exit(1)
        if res is not None:
            return res
        if idx < len(models) - 1:
            print("INFO    : Falling back to next model...")
    print("ERROR   : All models failed.")
    sys.exit(1)

# ---------- Output ----------
def format_output(result):
    try:
        # Anthropic Messages API: {"content":[{"type":"text","text":"..."}], ...}
        blocks = result.get("content", [])
        first_text = ""
        for b in blocks:
            if isinstance(b, dict) and b.get("type") == "text":
                first_text = b.get("text", "")
                if first_text:
                    break
        if not first_text and isinstance(blocks, list) and blocks:
            # fallback (some SDKs may structure differently)
            first_text = blocks[0].get("text", "")

        print("\n" + "=" * 60)
        print("CLAUDE RESPONSE")
        print("=" * 60)
        print(first_text.strip())
        print("=" * 60 + "\n")

        # Optional: save to file if ANTHROPIC_OUT is set
        out_path = os.getenv("ANTHROPIC_OUT")
        if out_path:
            Path(out_path).expanduser().write_text(first_text, encoding="utf-8")
            print(f"SAVED   : {out_path}")

    except Exception as e:
        print(f"ERROR   : Formatting output: {e}")
        print("RAW     :", json.dumps(result, indent=2))

# ---------- Main ----------
def main():
    print("Claude Runner v2.3")
    print("-" * 50)
    api_key, model, requested_max, temperature = load_environment()
    prompt = resolve_prompt_from_sources(sys.argv)
    result = call_with_fallback(api_key, model, prompt, requested_max, temperature)
    format_output(result)
    print("DONE    : Claude Runner completed")

if __name__ == "__main__":
    main()
