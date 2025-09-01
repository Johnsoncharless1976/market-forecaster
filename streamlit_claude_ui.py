# File: streamlit_claude_ui.py
# Title: Claude Prompt Runner UI
# Commit Notes: Enhanced UI with Notion sync capability

import streamlit as st
import subprocess
import os
from datetime import datetime

# --- CONFIG ---
NOTION_ENABLED = True
NOTION_TOKEN = os.getenv("NOTION_API_KEY")
NOTION_DB_ID = os.getenv("NOTION_PROMPT_DB")

# Only import notion-client if we have credentials
if NOTION_ENABLED and NOTION_TOKEN and NOTION_DB_ID:
    try:
        from notion_client import Client
        NOTION_AVAILABLE = True
    except ImportError:
        NOTION_AVAILABLE = False
        st.warning("⚠️ notion-client not installed. Notion sync disabled.")
else:
    NOTION_AVAILABLE = False

st.set_page_config(page_title="Claude Prompt Launcher", layout="wide")

st.title("🧠 Claude Runner: Prompt Launcher")
st.markdown("Run Claude prompts from your browser. Prompts and responses are logged and optionally synced to Notion.")

with st.form("prompt_form"):
    title = st.text_input("Prompt Title (optional):")
    tags = st.text_input("Tags (comma-separated):")
    prompt_input = st.text_area("Enter your Claude prompt (markdown supported):", height=300)
    submit_button = st.form_submit_button("Run Prompt")

def log_to_file(filename, content):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(filename, "a", encoding="utf-8") as f:
        f.write(f"\n---\n⏱ {timestamp}\n\n{content}\n")

def create_bundle_file(title, tags, prompt, response):
    os.makedirs("claude_prompts", exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    safe_title = title.strip().replace(" ", "_") if title else "Manual_Prompt_Run"
    filename = f"claude_prompts/prompt_{timestamp}.md"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# {safe_title}\n")
        f.write(f"⏱ Created: {timestamp}\n")
        f.write(f"🏷 Tags: {tags or 'none'}\n\n---\n\n")
        f.write("## 💭 Prompt\n" + prompt + "\n\n")
        f.write("## 🤖 Response\n" + response + "\n")
    return filename

def send_to_notion(title, tags, prompt, response):
    if not NOTION_AVAILABLE:
        return False
    
    notion = Client(auth=NOTION_TOKEN)
    try:
        # Parse tags into list
        tag_list = [{"name": t.strip()} for t in tags.split(",") if t.strip()] if tags else []
        
        notion.pages.create(
            parent={"database_id": NOTION_DB_ID},
            properties={
                "Title": {"title": [{"text": {"content": title}}]},
                "Tags": {"multi_select": tag_list},
                "Timestamp": {"rich_text": [{"text": {"content": datetime.utcnow().isoformat()}}]}
            },
            children=[
                {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Prompt"}}]}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": prompt}}]}},
                {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "Response"}}]}},
                {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": response}}]}}
            ]
        )
        return True
    except Exception as e:
        st.warning(f"⚠️ Notion sync failed: {e}")
        return False

if submit_button:
    runner_dir = os.path.join(os.getcwd(), "claude_runner")
    prompt_path = os.path.join(runner_dir, "prompt.md")

    if not os.path.exists(runner_dir):
        st.error(f"Claude runner directory not found: {runner_dir}")
    else:
        try:
            with open(prompt_path, "w", encoding="utf-8") as f:
                f.write(prompt_input)

            st.success("✅ Prompt saved to prompt.md")
            st.info("⏳ Running Claude Runner...")

            result = subprocess.run(["python", "claude_runner.py"], cwd=runner_dir, capture_output=True, text=True)

            if result.returncode == 0:
                st.success("Claude Runner completed.")
                st.code(result.stdout, language="text")

                # Triple logging + Notion sync
                log_to_file("claude_prompt_log.md", prompt_input)
                log_to_file("claude_response_log.md", result.stdout)
                bundle_file = create_bundle_file(title or "Manual Prompt Run", tags, prompt_input, result.stdout)
                
                # Attempt Notion sync
                if send_to_notion(title or "Manual Prompt Run", tags or "none", prompt_input, result.stdout):
                    st.success("🧠 Synced to Notion!")
                
                st.info("📦 Prompt and response saved + synced")
            else:
                st.error("Runner failed with error:")
                st.code(result.stderr, language="text")

        except Exception as e:
            st.error(f"Error running Claude Runner: {str(e)}")