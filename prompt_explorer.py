# File: prompt_explorer.py
# Title: Claude Prompt Explorer UI
# Commit Notes: Initial version of the tag/date/title filter dashboard for exploring Claude bundles

import streamlit as st
import os
import glob
from datetime import datetime
from pathlib import Path

st.set_page_config(page_title="Claude Prompt Explorer", layout="wide")
st.title("🔎 Claude Prompt Explorer")
st.markdown("Filter and browse saved Claude prompt sessions.")

PROMPT_DIR = "claude_prompts"

# Helper to parse metadata from each bundle
def load_prompt_bundle(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        return None

    # Extract title (first line after #)
    title = Path(file_path).stem.replace("_", " ")
    if content.startswith("# "):
        title = content.split("\n")[0][2:].strip()
    
    created = "Unknown"
    tags = []
    
    # Extract metadata
    if "⏱ Created:" in content:
        created = content.split("⏱ Created:")[1].split("\n")[0].strip()
    if "🏷 Tags:" in content:
        tags_line = content.split("🏷 Tags:")[1].split("\n")[0].strip()
        tags = [tag.strip() for tag in tags_line.split(",") if tag.strip() and tag.strip() != "none"]

    return {
        "path": file_path,
        "title": title,
        "created": created,
        "tags": tags,
        "content": content
    }

# Check if prompt directory exists
if not os.path.exists(PROMPT_DIR):
    st.warning(f"⚠️ Prompt directory '{PROMPT_DIR}' not found. Run some prompts first to create bundles!")
    st.stop()

# Load all bundles
bundle_files = sorted(glob.glob(f"{PROMPT_DIR}/*.md"))
if not bundle_files:
    st.info("No prompt bundles found. Run some prompts to see them here!")
    st.stop()

bundles = []
for f in bundle_files:
    bundle = load_prompt_bundle(f)
    if bundle:
        bundles.append(bundle)

if not bundles:
    st.error("Failed to load any prompt bundles. Check file formats.")
    st.stop()

# Extract tag set
all_tags = sorted({tag for b in bundles for tag in b["tags"] if tag})

# Filters
st.sidebar.header("🔍 Filters")
tags_filter = st.sidebar.multiselect("Filter by tags:", options=all_tags)
title_filter = st.sidebar.text_input("Search title contains:")
date_filter = st.sidebar.date_input("Filter by date range:", value=[])

# Filter logic
def bundle_matches(bundle):
    # Tag filter
    if tags_filter and not set(tags_filter).intersection(set(bundle["tags"])):
        return False
    
    # Title filter
    if title_filter and title_filter.lower() not in bundle["title"].lower():
        return False
    
    # Date filter
    if isinstance(date_filter, (list, tuple)) and len(date_filter) == 2:
        try:
            # Try to parse timestamp format YYYYMMDDTHHMMSS
            if len(bundle["created"]) >= 8:
                date_str = bundle["created"][:8]  # First 8 chars YYYYMMDD
                bundle_date = datetime.strptime(date_str, "%Y%m%d")
                if not (date_filter[0] <= bundle_date.date() <= date_filter[1]):
                    return False
        except:
            # If date parsing fails, skip date filtering for this bundle
            pass
    
    return True

filtered = [b for b in bundles if bundle_matches(b)]

# Display results
st.markdown(f"**{len(filtered)} result(s) found out of {len(bundles)} total bundles**")

if filtered:
    # Sort by created date (newest first)
    try:
        filtered = sorted(filtered, key=lambda x: x["created"], reverse=True)
    except:
        pass  # Keep original order if sorting fails

    for i, b in enumerate(filtered):
        with st.expander(f"📄 {b['title']} ({b['created']})"):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                if b["tags"]:
                    tag_badges = " ".join([f"`{tag}`" for tag in b["tags"]])
                    st.markdown(f"**Tags:** {tag_badges}")
                else:
                    st.markdown("**Tags:** none")
                
                st.markdown("**Full Content:**")
                st.code(b["content"], language="markdown")
            
            with col2:
                st.download_button(
                    "📥 Download Bundle",
                    b["content"],
                    file_name=os.path.basename(b["path"]),
                    key=f"download_{i}"
                )
                
                if st.button("📂 Show Path", key=f"path_{i}"):
                    st.code(b["path"])
else:
    st.info("No bundles match your current filters. Try adjusting the criteria.")