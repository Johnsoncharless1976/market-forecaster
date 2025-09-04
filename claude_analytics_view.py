# File: claude_analytics_view.py
# Title: Claude Prompt Analytics Dashboard
# Commit Notes: Initial version of analytics explorer for Claude prompt bundle metadata

import streamlit as st
import pandas as pd
import os
import re
from datetime import datetime
from glob import glob
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Claude Prompt Analytics", layout="wide")
st.title("📊 Claude Prompt Analytics Dashboard")
st.markdown("Analyze patterns and trends in your Claude prompt usage.")

# Directory where prompt bundles are stored
BUNDLE_DIR = "claude_prompts"

if not os.path.exists(BUNDLE_DIR):
    st.warning("⚠️ Prompt bundle directory not found. Run some prompts first to generate analytics.")
    st.stop()

# Get bundle files sorted by date (newest first)
bundle_files = sorted(glob(os.path.join(BUNDLE_DIR, "prompt_*.md")), reverse=True)

if not bundle_files:
    st.info("ℹ️ No prompt bundles found. Create some prompts to see analytics here!")
    st.stop()

# Parse bundles into DataFrame
@st.cache_data
def load_bundle_data():
    rows = []
    for file_path in bundle_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Extract metadata using regex
            title_match = re.search(r"^# (.+?)$", content, re.MULTILINE)
            timestamp_match = re.search(r"Created: (\d{8}T\d{6})", content)
            tags_match = re.search(r"Tags: (.+?)$", content, re.MULTILINE)

            # Parse extracted data
            title = title_match.group(1).strip() if title_match else "Untitled"
            raw_timestamp = timestamp_match.group(1) if timestamp_match else None
            created_at = datetime.strptime(raw_timestamp, "%Y%m%dT%H%M%S") if raw_timestamp else None
            
            # Parse tags
            if tags_match:
                tags_text = tags_match.group(1).strip()
                tags = [t.strip() for t in tags_text.split(",") if t.strip().lower() not in ["none", ""]]
            else:
                tags = []

            # Calculate content metrics
            word_count = len(content.split())
            char_count = len(content)
            
            # Count prompt vs response sections
            prompt_section = ""
            response_section = ""
            if "## 💭 Prompt" in content:
                parts = content.split("## 💭 Prompt")
                if len(parts) > 1:
                    prompt_part = parts[1]
                    if "## 🤖 Response" in prompt_part:
                        prompt_section = prompt_part.split("## 🤖 Response")[0]
                        response_section = prompt_part.split("## 🤖 Response")[1] if len(prompt_part.split("## 🤖 Response")) > 1 else ""
            
            prompt_word_count = len(prompt_section.split()) if prompt_section else 0
            response_word_count = len(response_section.split()) if response_section else 0

            row = {
                "title": title,
                "created_at": created_at,
                "tags": tags,
                "tag_count": len(tags),
                "filename": os.path.basename(file_path),
                "filepath": file_path,
                "word_count": word_count,
                "char_count": char_count,
                "prompt_words": prompt_word_count,
                "response_words": response_word_count,
                "has_tags": len(tags) > 0
            }
            rows.append(row)
        except Exception as e:
            st.warning(f"Error parsing {file_path}: {e}")
            continue

    return pd.DataFrame(rows)

# Load data
df = load_bundle_data()

if df.empty:
    st.warning("❌ No valid prompt data could be parsed.")
    st.stop()

# Add date column for grouping
df["date"] = df["created_at"].dt.date
df["hour"] = df["created_at"].dt.hour
df["day_of_week"] = df["created_at"].dt.day_name()

# Calculate metrics
total_prompts = len(df)
all_tags = sorted(set(tag for tags in df["tags"] for tag in tags))
avg_words_per_prompt = df["word_count"].mean()
total_words = df["word_count"].sum()

# Header metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Prompts", total_prompts)
with col2:
    st.metric("Unique Tags", len(all_tags))
with col3:
    st.metric("Avg Words/Prompt", f"{avg_words_per_prompt:.0f}")
with col4:
    st.metric("Total Words", f"{total_words:,}")

# Sidebar filters
with st.sidebar:
    st.header("🔍 Filters")
    selected_tags = st.multiselect("Filter by tags:", options=all_tags)
    date_range = st.date_input("Date range:", value=[])
    min_words = st.slider("Minimum word count:", 0, int(df["word_count"].max()), 0)

# Apply filters
filtered = df.copy()
if selected_tags:
    filtered = filtered[filtered["tags"].apply(lambda x: any(tag in x for tag in selected_tags))]
if date_range and len(date_range) == 2:
    start, end = date_range
    filtered = filtered[(filtered["date"] >= start) & (filtered["date"] <= end)]
if min_words > 0:
    filtered = filtered[filtered["word_count"] >= min_words]

st.markdown(f"**Showing {len(filtered)} of {total_prompts} prompts**")

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("📅 Prompt Volume Over Time")
    if not filtered.empty:
        prompt_counts = filtered.groupby("date").size().reset_index(name="count")
        fig_timeline = px.line(prompt_counts, x="date", y="count", 
                              title="Daily Prompt Count",
                              markers=True)
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No data to display")

with col2:
    st.subheader("🏷️ Top Tags")
    if not filtered.empty and all_tags:
        # Create tag frequency data
        tag_series = pd.Series([tag for tags in filtered["tags"] for tag in tags])
        if not tag_series.empty:
            top_tags = tag_series.value_counts().head(10)
            fig_tags = px.bar(x=top_tags.index, y=top_tags.values, 
                             title="Most Used Tags",
                             labels={'x': 'Tags', 'y': 'Frequency'})
            st.plotly_chart(fig_tags, use_container_width=True)
        else:
            st.info("No tags found")
    else:
        st.info("No data to display")

# Additional analytics
col1, col2 = st.columns(2)

with col1:
    st.subheader("⏰ Activity by Hour")
    if not filtered.empty:
        hourly_activity = filtered.groupby("hour").size().reset_index(name="count")
        fig_hours = px.bar(hourly_activity, x="hour", y="count", 
                          title="Prompts by Hour of Day")
        st.plotly_chart(fig_hours, use_container_width=True)

with col2:
    st.subheader("📊 Word Count Distribution") 
    if not filtered.empty:
        fig_words = px.histogram(filtered, x="word_count", 
                               title="Distribution of Prompt Word Counts",
                               nbins=20)
        st.plotly_chart(fig_words, use_container_width=True)

# Detailed table
st.subheader("📋 Detailed Prompt Data")
if not filtered.empty:
    display_df = filtered[["created_at", "title", "tag_count", "word_count", "prompt_words", "response_words"]].copy()
    display_df = display_df.sort_values("created_at", ascending=False)
    display_df["created_at"] = display_df["created_at"].dt.strftime("%Y-%m-%d %H:%M")
    
    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "created_at": "Created",
            "title": "Title", 
            "tag_count": "Tags",
            "word_count": "Total Words",
            "prompt_words": "Prompt Words",
            "response_words": "Response Words"
        }
    )
else:
    st.info("No data matches current filters")

# Summary statistics
if not filtered.empty:
    st.subheader("📈 Summary Statistics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Word Count Stats:**")
        st.write(f"• Mean: {filtered['word_count'].mean():.1f}")
        st.write(f"• Median: {filtered['word_count'].median():.1f}")
        st.write(f"• Max: {filtered['word_count'].max()}")
        st.write(f"• Min: {filtered['word_count'].min()}")
    
    with col2:
        st.write("**Tag Statistics:**")
        st.write(f"• Prompts with tags: {filtered['has_tags'].sum()}")
        st.write(f"• Avg tags per prompt: {filtered['tag_count'].mean():.1f}")
        st.write(f"• Most tagged prompt: {filtered['tag_count'].max()} tags")
    
    with col3:
        st.write("**Activity Patterns:**")
        most_active_day = filtered['day_of_week'].mode().iloc[0] if not filtered['day_of_week'].empty else "N/A"
        most_active_hour = filtered['hour'].mode().iloc[0] if not filtered['hour'].empty else "N/A"
        st.write(f"• Most active day: {most_active_day}")
        st.write(f"• Most active hour: {most_active_hour}:00")
        
        date_range_days = (filtered['date'].max() - filtered['date'].min()).days if len(filtered['date'].unique()) > 1 else 1
        st.write(f"• Prompts per day: {len(filtered) / max(date_range_days, 1):.1f}")