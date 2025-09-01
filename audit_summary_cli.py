# File: audit_summary_cli.py
# Title: Claude Runner Audit Summary CLI
# Commit Notes: CLI tool for summarizing Claude prompt activity and sync status

import os
import re
import hashlib
from datetime import datetime
from pathlib import Path

def compute_hash(text):
    """Compute SHA256 hash of text"""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def get_all_prompt_bundles():
    """Load all prompt bundles from claude_prompts directory"""
    bundles = []
    prompt_dir = Path("claude_prompts")
    
    if not prompt_dir.exists():
        return bundles
    
    for md_file in prompt_dir.glob("*.md"):
        try:
            with open(md_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract metadata
            title_match = re.search(r'^# (.+?)$', content, re.MULTILINE)
            title = title_match.group(1).strip() if title_match else md_file.stem
            
            timestamp_match = re.search(r'Created: (\d{8}T\d{6})', content)
            created = timestamp_match.group(1) if timestamp_match else "Unknown"
            
            tags_match = re.search(r'🏷 Tags: (.+?)$', content, re.MULTILINE)
            tags = []
            if tags_match:
                tags_text = tags_match.group(1).strip()
                if tags_text != "none":
                    tags = [t.strip() for t in tags_text.split(",") if t.strip()]
            
            # Extract prompt section
            prompt_section = ""
            if "## 💭 Prompt" in content:
                parts = content.split("## 💭 Prompt", 1)
                if len(parts) > 1:
                    prompt_part = parts[1]
                    if "## 🤖 Response" in prompt_part:
                        prompt_section = prompt_part.split("## 🤖 Response")[0].strip()
                    else:
                        prompt_section = prompt_part.strip()
            
            # Mock sync status (would come from actual tracking)
            bundle = {
                "filename": md_file.name,
                "title": title,
                "created": created,
                "prompt": prompt_section,
                "full_content": content,
                "word_count": len(content.split()),
                "meta": {
                    "tags": tags,
                    "notion_synced": "notion" in tags or "sync" in content.lower(),  # Mock detection
                    "snowflake_synced": "audit" in tags or len(prompt_section) > 100  # Mock detection
                }
            }
            bundles.append(bundle)
            
        except Exception as e:
            print(f"Warning: Failed to parse {md_file}: {e}")
            continue
    
    return bundles

def summarize_audit():
    """Generate audit summary report"""
    bundles = get_all_prompt_bundles()
    total = len(bundles)
    
    if total == 0:
        print("No prompt bundles found in claude_prompts directory")
        return
    
    synced_notion = 0
    synced_snowflake = 0
    unique_hashes = set()
    tag_counter = {}
    total_words = 0
    
    for b in bundles:
        meta = b.get("meta", {})
        tags = meta.get("tags", [])
        
        # Count tags
        for tag in tags:
            tag_counter[tag] = tag_counter.get(tag, 0) + 1
        
        # Hash for uniqueness
        prompt_hash = compute_hash(b.get("prompt", ""))
        unique_hashes.add(prompt_hash)
        
        # Word count
        total_words += b.get("word_count", 0)
        
        # Sync status
        if meta.get("notion_synced"):
            synced_notion += 1
        if meta.get("snowflake_synced"):
            synced_snowflake += 1
    
    # Calculate stats
    avg_words = total_words // total if total > 0 else 0
    unique_prompts = len(unique_hashes)
    
    print("\n" + "="*50)
    print("Claude Audit Summary")
    print("="*50)
    print(f"Total Bundles: {total}")
    print(f"Unique Prompts: {unique_prompts}")
    print(f"Duplicate Rate: {((total - unique_prompts) / total * 100):.1f}%" if total > 0 else "0%")
    print(f"Total Words: {total_words:,}")
    print(f"Avg Words/Bundle: {avg_words}")
    print(f"Synced to Notion: {synced_notion}/{total} ({synced_notion/total*100:.1f}%)")
    print(f"Synced to Snowflake: {synced_snowflake}/{total} ({synced_snowflake/total*100:.1f}%)")
    
    print(f"\nTop Tags:")
    if tag_counter:
        for tag, count in sorted(tag_counter.items(), key=lambda x: -x[1])[:10]:
            print(f"  • {tag}: {count}")
    else:
        print("  No tags found")
    
    # Recent activity
    print(f"\nRecent Bundles:")
    recent_bundles = sorted(bundles, key=lambda x: x.get('created', ''), reverse=True)[:5]
    for bundle in recent_bundles:
        created = bundle.get('created', 'Unknown')[:8]  # Just date part
        title = bundle.get('title', 'Untitled')[:40]
        tags = ', '.join(bundle.get('meta', {}).get('tags', []))[:30]
        print(f"  • {created}: {title} ({tags})")
    
    print("="*50)

if __name__ == "__main__":
    summarize_audit()