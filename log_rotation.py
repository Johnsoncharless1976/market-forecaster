#!/usr/bin/env python3
"""
Log rotation and retention system for ZenMarket AI
Rotates access logs daily and maintains 30-day retention
"""

import os
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path

def rotate_access_logs():
    """Rotate VIZ_ACCESS.md logs daily"""
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    # Find yesterday's access logs
    daily_dirs = list(Path("audit_exports/daily").glob(f"{yesterday}_*"))
    
    rotated_files = []
    total_size = 0
    
    for dir_path in daily_dirs:
        access_log = dir_path / "VIZ_ACCESS.md"
        
        if access_log.exists():
            # Create compressed archive
            compressed_name = f"VIZ_ACCESS_{yesterday}.md.gz"
            compressed_path = dir_path / compressed_name
            
            with open(access_log, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Remove original
            access_log.unlink()
            
            # Track rotated files
            size = compressed_path.stat().st_size
            rotated_files.append({
                'file': compressed_name,
                'path': str(compressed_path),
                'size': size
            })
            total_size += size
            
            print(f"📦 Rotated: {access_log.name} → {compressed_name} ({size} bytes)")
    
    return rotated_files, total_size

def cleanup_old_logs():
    """Remove logs older than 30 days"""
    cutoff_date = datetime.now() - timedelta(days=30)
    cutoff_str = cutoff_date.strftime('%Y%m%d')
    
    removed_files = []
    
    # Find old compressed logs
    for log_file in Path("audit_exports/daily").rglob("VIZ_ACCESS_*.md.gz"):
        try:
            # Extract date from filename
            date_str = log_file.stem.replace('VIZ_ACCESS_', '').replace('.md', '')
            
            if date_str < cutoff_str:
                size = log_file.stat().st_size
                removed_files.append({
                    'file': log_file.name,
                    'size': size
                })
                
                log_file.unlink()
                print(f"🗑️  Removed old log: {log_file.name} ({size} bytes)")
        
        except Exception as e:
            print(f"⚠️  Error processing {log_file}: {e}")
    
    return removed_files

def get_retained_logs():
    """Get list of all retained log files with sizes"""
    retained_files = []
    
    # Current access logs
    for access_log in Path("audit_exports/daily").rglob("VIZ_ACCESS.md"):
        size = access_log.stat().st_size
        retained_files.append({
            'file': f"{access_log.parent.name}/VIZ_ACCESS.md",
            'size': size,
            'type': 'current'
        })
    
    # Compressed archives
    for compressed_log in Path("audit_exports/daily").rglob("VIZ_ACCESS_*.md.gz"):
        size = compressed_log.stat().st_size
        retained_files.append({
            'file': f"{compressed_log.parent.name}/{compressed_log.name}",
            'size': size,
            'type': 'compressed'
        })
    
    return retained_files

def update_daily_index():
    """Update daily index with latest access log link"""
    index_path = "audit_exports/daily/INDEX.md"
    
    # Find latest access log
    latest_access_log = None
    for access_log in sorted(Path("audit_exports/daily").rglob("VIZ_ACCESS.md"), reverse=True):
        latest_access_log = access_log
        break
    
    if latest_access_log and os.path.exists(index_path):
        # Read current index
        with open(index_path, 'r') as f:
            content = f.read()
        
        # Add/update access log link
        access_line = f"**Access Log**: [VIZ_ACCESS.md]({latest_access_log.parent.name}/VIZ_ACCESS.md)"
        
        if "**Access Log**:" in content:
            # Update existing line
            lines = content.split('\\n')
            for i, line in enumerate(lines):
                if line.startswith("**Access Log**:"):
                    lines[i] = access_line
                    break
            content = '\\n'.join(lines)
        else:
            # Add new line after Pipeline badge
            lines = content.split('\\n')
            for i, line in enumerate(lines):
                if line.startswith("**Pipeline(main)**:"):
                    lines.insert(i + 1, "")
                    lines.insert(i + 2, access_line)
                    break
            content = '\\n'.join(lines)
        
        # Write updated index
        with open(index_path, 'w') as f:
            f.write(content)
        
        print(f"📝 Updated index with latest access log link")

def main():
    """Main log rotation function"""
    print("🔄 Starting log rotation and retention...")
    
    # Rotate yesterday's logs
    rotated_files, rotated_size = rotate_access_logs()
    
    # Cleanup old logs
    removed_files = cleanup_old_logs()
    
    # Get current retention status
    retained_files = get_retained_logs()
    
    # Update daily index
    update_daily_index()
    
    # Print summary
    print(f"\\n📊 Log Rotation Summary:")
    print(f"  - Rotated: {len(rotated_files)} files ({rotated_size} bytes)")
    print(f"  - Removed: {len(removed_files)} files")
    print(f"  - Retained: {len(retained_files)} files")
    
    print(f"\\n📁 Retained Files:")
    for file_info in retained_files:
        file_type = "📄" if file_info['type'] == 'current' else "📦"
        print(f"  {file_type} {file_info['file']} ({file_info['size']} bytes)")
    
    return retained_files

if __name__ == "__main__":
    main()