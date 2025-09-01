# File: run_tool.py
# Title: Unified Claude + ZenMarket Orchestrator
# Commit Notes:
# - Loads commands from orchestrator.yaml
# - Adds ZenMarket forecast stages + batch groups
# - --list, batch ops, robust errors, summary

import argparse
import subprocess
import sys
import os
import yaml
import shlex
from datetime import datetime

CONFIG_FILE = "orchestrator.yaml"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"ERROR: Missing {CONFIG_FILE}. Create it first.")
        sys.exit(1)
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR: Failed to load {CONFIG_FILE}: {e}")
        sys.exit(1)

def run_command(name, cmd):
    print(f"\nRunning: {name}")
    print(f"Command: {cmd}")
    start = datetime.now()
    
    try:
        # Handle shell commands with SHELL: prefix
        shell = False
        if isinstance(cmd, str) and cmd.strip().startswith("SHELL:"):
            shell = True
            cmd = cmd.replace("SHELL:", "", 1).strip()
        
        # Execute command
        if shell:
            proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        else:
            proc = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
        
        duration = (datetime.now() - start).total_seconds()
        
        if proc.returncode == 0:
            print(f"SUCCESS: {name} completed ({duration:.1f}s)")
            if proc.stdout.strip():
                print(f"Output: {proc.stdout.strip()[:200]}...")
            return True
        else:
            print(f"ERROR: {name} failed (exit {proc.returncode}, {duration:.1f}s)")
            if proc.stderr.strip():
                print(f"Error: {proc.stderr.strip()[:200]}...")
            return False
            
    except Exception as e:
        print(f"ERROR: {name} crashed: {e}")
        return False

def main():
    # Load configuration
    cfg = load_config()
    modules = cfg.get("modules", {})
    groups = cfg.get("groups", {})
    
    # Build argument parser
    parser = argparse.ArgumentParser(
        description="Claude + ZenMarket Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--list", action="store_true", help="List available modules and groups")
    
    # Add module flags dynamically
    for key in modules.keys():
        parser.add_argument(f"--{key}", action="store_true", help=f"Run module '{key}'")
    
    # Add group flags dynamically  
    for g in groups.keys():
        parser.add_argument(f"--{g}", action="store_true", help=f"Run group '{g}'")
    
    args = parser.parse_args()
    
    # Handle list command
    if args.list:
        print("\nModules:")
        for k, v in modules.items():
            cmd_display = v[:50] + "..." if len(v) > 50 else v
            print(f"  --{k:20} : {cmd_display}")
        
        print("\nGroups:")
        for g, module_list in groups.items():
            print(f"  --{g:20} : {', '.join(module_list)}")
        
        return
    
    # Collect selected modules
    selected = []
    
    # Process group flags first (expand to individual modules)
    for g in groups.keys():
        if getattr(args, g, False):
            selected.extend(groups[g])
    
    # Process individual module flags
    for k in modules.keys():
        if getattr(args, k, False):
            selected.append(k)
    
    # Remove duplicates while preserving order
    seen = set()
    run_list = []
    for k in selected:
        if k not in seen:
            seen.add(k)
            run_list.append(k)
    
    if not run_list:
        print("No modules selected. Use --list to see options.")
        return
    
    # Execute selected modules
    print("Claude + ZenMarket Orchestrator")
    print("=" * 40)
    print(f"Run plan: {', '.join(run_list)}")
    
    success_list = []
    failed_list = []
    
    for module_name in run_list:
        cmd = modules.get(module_name)
        if not cmd:
            print(f"WARNING: Unknown module '{module_name}' (check {CONFIG_FILE})")
            failed_list.append(module_name)
            continue
        
        if run_command(module_name, cmd):
            success_list.append(module_name)
        else:
            failed_list.append(module_name)
    
    # Summary
    print("\n" + "=" * 40)
    print("Execution Summary:")
    print(f"SUCCESS: {', '.join(success_list) if success_list else 'None'}")
    print(f"FAILED:  {', '.join(failed_list) if failed_list else 'None'}")
    print(f"Total:   {len(success_list)}/{len(run_list)} completed successfully")
    print("=" * 40)

if __name__ == "__main__":
    main()