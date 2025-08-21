# src/log_time.py
import argparse
from datetime import datetime
from pathlib import Path

# --- Resolve repo root (one directory up from /src)
ROOT = Path(__file__).resolve().parent.parent
LOG_FILE = ROOT / "project_time_log.md"

def log_time(entry_date, hours, notes):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n**{entry_date}** – {hours}h  \n{notes}\n")

def summarize_log():
    if not LOG_FILE.exists():
        print("⚠️ No log file found yet.")
        return

    total_hours = 0.0
    print("\n📊 Project Time Summary\n" + "-"*30)
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if "–" in line and "h" in line:
                try:
                    hours = float(line.split("–")[1].split("h")[0].strip())
                    total_hours += hours
                    print(line.strip())
                except:
                    continue
    print("-"*30)
    print(f"Total = {round(total_hours, 2)}h\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append or summarize time log")
    parser.add_argument("--date", help="Date (YYYY-MM-DD) or leave blank for now",
                        default=datetime.now().strftime("%Y-%m-%d %H:%M"))
    parser.add_argument("--hours", type=float, help="Hours spent")
    parser.add_argument("--notes", type=str, help="Notes on work done")
    parser.add_argument("--summary", action="store_true", help="Show summary of log file")
    args = parser.parse_args()

    if args.summary:
        summarize_log()
    else:
        # Defaults if args not provided
        hours = args.hours if args.hours is not None else 1
        notes = args.notes if args.notes is not None else "General work session"
        log_time(args.date, hours, notes)
        print(f"✅ Logged {hours}h for {args.date}: {notes}")
