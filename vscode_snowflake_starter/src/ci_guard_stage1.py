import csv, sys, pathlib

base = pathlib.Path("audit_exports")
dirs = sorted((d for d in base.glob("stage1_exec_*") if d.is_dir()), key=lambda p: p.stat().st_mtime, reverse=True)
if not dirs:
    print("No stage1_exec_* directory found under audit_exports", file=sys.stderr)
    sys.exit(2)
latest = dirs[0]
csv_path = latest / "summary.csv"
if not csv_path.exists():
    print(f"summary.csv not found in {latest}", file=sys.stderr)
    sys.exit(2)

failed = []
with open(csv_path, newline="", encoding="utf-8") as f:
    r = csv.DictReader(f)
    for row in r:
        if str(row.get("status","")).upper() == "FAIL":
            failed.append(row)

if failed:
    print("Stage-1 Exec Audit FAIL rows:")
    for row in failed:
        print(row)
    sys.exit(1)

print("Stage-1 Exec Audit: PASS")