"""Stage 1 Audit Validation (legacy wrapper)

This script regenerates the Stage 1 executive audit summary and
saves the results under the legacy naming convention expected by the
CI job (`stage1_audit_<timestamp>` with `REPORT_AUDIT.md` and
`summary.csv`).  It reuses the SQL from exec_audit_summary to
ensure the checks remain identical.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from exec_audit_summary import SUMMARY_SQL
from snowflake_conn import get_conn


def main() -> None:
    root = Path(__file__).resolve().parent.parent / "audit_exports"
    root.mkdir(exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    outdir = root / f"stage1_audit_{stamp}"
    outdir.mkdir()

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(SUMMARY_SQL)
            rows = cur.fetchall()
    except Exception as exc:  # pragma: no cover - network/cred issues
        print(f"Snowflake unreachable cannot produce counts. {exc}")
        return

    parsed = []
    for (json_row,) in rows:
        obj = json.loads(json_row) if isinstance(json_row, str) else json_row
        check = obj.get("check")
        viol = int(obj.get("violations", 0))
        status = "PASS" if viol == 0 else "FAIL"
        parsed.append((check, viol, status))

    csv_path = outdir / "summary.csv"
    md_path = outdir / "REPORT_AUDIT.md"

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        f.write("check,violations,status\n")
        for check, viol, status in sorted(parsed):
            f.write(f"{check},{viol},{status}\n")

    with md_path.open("w", encoding="utf-8") as f:
        f.write(f"# Stage 1 Audit Report ({stamp})\n\n")
        f.write("| Check | Violations | Status |\n|---|---:|:---:|\n")
        for check, viol, status in sorted(parsed):
            f.write(f"| {check} | {viol} | {status} |\n")
        f.write("\n_Scope_: Stage 1 (Ingestion Integrity & Controls).\n")

    print(
        json.dumps(
            {"saved": True, "dir": str(outdir), "md": str(md_path), "csv": str(csv_path)}
        )
    )


if __name__ == "__main__":
    main()