# File: src/log_audit_event.py
# Title: Audit Utility  Write generic event to AUDIT_LOG
# Commit Notes:
# - Inserts an event row with stage, job, status, message, and optional context JSON.
# - For failures, set --status FAIL and pass --error "message".
# - No secrets; VARIANT fields via parse_json.

import argparse, json
from datetime import datetime, timezone
from snowflake_conn import get_conn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True)
    ap.add_argument("--job", required=True)
    ap.add_argument("--status", choices=["OK","FAIL","INFO"], default="INFO")
    ap.add_argument("--message", default="")
    ap.add_argument("--error", default=None)
    ap.add_argument("--context", default=None, help="JSON string with extra fields")
    args = ap.parse_args()

    input_obj = {
        "status": args.status,
        "message": args.message,
        "error": args.error,
        "ts": datetime.now(timezone.utc).isoformat()
    }
    if args.context:
        try:
            extra = json.loads(args.context)
            if isinstance(extra, dict):
                input_obj.update(extra)
        except Exception:
            input_obj["context_raw"] = args.context

    output_obj = {"ack": True}

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO AUDIT_LOG(stage, run_timestamp, input_data, output_data, pipeline_job_id, sha256_hash)
            SELECT %s, CURRENT_TIMESTAMP(), parse_json(%s), parse_json(%s), %s, %s
        """, (
            args.stage,
            json.dumps(input_obj, separators=(",",":")),
            json.dumps(output_obj, separators=(",",":")),
            args.job,
            ""  # no checksum for generic events
        ))
        conn.commit()
        print({"event":"logged","stage":args.stage,"job":args.job,"status":args.status})

if __name__ == "__main__":
    main()
