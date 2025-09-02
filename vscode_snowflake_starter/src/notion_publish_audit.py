import os, csv, json, datetime as dt
from notion_client import Client

def _find_title_prop(db):
    # Return the name of the database's title property (Notion requires one)
    for prop_name, meta in db["properties"].items():
        if meta.get("type") == "title":
            return prop_name
    # Fallback to common default
    return "Name"

def read_summary(audit_dir: str):
    path = os.path.join(audit_dir, "summary.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"summary.csv not found at {path}")
    out = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            out[row["check"]] = {"violations": int(row["violations"]), "status": row["status"]}
    return out

def main():
    token  = os.environ["NOTION_TOKEN"]
    db_id  = os.environ["NOTION_AUDIT_DB_ID"]  # <- just one Notion DB to receive audit entries
    audit_dir = os.environ.get("AUDIT_DIR", "").strip()
    if not audit_dir:
        raise RuntimeError("AUDIT_DIR is empty; make sure the audit job exported exec.env and this job 'needs' it")

    summary = read_summary(audit_dir)
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Assemble a short title & a code-block body that always works with any DB schema
    title_text = f"Stage 1 Audit — {now}"
    body_md = [
        f"*CI project:* {os.environ.get('CI_PROJECT_PATH','')}",
        f"*Pipeline:* {os.environ.get('CI_PIPELINE_ID','')}",
        f"*Job URL:* {os.environ.get('CI_JOB_URL','')}",
        f"*Audit dir:* {audit_dir}",
        "",
        "```json",
        json.dumps(summary, indent=2),
        "```",
    ]
    notion = Client(auth=token)

    # Discover the title property so we don't depend on DB column names
    db = notion.databases.retrieve(database_id=db_id)
    title_prop = _find_title_prop(db)

    # Create a page with just a title + children blocks (works with almost any DB)
    notion.pages.create(
        parent={"database_id": db_id},
        properties={
            title_prop: {"title": [{"type": "text", "text": {"content": title_text}}]}
        },
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "\n".join(body_md)}}]},
            }
        ],
    )

if __name__ == "__main__":
    main()
