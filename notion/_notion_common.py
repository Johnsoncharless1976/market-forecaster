import os
from typing import Dict, Any
from notion_client import Client

def get_notion() -> Client:
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise RuntimeError("NOTION_TOKEN is not set in .env")
    return Client(auth=token)

def get_db_id(env_var: str) -> str:
    db_id = os.getenv(env_var, "").strip()
    if not db_id:
        raise RuntimeError(f"{env_var} is not set in .env")
    return db_id

def find_title_prop(notion: Client, database_id: str) -> str:
    db = notion.databases.retrieve(database_id=database_id)
    for name, meta in db.get("properties", {}).items():
        if meta.get("type") == "title":
            return name
    # best-effort fallback (Notion requires a title prop somewhere)
    return "Name"

def upsert_by_date(notion: Client, database_id: str, date_iso: str, title_prop: str):
    """Return page_id if a row exists for this exact date (Date property equals date_iso)."""
    # Try property named "Date" first; gracefully fallback to created_time search if absent.
    try:
        query = notion.databases.query(
            **{
                "database_id": database_id,
                "filter": {
                    "property": "Date",
                    "date": {"equals": date_iso.split("T")[0]},
                },
                "page_size": 1,
            }
        )
        results = query.get("results", [])
        if results:
            return results[0]["id"]
    except Exception:
        pass  # DB might not have a "Date" property; we will just create a new row.
    return None

def text_rich(s: str) -> Dict[str, Any]:
    return {"type": "text", "text": {"content": s[:1999]}}  # Notion per-node length safety
