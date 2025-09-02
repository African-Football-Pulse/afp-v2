# src/produce_auto.py
import os, sys, json, yaml
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.storage.azure_blob import get_text, put_text
from src.produce_section import run as run_section

TZ = ZoneInfo("Europe/Stockholm")

def today_str() -> str:
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[FATAL] Missing env: {name}", flush=True)
        sys.exit(1)
    return v

def read_json_blob(container: str, path: str):
    txt = get_text(container, path)  # raises on 404/perm
    return json.loads(txt)

def write_json_blob(container: str, path: str, obj):
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    put_text(container, path, payload, content_type="application/json; charset=utf-8")

def main():
    # Hårda krav: konto + container + (KEY eller SAS) hanteras i azure_blob._client()
    require_env("AZURE_STORAGE_ACCOUNT")
    container = require_env("AZURE_BLOB_CONTAINER")
    prefix = os.getenv("BLOB_PREFIX", "")  # t.ex. "collector/" eller tomt

    # Läs plan
    with open("config/produce_plan.yaml", "r", encoding="utf-8") as f:
        plan = yaml.safe_load(f) or {}

    defaults = plan.get("defaults", {}) or {}
    tasks = plan.get("tasks", []) or []

    default_league = defaults.get("league")
    default_date = defaults.get("date") or today_str()

    def expand_date(d):
        if isinstance(d, str) and "{{today}}" in d:
            return today_str()
        return d or default_date

    for t in tasks:
        section_code = t["section_code"]
        source = t["source"]
        league = t.get("league") or default_league
        date = expand_date(t.get("date"))

        in_path  = f"{prefix}curated/news/{source}/{league}/{date}/items.json"
        out_path = f"{prefix}curated/produce/{section_code}/{league}/{date}/manifest.json"

        print(f"[Produce] Section={section_code}  League={league}  Date={date}  Source={source}")
        print(f"[Produce] Reading:  {container}/{in_path}")

        try:
            items = read_json_blob(container, in_path)
        except Exception as e:
            print(f"[WARN] Could not read items ({in_path}): {e}")
            continue

        if not items:
            print(f"[WARN] No items for {section_code} {league} {date}")
            continue

        try:
            manifest = run_section(section_code, items, league, date, defaults)
        except Exception as e:
            print(f"[ERROR] Section runner failed ({section_code}): {e}")
            continue

        print(f"[Produce] Writing: {container}/{out_path}")
        try:
            write_json_blob(container, out_path, manifest)
        except Exception as e:
            print(f"[ERROR] Failed to write manifest: {e}")
            continue

    print("[Produce] DONE")

if __name__ == "__main__":
    main()
