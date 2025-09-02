# produce_auto.py
import os, sys, yaml
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.common.blob_io import BlobIO
from src.produce_section import run as run_section

TZ = ZoneInfo("Europe/Stockholm")

def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()

def main():
    sas = os.getenv("BLOB_CONTAINER_SAS_URL")
    if not sas:
        print("[FATAL] BLOB_CONTAINER_SAS_URL is missing. Produce requires Azure Blob access.", flush=True)
        sys.exit(1)

    blob = BlobIO(sas)

    # Läs planfil
    with open("config/produce_plan.yaml", "r", encoding="utf-8") as f:
        plan = yaml.safe_load(f)

    defaults = plan.get("defaults", {})
    tasks = plan.get("tasks", [])

    default_date = defaults.get("date") or today_str()
    league_default = defaults.get("league")

    for t in tasks:
        section = t["section_code"]
        league = t.get("league") or league_default
        date = t.get("date") or default_date
        source = t["source"]

        # expandera ev. {{today}}
        if isinstance(date, str) and "{{today}}" in date:
            date = today_str()

        in_path = f"curated/news/{source}/{league}/{date}/items.json"
        out_path = f"curated/produce/{section}/{league}/{date}/manifest.json"

        print(f"[Produce] Section={section} League={league} Date={date} Source={source}")
        print(f"[Produce] Reading: {in_path}")

        try:
            items = blob.read_json(in_path)
        except Exception as e:
            print(f"[WARN] Could not read items for {section}: {e}")
            continue

        if not items:
            print(f"[WARN] No items for {section} {league} {date}")
            continue

        manifest = run_section(section, items, league, date, defaults)
        print(f"[Produce] Writing: {out_path}")
        blob.write_json(out_path, manifest)

    print("[Produce] DONE")

if __name__ == "__main__":
    main()
