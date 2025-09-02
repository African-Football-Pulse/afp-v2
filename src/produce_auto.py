import os, sys, yaml
from utils.blob import BlobIO   # antar att du har en blob-helper

def main():
    sas = os.getenv("BLOB_CONTAINER_SAS_URL")
    if not sas:
        print("[FATAL] BLOB_CONTAINER_SAS_URL is missing. Produce requires Azure Blob access.", flush=True)
        sys.exit(1)

    # Initiera BlobIO
    blob = BlobIO(sas)

    # Läs planfil
    with open("config/produce_plan.yaml", "r", encoding="utf-8") as f:
        plan = yaml.safe_load(f)

    defaults = plan.get("defaults", {})
    tasks = plan.get("tasks", [])

    for t in tasks:
        section = t["section_code"]
        league = t.get("league") or defaults.get("league")
        date = t.get("date") or defaults.get("date")
        source = t["source"]

        in_path = f"curated/news/{source}/{league}/{date}/items.json"
        out_path = f"curated/produce/{section}/{league}/{date}/manifest.json"

        print(f"[Produce] Section={section} League={league} Date={date} Source={source}")
        print(f"[Produce] Reading: {in_path}")
        items = blob.read_json(in_path)

        if not items:
            print(f"[WARN] No items for {section} {league} {date}")
            continue

        manifest = run_section(section, items, league, date, defaults)
        print(f"[Produce] Writing: {out_path}")
        blob.write_json(out_path, manifest)

def run_section(section_code, items, league, date, defaults):
    # TODO: kalla produce_section.run() eller motsvarande
    # och returnera manifest som dict
    from src.produce_section import run
    return run(section_code, items, league, date, defaults)

if __name__ == "__main__":
    main()
