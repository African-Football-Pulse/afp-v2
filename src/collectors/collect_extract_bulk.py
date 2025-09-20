import argparse
import os
import yaml
import json
from src.storage import azure_blob

def run_bulk(season: str, config_path="config/leagues.yaml"):
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs ligor från config
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    leagues = [l for l in cfg.get("leagues", []) if l.get("enabled", False)]

    print(f"[bulk_extract] Starting bulk extract for season {season}")

    for league in leagues:
        league_id = league["id"]
        name = league["name"]

        manifest_path = f"stats/{season}/{league_id}/manifest.json"

        try:
            manifest_text = azure_blob.get_text(container, manifest_path)
        except Exception:
            print(f"[bulk_extract] ⚠️ Manifest not found: {manifest_path}")
            continue

        manifest = json.loads(manifest_text)

        # Hantera både listor och dict med "results"
        if isinstance(manifest, list):
            matches = manifest
        else:
            matches = manifest.get("results", [])

        print(f"[bulk_extract] {name} (league_id={league_id}): {len(matches)} matches")

        for match in matches:
            match_id = match.get("id")
            if not match_id:
                continue
            blob_path = f"stats/{season}/{league_id}/{match_id}.json"
            azure_blob.upload_json(container, blob_path, match)
            print(f"[bulk_extract]   -> wrote {blob_path}")

    print(f"[bulk_extract] ✅ Done for season {season}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=str, required=True, help=_
