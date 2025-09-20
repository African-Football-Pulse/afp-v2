import argparse
import os
import yaml
from src.storage import azure_blob


def run_bulk(season: str):
    """
    Kör bulk-extraktion för alla ligor i en viss säsong.
    Hämtar manifest från Azure och exporterar matchfiler.
    """
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs in konfiguration för ligor
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = [lg for lg in cfg.get("leagues", []) if lg.get("enabled", False)]

    print(f"[bulk_extract] Starting bulk extract for season {season}", flush=True)

    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]

        manifest_path = f"stats/{season}/{league_id}/manifest.json"
        try:
            manifest_text = azure_blob.get_text(container, manifest_path)
        except Exception:
            print(f"[bulk_extract] ⚠️ No manifest found for {league_name} (id={league_id})")
            continue

        try:
            import json
            manifest = json.loads(manifest_text)
        except Exception as e:
            print(f"[bulk_extract] ⚠️ Could not parse manifest for {league_name} ({e})")
            continue

        # Hantera olika format (dict eller list)
        if isinstance(manifest, dict):
            matches = manifest.get("results", [])
        elif isinstance(manifest, list):
            matches = manifest
        else:
            print(f"[bulk_extract] ⚠️ Unexpected manifest format for {league_name}")
            continue

        print(f"[bulk_extract] {league_name} (league_id={league_id}): {len(matches)} matches in manifest")

        exported, skipped = 0, 0
        if matches:
            print(f"[bulk_extract]   -> extracting {len(matches)} matches...", flush=True)

            for match in matches:
                match_id = match.get("id")
                if not match_id:
                    skipped += 1
                    continue

                blob_path = f"stats/{season}/{league_id}/{match_id}.json"

                # Hoppa om filen redan finns
                try:
                    azure_blob.get_text(container, blob_path)
                    skipped += 1
                    continue
                except Exception:
                    pass

                azure_blob.upload_json(container, blob_path, match)
                exported += 1

        print(f"[bulk_extract]   Done: {exported} exported, {skipped} skipped\n", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=str, required=True, help="Season string, e.g. 2024-2025")
    args = parser.parse_args()

    run_bulk(args.season)
