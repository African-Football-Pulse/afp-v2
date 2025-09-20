import argparse
import os
import yaml
from src.storage import azure_blob


def run_bulk(season: str):
    """
    Kör extract för ALLA ligor i en given säsong (baserat på config/leagues.yaml).
    Läser manifest.json för varje liga och laddar upp matchfilerna till Azure.
    """
    print(f"[bulk_extract] Starting bulk extract for season {season}", flush=True)

    # Läs ligorna från config
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    total_exported = 0
    total_skipped = 0

    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        league_name = league["name"]

        manifest_path = f"stats/{season}/{league_id}/manifest.json"

        try:
            manifest_text = azure_blob.get_text(container, manifest_path)
        except Exception:
            print(f"[bulk_extract] ⚠️ No manifest for {league_name} ({league_id})", flush=True)
            continue

        try:
            import json
            manifest = json.loads(manifest_text)
        except Exception as e:
            print(f"[bulk_extract] ⚠️ Failed to parse manifest for {league_name}: {e}", flush=True)
            continue

        # Anta standardstruktur
        matches = []
        if isinstance(manifest, dict) and isinstance(manifest.get("results"), list):
            matches = manifest["results"]
        elif isinstance(manifest, list):
            matches = manifest

        print(f"[bulk_extract] {league_name} (league_id={league_id}): {len(matches)} matches in manifest", flush=True)

        exported = 0
        skipped = 0

        if matches:
            print(f"[bulk_extract]   -> extracting {len(matches)} matches...", flush=True)

            for match in matches:
                match_id = match.get("id")
                if not match_id:
                    skipped += 1
                    continue

                blob_path = f"stats/{season}/{league_id}/{match_id}.json"

                # Om filen redan finns, hoppa över
                try:
                    azure_blob.get_text(container, blob_path)
                    skipped += 1
                    continue
                except Exception:
                    pass

                azure_blob.upload_json(container, blob_path, match)
                exported += 1

        print(f"[bulk_extract]   Done: {exported} exported, {skipped} skipped\n", flush=True)
        total_exported += exported
        total_skipped += skipped

    print("=== Summary ===", flush=True)
    print(f"TOTAL exported: {total_exported}", flush=True)
    print(f"TOTAL skipped: {total_skipped}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=str, required=True, help="Season string, e.g. 2024-2025")
    args = parser.parse_args()

    run_bulk(args.season)
