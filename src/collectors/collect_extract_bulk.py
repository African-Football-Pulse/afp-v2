import argparse
import os
import yaml
import json
from src.storage import azure_blob


def run_bulk(season: str, leagues: list):
    """
    Kör bulk-extraktion för alla ligor/cuper i en given säsong.
    Läser manifest.json för varje liga/cup och laddar upp matchfiler till Azure.
    """
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    print(f"[bulk_extract] Starting bulk extract for season {season}", flush=True)

    total_exported = 0
    total_skipped = 0

    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]
        is_cup = league.get("is_cup", False)

        manifest_path = f"stats/{season}/{league_id}/manifest.json"
        try:
            manifest_text = azure_blob.get_text(container, manifest_path)
        except Exception:
            print(f"[bulk_extract] ⚠️ No manifest found for {league_name} (id={league_id})", flush=True)
            continue

        try:
            manifest = json.loads(manifest_text)
        except Exception as e:
            print(f"[bulk_extract] ⚠️ Could not parse manifest for {league_name} ({e})", flush=True)
            continue

        matches = []

        if is_cup:
            # Cup: kan vara dict med results eller lista av stages
            if isinstance(manifest, dict) and isinstance(manifest.get("results"), list):
                for stage in manifest["results"]:
                    matches.extend(stage.get("matches", []))
            elif isinstance(manifest, list):
                for stage in manifest:
                    matches.extend(stage.get("matches", []))
        else:
            # Liga: kan vara dict (results direkt) eller lista (stages med matcher)
            if isinstance(manifest, dict) and isinstance(manifest.get("results"), list):
                matches = manifest["results"]
            elif isinstance(manifest, list):
                for league_data in manifest:
                    for stage in league_data.get("stage", []):
                        matches.extend(stage.get("matches", []))

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

                # Hoppa över om filen redan finns
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
    parser.add_argument(
        "--mode",
        type=str,
        choices=["league", "cup", "all"],
        default="all",
        help="Filter to run only leagues, only cups, or all"
    )
    args = parser.parse_args()

    # Ladda config
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    leagues = [l for l in cfg.get("leagues", []) if l.get("enabled", False)]

    if args.mode == "league":
        leagues = [l for l in leagues if not l.get("is_cup", False)]
    elif args.mode == "cup":
        leagues = [l for l in leagues if l.get("is_cup", False)]

    run_bulk(args.season, leagues)
