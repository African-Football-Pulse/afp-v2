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

    summary = []
    total_matches = 0

    for league in leagues:
        league_id = league["id"]
        name = league["name"]
        is_cup = league.get("is_cup", False)

        manifest_path = f"stats/{season}/{league_id}/manifest.json"

        try:
            manifest_text = azure_blob.get_text(container, manifest_path)
        except Exception:
            print(f"[bulk_extract] ⚠️ Manifest not found: {manifest_path}")
            summary.append((name, league_id, 0, "no manifest"))
            continue

        manifest = json.loads(manifest_text)

        matches = []

        if is_cup:
            # Cup-struktur: results = list of stages, each with matches
            for stage in manifest.get("results", []):
                matches.extend(stage.get("matches", []))
        else:
            # Liga-struktur: results = list of matches direkt
            matches = manifest.get("results", [])

        if not matches:
            print(f"[bulk_extract] ⚠️ No matches found in manifest for {name} (league_id={league_id})")
            summary.append((name, league_id, 0, "empty manifest"))
            continue

        exported = 0
        skipped = 0

        print(f"[bulk_extract] {name} (league_id={league_id}): {len(matches)} matches in manifest")

        for match in matches:
            match_id = match.get("id")
            if not match_id:
                print(f"[bulk_extract] ⚠️ Skipped match without id in {name} ({season})")
                continue

            blob_path = f"stats/{season}/{league_id}/{match_id}.json"

            # Skip om fil redan finns
            try:
                azure_blob.get_text(container, blob_path)
                skipped += 1
                continue
            except Exception:
                pass  # filen finns inte, kör vidare

            azure_blob.upload_json(container, blob_path, match)
            exported += 1

        total_matches += exported
        summary.append((name, league_id, exported, f"skipped {skipped}" if skipped else "ok"))

    print("\n[bulk_extract] ✅ Done for season", season)
    print("=== Summary ===")
    for name, league_id, count, note in summary:
        print(f"{name:25} (id={league_id}): {count} matches exported ({note})")
    print(f"TOTAL: {total_matches} matches exported across {len(leagues)} leagues")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--season",
        type=str,
        required=True,
        help="Season string, e.g. 2024-2025"
    )
    args = parser.parse_args()

    run_bulk(args.season)
