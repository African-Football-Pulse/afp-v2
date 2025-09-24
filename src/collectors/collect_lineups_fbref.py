import os
import json
import soccerdata as sd
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"

    # ⚙️ Miljövariabler (för enkel test)
    season = os.environ.get("SEASON", "2024-2025")
    league_id = os.environ.get("LEAGUE", "228")  # Premier League som exempel

    # 🔎 Ladda manifestet för ligan & säsongen
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        manifest = load_json_from_blob(container, manifest_path)
    except Exception as e:
        print(f"[collect_lineups_fbref] ❌ Kunde inte läsa manifest {manifest_path}: {e}")
        return

    matches = manifest.get("matches", [])
    if not matches:
        print(f"[collect_lineups_fbref] ⚠️ Inga matcher i manifestet {manifest_path}")
        return

    print(f"[collect_lineups_fbref] Hittade {len(matches)} matcher i manifestet")

    # 📥 Initiera FBref-collector (no cache/store eftersom vi kör i container)
    fbref = sd.FBref(leagues="ENG-Premier League", seasons=2024, no_cache=True, no_store=True)

    processed = []
    limit = 3  # bara testa på de första 3 matcherna

    for i, match in enumerate(matches[:limit], start=1):
        match_id = str(match.get("id"))
        if not match_id:
            continue

        try:
            df_lineup = fbref.read_lineup(match_id=match_id)
        except Exception as e:
            print(f"[collect_lineups_fbref] ⚠️ Kunde inte hämta lineup för match {match_id}: {e}")
            continue

        # Konvertera till JSON
        lineup_json = df_lineup.reset_index().to_dict(orient="records")

        output_path = f"stats/{season}/{league_id}/{match_id}/lineups.json"
        azure_blob.upload_json(container, output_path, lineup_json)
        processed.append(match_id)

        print(f"[collect_lineups_fbref] ✅ Sparade lineup för match {match_id} → {output_path}")

    # 📦 Manifest för lineups
    if processed:
        manifest_out = {
            "season": season,
            "league_id": league_id,
            "matches": processed,
        }
        manifest_path_out = f"stats/{season}/{league_id}/lineups_manifest.json"
        azure_blob.upload_json(container, manifest_path_out, manifest_out)
        print(f"[collect_lineups_fbref] ✅ Skrev lineup-manifest → {manifest_path_out}")


if __name__ == "__main__":
    main()
