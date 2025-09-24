import os
import json
import soccerdata as sd
import pandas as pd
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"
    season = os.environ.get("SEASON", "2024-2025")
    league_id = os.environ.get("LEAGUE", "228")  # EPL default

    # 🔎 Ladda vårt manifest
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    manifest = load_json_from_blob(container, manifest_path)

    # Plocka matcher ur manifest
    matches = []
    for league in manifest:
        for stage in league.get("stage", []):
            matches.extend(stage.get("matches", []))

    print(f"[map_fbref_match_ids] Hittade {len(matches)} matcher i manifestet")

    # 📥 Läs FBref schedule
    fbref = sd.FBref(leagues="ENG-Premier League", seasons=2024, no_cache=True, no_store=True)
    df_schedule = fbref.read_schedule().reset_index()

    # Normalisera kolumner för enklare matchning
    df_schedule["date_str"] = pd.to_datetime(df_schedule["date"]).dt.strftime("%d/%m/%Y")
    df_schedule["home_team"] = df_schedule["home_team"].str.strip()
    df_schedule["away_team"] = df_schedule["away_team"].str.strip()

    mapping = {}
    unmatched = []

    for m in matches:
        mid = str(m.get("id"))
        date = m.get("date")
        home = (m.get("teams", {}).get("home") or {}).get("name", "").strip()
        away = (m.get("teams", {}).get("away") or {}).get("name", "").strip()

        df_match = df_schedule[
            (df_schedule["date_str"] == date) &
            (df_schedule["home_team"].str.contains(home, case=False, na=False)) &
            (df_schedule["away_team"].str.contains(away, case=False, na=False))
        ]

        if not df_match.empty:
            fbref_id = str(df_match.iloc[0].get("game"))  # unikt FBref-match-id
            mapping[mid] = fbref_id
            print(f"[map_fbref_match_ids] ✅ Matchad {home} vs {away} ({date}) → {fbref_id}")
        else:
            unmatched.append({"id": mid, "date": date, "home": home, "away": away})
            print(f"[map_fbref_match_ids] ⚠️ Ingen match för {home} vs {away} ({date})")

    # 📦 Ladda upp mapping till Azure
    output_path = f"meta/fbref_match_ids_{season}_{league_id}.json"
    azure_blob.upload_json(container, output_path, mapping)
    print(f"[map_fbref_match_ids] ✅ Sparade {len(mapping)} mappingar → {output_path}")

    # 📄 Spara även unmatched för felsökning
    if unmatched:
        unmatched_path = f"meta/fbref_unmatched_{season}_{league_id}.json"
        azure_blob.upload_json(container, unmatched_path, unmatched)
        print(f"[map_fbref_match_ids] ⚠️ Sparade {len(unmatched)} omatchade matcher → {unmatched_path}")


if __name__ == "__main__":
    main()
