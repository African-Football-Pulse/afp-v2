import json
import os

EPL_SQUADS_PATH = "meta/2025-2026/epl_squads.json"
AFRICA_CODES_PATH = "config/africa_fifa_codes.json"
OUTPUT_PATH = "players/africa/players_by_club.json"

def build_players_by_club():
    # Load EPL squads
    with open(EPL_SQUADS_PATH, "r", encoding="utf-8") as f:
        squads = json.load(f)

    # Load Africa FIFA codes
    with open(AFRICA_CODES_PATH, "r", encoding="utf-8") as f:
        africa_codes = set(json.load(f).keys())

    result = {}

    # Loop through clubs
    for club, players in squads.items():
        africa_players = []
        for p in players:
            nation = p.get("nation", "").upper()
            if nation in africa_codes:
                africa_players.append({
                    "name": p.get("name"),
                    "nation": nation,
                    "pos": p.get("pos"),
                    "no": p.get("no")
                })
        if africa_players:
            result[club] = africa_players

    # Ensure output dir exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Save result
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"[build_africa_lists] Saved {len(result)} clubs with African players → {OUTPUT_PATH}")

if __name__ == "__main__":
    build_players_by_club()
