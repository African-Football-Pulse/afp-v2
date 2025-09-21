import json
import os
from src.storage import azure_blob

EPL_SQUADS_BLOB = "meta/2025-2026/epl_squads.json"
AFRICA_CODES_PATH = "config/africa_fifa_codes.json"
OUTPUT_PATH = "players/africa/players_by_club.json"

def build_players_by_club():
    container = os.environ.get("AZURE_STORAGE_CONTAINER")

    # Load EPL squads from Azure
    squads = azure_blob.get_json(container, EPL_SQUADS_BLOB)

    # Load Africa FIFA codes from local config
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

    # Save result back to Azure
    azure_blob.upload_json(container, OUTPUT_PATH, result)
    print(f"[build_africa_lists] Saved {len(result)} clubs with African players → {OUTPUT_PATH}")

if __name__ == "__main__":
    build_players_by_club()
