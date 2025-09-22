import json
import os
from src.storage import azure_blob

EPL_SQUADS_BLOB = "meta/2025-2026/epl_squads.json"
AFRICA_CODES_PATH = "config/africa_fifa_codes.json"
OUTPUT_PATH = "players/africa/players_by_country.json"

def build_players_by_country():
    container = os.environ.get("AZURE_STORAGE_CONTAINER")

    # Load EPL squads from Azure
    squads = azure_blob.get_json(container, EPL_SQUADS_BLOB)

    # Load Africa FIFA codes from local config
    with open(AFRICA_CODES_PATH, "r", encoding="utf-8") as f:
        africa_codes = json.load(f)

    result = {}

    # Loop through clubs
    for club, players in squads.items():
        for p in players:
            nation = p.get("nation", "").upper()
            if nation in africa_codes:
                if nation not in result:
                    result[nation] = []
                result[nation].append({
                    "club": club,
                    "name": p.get("name"),
                    "pos": p.get("pos"),
                    "no": p.get("no")
                })

    # Save result back to Azure
    azure_blob.upload_json(container, OUTPUT_PATH, result)
    print(f"[build_africa_by_country] Saved {len(result)} countries with African players → {OUTPUT_PATH}")

if __name__ == "__main__":
    build_players_by_country()
