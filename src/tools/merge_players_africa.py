import os
import json
from src.storage import azure_blob

def merge_players_africa(whitelist_path="players_africa.json", ids_path="player_ID.txt", season="2024-2025"):
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # 1. Läs whitelist
    with open(whitelist_path, "r", encoding="utf-8") as f:
        whitelist = json.load(f)

    # 2. Läs player_ID.txt
    player_id_map = {}
    with open(ids_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 2:
                name = parts[0]
                entry = {}
                entry["id"] = int(parts[1]) if parts[1].isdigit() else None
                if len(parts) >= 3 and parts[2].startswith("http"):
                    entry["wikipedia"] = parts[2]
                if len(parts) >= 4 and parts[3].startswith("http"):
                    entry["sportnewsafrica"] = parts[3]
                player_id_map[name] = entry

    # 3. Slå ihop
    merged = []
    for p in whitelist.get("players", []):
        name = p.get("name")
        merged_entry = {
            "name": name,
            "aliases": p.get("aliases", []),
            "country": p.get("country"),
            "club": p.get("club"),
            "id": None,
            "sources": {}
        }
        if name in player_id_map:
            if "id" in player_id_map[name]:
                merged_entry["id"] = player_id_map[name]["id"]
            merged_entry["sources"]["wikipedia"] = player_id_map[name].get("wikipedia")
            merged_entry["sources"]["sportnewsafrica"] = player_id_map[name].get("sportnewsafrica")
        merged.append(merged_entry)

    # 4. Skriv till Azure
    blob_path = f"players/africa/players_africa_master.json"
    azure_blob.put_text(container, blob_path, json.dumps({"players": merged}, indent=2, ensure_ascii=False))

    print(f"[merge_players_africa] Uploaded {len(merged)} players → {blob_path}")


if __name__ == "__main__":
    merge_players_africa()
