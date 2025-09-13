import os
import json

def merge_players_africa(whitelist_path="players_africa.json", ids_path="player_ID.txt", output_path="players_africa_master.json"):
    # 1. Läs whitelist
    with open(whitelist_path, "r", encoding="utf-8") as f:
        whitelist = json.load(f)

    # 2. Läs player_ID.txt (fri text, vi försöker tolka raderna)
    player_id_map = {}  # name -> {id, wiki, sns}
    with open(ids_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Exempelrad: Mohamed Salah | 61819 | https://en.wikipedia... | https://sportnewsafrica...
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 2:
                name = parts[0]
                entry = {}
                try:
                    entry["id"] = int(parts[1]) if parts[1].isdigit() else None
                except:
                    entry["id"] = None
                if len(parts) >= 3:
                    entry["wikipedia"] = parts[2] if parts[2].startswith("http") else None
                if len(parts) >= 4:
                    entry["sportnewsafrica"] = parts[3] if parts[3].startswith("http") else None
                player_id_map[name] = entry

    # 3. Slå ihop whitelist + ID-map
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

    # 4. Skriv ut masterlista
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"players": merged}, f, indent=2, ensure_ascii=False)

    print(f"[merge_players_africa] Wrote {len(merged)} players → {output_path}")


if __name__ == "__main__":
    merge_players_africa()
