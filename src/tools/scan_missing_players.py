import os
import re
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

MISSING_PATH = "players/africa/missing_history.json"
MASTER_PATH = "players/africa/players_africa_master.json"
OUTPUT_PATH = "players/africa/missing_scan_results.json"


def normalize(name: str) -> str:
    """Normalisera namn till jämförbar sträng."""
    return re.sub(r"[^a-z]", "", name.lower())


def scan_missing_players():
    # Ladda missing + master
    missing = azure_blob.get_json(CONTAINER, MISSING_PATH)
    master = azure_blob.get_json(CONTAINER, MASTER_PATH)

    # Bygg en alias-mapp från master
    name_to_alias = {}
    for p in master:
        pname = normalize(p["name"])
        aliases = [normalize(a) for a in p.get("aliases", [])]
        name_to_alias[pname] = aliases

    results = {}

    # Loopa igenom alla manifest som redan ligger i stats/
    blob_list = azure_blob.list_prefix(CONTAINER, "stats/")
    manifests = [b for b in blob_list if b.endswith("manifest.json")]

    for mpath in manifests:
        parts = mpath.split("/")
        if len(parts) < 3:
            continue
        season = parts[1]
        league_id = parts[2]

        data = azure_blob.get_json(CONTAINER, mpath)
        if not isinstance(data, list):
            continue

        for event in data:
            player = event.get("player", {})
            pid = str(player.get("id", ""))
            pname = player.get("name", "")

            norm_name = normalize(pname)

            for mid, mdata in missing.items():
                target_name = normalize(mdata["name"])
                aliases = name_to_alias.get(target_name, [])

                if norm_name == target_name or norm_name in aliases:
                    if mid not in results:
                        results[mid] = {"name": mdata["name"], "found_ids": {}}
                    results[mid]["found_ids"].setdefault(pid, [])
                    results[mid]["found_ids"][pid].append(
                        {"season": season, "league_id": league_id}
                    )

    # Ladda upp resultat
    azure_blob.upload_json(CONTAINER, OUTPUT_PATH, results)
    print(f"[scan_missing_players] Uploaded results → {OUTPUT_PATH}")
    print(f"[scan_missing_players] Players scanned: {len(missing)}")
    print(f"[scan_missing_players] Players matched: {len(results)}")

    # Extra: skriv lite loggsammanfattning
    for mid, r in results.items():
        print(f"[scan_missing_players] {r['name']} → {list(r['found_ids'].keys())}")


if __name__ == "__main__":
    if not CONTAINER or not CONTAINER.strip():
        raise RuntimeError("AZURE_STORAGE_CONTAINER is missing or empty")
    scan_missing_players()
