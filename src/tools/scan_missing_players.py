import os
import re
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

MISSING_PATH = "players/africa/missing_history.json"
MASTER_PATH = "players/africa/players_africa_master.json"
OUTPUT_PATH = "players/africa/missing_scan_results.json"


def normalize(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower())


def get_lastname(name: str) -> str:
    """Plocka ut efternamn, ignorera initialer som 'V.'."""
    parts = re.split(r"\s+", name.strip())
    for token in reversed(parts):
        norm = normalize(token)
        if len(norm) >= 3:  # hoppa över initialer
            return norm
    return normalize(parts[-1]) if parts else ""


def scan_missing_players():
    missing = azure_blob.get_json(CONTAINER, MISSING_PATH)
    master = azure_blob.get_json(CONTAINER, MASTER_PATH)

    # Bygg alias från master (fix för dict/list values)
    name_to_alias = {}
    for pid, pdata in master.items():
        if isinstance(pdata, list) and pdata:
            pdata = pdata[0]
        if not isinstance(pdata, dict):
            continue
        pname = normalize(pdata.get("name", ""))
        aliases = [normalize(a) for a in pdata.get("aliases", [])]
        name_to_alias[pname] = aliases

    results = {}

    # Hitta alla manifest
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
            # 👇 Täck alla roller där spelare kan förekomma
            for role in ["player", "assist_player", "player_in", "player_out"]:
                p = event.get(role, {})
                if not isinstance(p, dict):
                    continue
                pid = str(p.get("id", ""))
                pname = p.get("name", "")
                if not pname:
                    continue

                norm_name = normalize(pname)
                lastname = get_lastname(pname)

                for mid, mdata in missing.items():
                    target_name = normalize(mdata["name"])
                    target_last = get_lastname(mdata["name"])
                    aliases = name_to_alias.get(target_name, [])

                    if (
                        norm_name == target_name
                        or norm_name in aliases
                        or lastname == target_last
                    ):
                        if mid not in results:
                            results[mid] = {"name": mdata["name"], "found_ids": {}}
                        results[mid]["found_ids"].setdefault(pid, [])
                        results[mid]["found_ids"][pid].append(
                            {"season": season, "league_id": league_id}
                        )

    # Ladda upp resultat till Azure
    azure_blob.upload_json(CONTAINER, OUTPUT_PATH, results)
    print(f"[scan_missing_players] Uploaded results → {OUTPUT_PATH}")
    print(f"[scan_missing_players] Players scanned: {len(missing)}")
    print(f"[scan_missing_players] Players matched: {len(results)}")

    for mid, r in results.items():
        print(f"[scan_missing_players] {r['name']} → {list(r['found_ids'].keys())}")


if __name__ == "__main__":
    if not CONTAINER or not CONTAINER.strip():
        raise RuntimeError("AZURE_STORAGE_CONTAINER is missing or empty")
    scan_missing_players()
