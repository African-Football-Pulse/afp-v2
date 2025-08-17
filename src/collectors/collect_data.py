import json, os
from src.storage.azure_blob import put_text, utc_now_iso
from src.storage.hash_util import hash_dict

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def main():
    hello = {"msg": "hello from collect_data", "ts": utc_now_iso()}
    put_text(CONTAINER, "raw/hello.json", json.dumps(hello, ensure_ascii=False, indent=2), "application/json")

    curated_demo = {"provider": "demo", "league": "premier_league", "players": [
        {"name": "Mohamed Salah", "club": "Liverpool", "xg": 0.6, "xa": 0.3, "minutes": 90},
        {"name": "Thomas Partey", "club": "Arsenal", "xg": 0.1, "xa": 0.2, "minutes": 78},
        {"name": "Andre Onana", "club": "Man United", "xg": 0.0, "xa": 0.0, "minutes": 90}
    ]}
    inputs_hash = hash_dict(curated_demo)
    manifest = {"inputs_hash": inputs_hash, "generated_at": utc_now_iso()}

    put_text(CONTAINER, "curated/demo/premier_league/players.json", json.dumps(curated_demo, ensure_ascii=False), "application/json")
    put_text(CONTAINER, "curated/demo/premier_league/input_manifest.json", json.dumps(manifest, ensure_ascii=False), "application/json")

if __name__ == "__main__":
    main()
