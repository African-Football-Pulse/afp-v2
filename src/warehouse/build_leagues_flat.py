import os
import json
import pandas as pd
import yaml
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def load_yaml_local(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    container = "afp"

    # Inputs
    local_yaml_path = "config/leagues.yaml"     # från repo
    blob_json_path = "meta/leagues.json"        # från Azure

    leagues_yaml = load_yaml_local(local_yaml_path).get("leagues", [])
    leagues_json = load_json_from_blob(container, blob_json_path).get("results", [])

    rows = []

    # Från leagues.yaml (primär config)
    for lg in leagues_yaml:
        rows.append({
            "league_id": str(lg.get("id")),
            "league_key": lg.get("key"),
            "league_name": lg.get("name"),
            "country": lg.get("country"),
            "season": lg.get("season"),
            "is_cup": lg.get("is_cup"),
            "source": "yaml"
        })

    # Från leagues.json (kompletterande info)
    for lg in leagues_json:
        rows.append({
            "league_id": str(lg.get("id")),
            "league_key": None,
            "league_name": lg.get("name"),
            "country": (lg.get("country") or {}).get("name"),
            "season": None,
            "is_cup": lg.get("is_cup"),
            "source": "json"
        })

    df = pd.DataFrame(rows)

    # 📦 Spara till Parquet
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    output_path = "warehouse/base/leagues_flat.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_leagues_flat] ✅ Uploaded {len(df)} rows → {output_path}")


if __name__ == "__main__":
    main()
