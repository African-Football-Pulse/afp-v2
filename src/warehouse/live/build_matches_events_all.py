import os
import subprocess
import pandas as pd
import io
from src.storage import azure_blob

# Konstanter
CONTAINER = os.environ.get("AZURE_CONTAINER", "afp")
SEASONS_PARQUET = "warehouse/base/seasons_flat.parquet"
CURRENT_SEASON = "2025-2026"

def load_seasons_flat():
    """Läs seasons_flat.parquet från Azure Blob Storage."""
    if not azure_blob.exists(CONTAINER, SEASONS_PARQUET):
        raise FileNotFoundError(
            f"❌ Hittar inte {SEASONS_PARQUET} i container {CONTAINER}. "
            "Kör build_seasons_flat först."
        )

    svc = azure_blob._client()
    container_client = svc.get_container_client(CONTAINER)
    blob_client = container_client.get_blob_client(SEASONS_PARQUET)
    stream = io.BytesIO(blob_client.download_blob().readall())
    return pd.read_parquet(stream)

def main():
    df = load_seasons_flat()

    # Filtrera på innevarande säsong och aktiva ligor
    active = df[df["is_active"] == True]

    print(f"▶️ Hittade {len(active)} aktiva ligor för {CURRENT_SEASON}")

    for _, row in active.iterrows():
        league_id = row["league_id"]
        league_name = row.get("league_name", str(league_id))

        print(f"▶️ Kör build_matches_events_flat för {league_name} ({league_id})")

        env = {
            **os.environ,
            "SEASON": CURRENT_SEASON,
            "LEAGUE_ID": str(league_id),
        }

        subprocess.run(
            ["python", "-m", "src.warehouse.live.build_matches_events_flat"],
            env=env,
            check=True,
        )

if __name__ == "__main__":
    main()
