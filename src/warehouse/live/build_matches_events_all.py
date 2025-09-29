import os
import subprocess
import pandas as pd

CURRENT_SEASON = "2025-2026"
SEASONS_PARQUET = "warehouse/base/seasons_flat.parquet"

def main():
    if not os.path.exists(SEASONS_PARQUET):
        raise FileNotFoundError(f"❌ Hittar inte {SEASONS_PARQUET}, kör build_seasons_flat först.")

    # Läs in seasons_flat
    df = pd.read_parquet(SEASONS_PARQUET)

    # Filtrera på innevarande säsong och aktiva ligor
    active = df[(df["season"] == CURRENT_SEASON) & (df["is_active"] == True)]

    print(f"▶️ Hittade {len(active)} aktiva ligor för {CURRENT_SEASON}")

    for _, row in active.iterrows():
        league_id = row["league_id"]
        league_name = row.get("league_name", str(league_id))

        print(f"▶️ Kör build_matches_events_flat för {league_name} ({league_id})")

        env = {**os.environ, "SEASON": CURRENT_SEASON, "LEAGUE_ID": str(league_id)}

        subprocess.run(
            ["python", "-m", "src.warehouse.live.build_matches_events_flat"],
            env=env,
            check=True
        )

if __name__ == "__main__":
    main()
