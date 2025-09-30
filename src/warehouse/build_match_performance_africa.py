import os
import io
import pandas as pd
from src.storage import azure_blob
from src.warehouse.utils_ids import normalize_ids


def read_parquet_from_blob(container: str, path: str) -> pd.DataFrame:
    """Ladda en Parquet-fil från Azure Blob till DataFrame."""
    blob_bytes = azure_blob.get_bytes(container, path)
    return pd.read_parquet(io.BytesIO(blob_bytes), engine="pyarrow")


def main():
    container = "afp"

    # Paths
    goals_path = "warehouse/metrics/goals_assists_africa.parquet"
    cards_path = "warehouse/metrics/cards_africa.parquet"
    players_path = "warehouse/base/players_flat.parquet"
    output_path = "warehouse/metrics/toplists_africa.parquet"

    print("[build_toplists_africa] 🔄 Start")

    # Load datasets
    df_goals = read_parquet_from_blob(container, goals_path)
    df_cards = read_parquet_from_blob(container, cards_path)

    df_players = (
        read_parquet_from_blob(container, players_path)[
            ["player_id", "name", "current_club", "country"]
        ]
        .rename(columns={"current_club": "club", "name": "player_name"})
    )

    # ---- Merge metrics ----
    df = (
        df_goals.merge(df_cards, on=["player_id", "season"], how="outer")
        .merge(df_players, on="player_id", how="left")
    )

    # Normalisera ID-kolumner
    df = normalize_ids(df, ["player_id"])

    # Fyll endast numeriska kolumner med 0
    num_cols = ["total_goals", "total_assists", "goal_contributions", "total_cards"]
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # ---- Build toplists ----
    toplists = {}

    # Goals
    toplists["top_scorers"] = (
        df.groupby(["player_id", "player_name", "country", "club"])["total_goals"]
        .sum()
        .reset_index()
        .sort_values("total_goals", ascending=False)
        .head(20)
    )

    # Assists
    toplists["top_assists"] = (
        df.groupby(["player_id", "player_name", "country", "club"])["total_assists"]
        .sum()
        .reset_index()
        .sort_values("total_assists", ascending=False)
        .head(20)
    )

    # Goal contributions
    toplists["top_contributions"] = (
        df.groupby(["player_id", "player_name", "country", "club"])["goal_contributions"]
        .sum()
        .reset_index()
        .sort_values("goal_contributions", ascending=False)
        .head(20)
    )

    # Cards
    toplists["bad_boys"] = (
        df.groupby(["player_id", "player_name", "country", "club"])["total_cards"]
        .sum()
        .reset_index()
        .sort_values("total_cards", ascending=False)
        .head(20)
    )

    # ---- Save all toplists ----
    out = pd.concat(
        {name: tbl.reset_index(drop=True) for name, tbl in toplists.items()},
        names=["toplist", "rank"]
    ).reset_index(level="toplist").reset_index(drop=True)

    parquet_bytes = out.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_toplists_africa] ✅ Uploaded {len(out)} rows → {output_path}")
    print("[build_toplists_africa] 🔎 Sample:")
    print(out.head(15))


if __name__ == "__main__":
    main()
