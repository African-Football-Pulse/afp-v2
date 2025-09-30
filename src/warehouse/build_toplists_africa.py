# src/warehouse/build_toplists_africa.py

import os
import io
import pandas as pd
from src.storage import azure_blob
from src.utils.normalize import normalize_ids  # om ni har lagt normalize_ids i utils


CONTAINER = "afp"


def read_parquet_from_blob(container: str, path: str) -> pd.DataFrame:
    """Ladda en Parquet-fil från Azure Blob till DataFrame."""
    blob_bytes = azure_blob.get_bytes(container, path)
    return pd.read_parquet(io.BytesIO(blob_bytes), engine="pyarrow")


def main():
    print("[build_toplists_africa] 🔄 Start")

    # Paths
    goals_path = "warehouse/metrics/goals_assists_africa.parquet"
    cards_path = "warehouse/metrics/cards_africa.parquet"
    players_path = "warehouse/base/players_flat.parquet"
    output_path = "warehouse/metrics/toplists_africa.parquet"

    # Load datasets
    df_goals = read_parquet_from_blob(CONTAINER, goals_path)
    df_cards = read_parquet_from_blob(CONTAINER, cards_path)
    df_players = (
        read_parquet_from_blob(CONTAINER, players_path)[
            ["player_id", "name", "current_club", "country"]
        ]
        .rename(columns={"current_club": "club", "name": "player_name"})
    )

    # Normalize IDs
    df_goals = normalize_ids(df_goals, ["player_id"])
    df_cards = normalize_ids(df_cards, ["player_id"])
    df_players = normalize_ids(df_players, ["player_id"])

    # Ensure season is string
    if "season" in df_goals.columns:
        df_goals["season"] = df_goals["season"].astype(str)
    if "season" in df_cards.columns:
        df_cards["season"] = df_cards["season"].astype(str)

    # ---- Merge metrics ----
    df = (
        df_goals.merge(df_cards, on=["player_id", "season"], how="outer")
        .merge(df_players, on="player_id", how="left")
    )

    # Fill NaNs with 0 for numeric cols
    for col in ["total_goals", "total_assists", "goal_contributions", "total_cards"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

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
        container=CONTAINER,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_toplists_africa] ✅ Uploaded {len(out)} rows → {output_path}")
    print("[build_toplists_africa] 🔎 Sample:")
    print(out.head(15))


if __name__ == "__main__":
    main()
