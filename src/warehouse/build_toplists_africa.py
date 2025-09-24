import os
import pandas as pd
from src.storage import azure_blob

def main():
    container = "afp"

    # Input paths
    goals_path = "warehouse/metrics/goals_assists_africa.parquet"
    cards_path = "warehouse/metrics/cards_africa.parquet"
    players_path = "warehouse/base/players_flat.parquet"

    # Output paths
    output_parquet = "warehouse/toplists/toplists_africa.parquet"
    output_json = "warehouse/toplists/toplists_africa.json"
    output_md = "warehouse/toplists/toplists_africa.md"

    # Ladda metrics
    df_goals = pd.read_parquet(azure_blob.load_parquet(container, goals_path))
    df_cards = pd.read_parquet(azure_blob.load_parquet(container, cards_path))

    # Ladda player master (för club + country)
    df_players = pd.read_parquet(azure_blob.load_parquet(container, players_path))[
        ["player_id", "club", "country"]
    ]
    df_players["player_id"] = df_players["player_id"].astype(str)

    # Enrich goals & cards
    df_goals["player_id"] = df_goals["player_id"].astype(str)
    df_cards["player_id"] = df_cards["player_id"].astype(str)

    df_goals = df_goals.merge(df_players, on="player_id", how="left")
    df_cards = df_cards.merge(df_players, on="player_id", how="left")

    # --- Top scorers ---
    top_goals = (
        df_goals.groupby(["player_id", "player_name", "country", "club"])
        ["total_goals"].sum()
        .reset_index()
        .sort_values("total_goals", ascending=False)
        .head(10)
    )
    top_goals["metric"] = "goals"

    # --- Top assists ---
    top_assists = (
        df_goals.groupby(["player_id", "player_name", "country", "club"])
        ["total_assists"].sum()
        .reset_index()
        .sort_values("total_assists", ascending=False)
        .head(10)
    )
    top_assists["metric"] = "assists"

    # --- Goal contributions ---
    top_gc = (
        df_goals.groupby(["player_id", "player_name", "country", "club"])
        ["goal_contributions"].sum()
        .reset_index()
        .sort_values("goal_contributions", ascending=False)
        .head(10)
    )
    top_gc["metric"] = "goal_contributions"

    # --- Cards (discipline) ---
    top_cards = (
        df_cards.groupby(["player_id", "player_name", "country", "club"])
        ["total_cards"].sum()
        .reset_index()
        .sort_values("total_cards", ascending=False)
        .head(10)
    )
    top_cards["metric"] = "cards"

    # Kombinera
    toplists = pd.concat([top_goals, top_assists, top_gc, top_cards], ignore_index=True)

    # 📦 Spara Parquet
    parquet_bytes = toplists.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(container, output_parquet, parquet_bytes, content_type="application/octet-stream")

    # 📦 Spara JSON
    toplists_json = toplists.to_dict(orient="records")
    azure_blob.upload_json(container, output_json, toplists_json)

    # 📦 Spara Markdown
    md_lines = ["# African Toplists\n"]
    for metric in ["goals", "assists", "goal_contributions", "cards"]:
        subset = toplists[toplists["metric"] == metric]
        md_lines.append(f"## Top {metric.capitalize()}")
        for _, row in subset.iterrows():
            val = row[metric]
            md_lines.append(f"- {row['player_name']} ({row['club']} / {row['country']}) – {val}")
        md_lines.append("")

    azure_blob.put_text(container, output_md, "\n".join(md_lines))

    print(f"[build_toplists_africa] ✅ Uploaded toplists → {output_parquet}, {output_json}, {output_md}")


if __name__ == "__main__":
    main()
