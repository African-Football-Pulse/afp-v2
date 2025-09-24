import os
import pandas as pd
import io
from src.storage import azure_blob


# Helper för att läsa parquet från Azure
def read_parquet_from_blob(container: str, path: str) -> pd.DataFrame:
    blob_bytes = (
        azure_blob._client()
        .get_container_client(container)
        .get_blob_client(path)
        .download_blob()
        .readall()
    )
    return pd.read_parquet(io.BytesIO(blob_bytes), engine="pyarrow")


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

    # 📥 Läs in data
    df_goals = read_parquet_from_blob(container, goals_path)
    df_cards = read_parquet_from_blob(container, cards_path)
    df_players = read_parquet_from_blob(container, players_path)[
        ["player_id", "club", "country"]
    ]

    # Summera senaste säsongen
    latest_season = df_goals["season"].max()
    df_goals_latest = df_goals[df_goals["season"] == latest_season]
    df_cards_latest = df_cards[df_cards["season"] == latest_season]

    # Toplists
    top_goals = (
        df_goals_latest.sort_values("total_goals", ascending=False)
        .head(10)
        .assign(metric="goals")
    )
    top_assists = (
        df_goals_latest.sort_values("total_assists", ascending=False)
        .head(10)
        .assign(metric="assists")
    )
    top_cards = (
        df_cards_latest.sort_values("total_cards", ascending=False)
        .head(10)
        .assign(metric="cards")
    )

    toplists = pd.concat([top_goals, top_assists, top_cards], ignore_index=True)

    # Lägg till club & country
    toplists = toplists.merge(df_players, on="player_id", how="left")

    # 📦 Spara till Parquet
    parquet_bytes = toplists.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(
        container=container,
        blob_path=output_parquet,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    # 📦 Spara JSON
    azure_blob.upload_json(container, output_json, toplists.to_dict(orient="records"))

    # 📦 Spara Markdown
    md_lines = ["# Africa Toplists", f"Season: {latest_season}", ""]
    for metric in ["goals", "assists", "cards"]:
        md_lines.append(f"## Top {metric.capitalize()}")
        subset = toplists[toplists["metric"] == metric]
        for _, row in subset.iterrows():
            md_lines.append(
                f"- {row['player_name']} ({row['country']}, {row['club']}) → {row['metric']}: {row['total_goals'] if metric=='goals' else row['total_assists'] if metric=='assists' else row['total_cards']}"
            )
        md_lines.append("")
    azure_blob.put_text(container, output_md, "\n".join(md_lines))

    print(
        f"[build_toplists_africa] ✅ Uploaded {len(toplists)} rows → {output_parquet}, {output_json}, {output_md}"
    )


if __name__ == "__main__":
    main()
