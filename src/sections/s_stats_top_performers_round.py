import os
import pandas as pd

from src.storage import azure_blob
from src.producer import role_utils   # ✅ rätt plats
from src.utils_section import SectionWriter
from src.warehouse.utils_ids import normalize_ids
from src.warehouse.utils_mapping import map_ids

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def build(args, writer: SectionWriter):
    """
    Bygger sektionen 'Top performing African players in a round'
    baserat på warehouse/metrics/match_performance_africa.
    """

    league = args.league
    season = args.season if hasattr(args, "season") else "2025-2026"

    path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"

    if not azure_blob.exists(CONTAINER, path):
        writer.log(f"[s_stats_top_performers_round] ⚠️ Hittar inte {path}")
        writer.set_status("empty")
        return

    # Ladda parquet
    writer.log(f"[s_stats_top_performers_round] 📥 Laddar {path}")
    data_bytes = azure_blob.get_bytes(CONTAINER, path)
    df = pd.read_parquet(pd.io.common.BytesIO(data_bytes))

    if df.empty:
        writer.log("[s_stats_top_performers_round] ⚠️ Tom parquet – inga spelare denna omgång")
        writer.set_status("empty")
        return

    # Normalisera IDs (om det finns blandade typer)
    df["player_id"] = normalize_ids(df["player_id"])

    # Summera score per spelare
    df_grouped = (
        df.groupby(["player_id", "player_name"])
        .agg({"score": "sum"})
        .reset_index()
        .sort_values("score", ascending=False)
    )

    if df_grouped.empty:
        writer.log("[s_stats_top_performers_round] ⚠️ Inga poäng att visa")
        writer.set_status("empty")
        return

    # Ta topp 5 spelare
    top_players = df_grouped.head(5)

    # Bygg text för sektionen
    lines = []
    for _, row in top_players.iterrows():
        lines.append(f"{row['player_name']} – {row['score']} poäng")

    text = "Top performing African players this round:\n" + "\n".join(lines)

    # Skriv ut
    writer.write(
        content=text,
        role=role_utils.resolve("storyteller"),  # ✅ rätt modul
        meta={"season": season, "league": league, "count": len(top_players)},
    )

    writer.set_status("success")
    writer.log(f"[s_stats_top_performers_round] ✅ Klar – {len(top_players)} spelare med i topplistan")


def main():
    import argparse
    from src.utils_args import add_common_args

    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True)
    parser.add_argument("--season", required=True)
    add_common_args(parser)

    args = parser.parse_args()
    writer = SectionWriter(args)
    build(args, writer)


if __name__ == "__main__":
    main()
