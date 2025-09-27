# src/sections/s_stats_top_performers_round.py

import argparse
import pandas as pd
from src.storage import azure_blob
import os
from src.utils import write_outputs

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def build(section: str, date: str, league: str, pod: str, path_scope: str, season: str, top_n: int = 3):
    print(f"[{section}] ▶️ Startar för liga {league}, säsong {season}")

    # Ladda performance-data
    perf_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    if not azure_blob.exists(CONTAINER, perf_path):
        print(f"[{section}] ⚠️ Performance-fil saknas: {perf_path}")
        return {"status": "empty"}

    bytes_data = azure_blob.get_bytes(CONTAINER, perf_path)
    df = pd.read_parquet(pd.io.common.BytesIO(bytes_data))

    if df.empty:
        print(f"[{section}] ⚠️ Ingen data i {perf_path}")
        return {"status": "empty"}

    print(f"[{section}] 📥 Laddade {len(df)} rader från {perf_path}")

    # Gruppera per spelare (summa score)
    grouped = df.groupby(["player_id", "player_name"], dropna=False)["score"].sum().reset_index()
    grouped = grouped.sort_values("score", ascending=False)

    if grouped.empty:
        print(f"[{section}] ⚠️ Inga spelare med score hittades")
        return {"status": "empty"}

    # Ta topp N
    top_players = grouped.head(top_n)
    print(f"[{section}] 🏆 Topp {top_n}:")
    for _, row in top_players.iterrows():
        print(f"   {row['player_name']} → {row['score']} poäng")

    # Bygg utdata (markdown + json)
    md_lines = ["### Top Performing African Players this Round", ""]
    for _, row in top_players.iterrows():
        md_lines.append(f"- **{row['player_name']}**: {row['score']} points")

    outputs = {
        "status": "success",
        "section": section,
        "date": date,
        "league": league,
        "season": season,
        "players": top_players.to_dict(orient="records"),
        "markdown": "\n".join(md_lines)
    }

    # Skriv utdata via utils
    write_outputs(section, date, league, pod, path_scope, outputs)
    print(f"[{section}] ✅ Klar – skrev topp {top_n} spelare")

    return outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--section", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--league", required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--path-scope", required=True)
    parser.add_argument("--season", required=True)
    parser.add_argument("--top-n", type=int, default=3)

    args = parser.parse_args()

    build(
        section=args.section,
        date=args.date,
        league=args.league,
        pod=args.pod,
        path_scope=args.path_scope,
        season=args.season,
        top_n=args.top_n
    )
