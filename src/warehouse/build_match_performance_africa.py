import os
import io
import argparse
import pandas as pd
from src.storage import azure_blob
from src.warehouse.utils import normalize_ids

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def build(league: str, season: str, num_matches: int):
    print(f"[build_match_performance_africa] ▶️ Startar för liga {league}, säsong {season}, senaste {num_matches} matcher")

    # === Ladda spelardata ===
    players_path = f"warehouse/base/players_flat/{season}/players_flat.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {players_path}")
    df_players = pd.read_parquet(io.BytesIO(azure_blob.get_bytes(CONTAINER, players_path)))
    african_players = set(df_players[df_players["country"].notna()]["id"].unique())
    print(f"[build_match_performance_africa] 👥 Antal afrikanska spelare: {len(african_players)}")

    # === Ladda events ===
    events_path = f"warehouse/live/events_flat/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {events_path}")
    df_events = pd.read_parquet(io.BytesIO(azure_blob.get_bytes(CONTAINER, events_path)))
    print(f"[DEBUG] Event-kolumner: {list(df_events.columns)}")
    print(f"[DEBUG] Exempelrader från events:\n{df_events.head(10).to_dict()}")

    # === Ladda matcher för att hämta senaste N ===
    matches_path = f"warehouse/live/matches_flat/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {matches_path}")
    df_matches = pd.read_parquet(io.BytesIO(azure_blob.get_bytes(CONTAINER, matches_path)))

    if "date" in df_matches.columns:
        df_matches["match_date"] = pd.to_datetime(df_matches["date"], errors="coerce")
        recent_matches = df_matches.sort_values("match_date", ascending=False).head(num_matches)
    else:
        # fallback – ta senaste N match_id
        recent_matches = df_matches.sort_values("match_id", ascending=False).head(num_matches)

    recent_ids = set(recent_matches["match_id"].unique())
    print(f"[build_match_performance_africa] 📅 Antal matcher som används: {len(recent_ids)}")

    # === Filtrera events ===
    df_events["player_id"] = normalize_ids(df_events["player_id"])
    df_africa = df_events[df_events["player_id"].isin(african_players)]
    df_africa = df_africa[df_africa["match_id"].isin(recent_ids)]
    print(f"[build_match_performance_africa] 🎯 Events efter filter: {len(df_africa)}")

    if df_africa.empty:
        print("[build_match_performance_africa] ⚠️ Inga events för afrikanska spelare hittades.")
        return

    # === Poängsättning ===
    score_map = {
        "goal": 3,
        "assist": 2,
        "yellow_card": -1,
        "red_card": -3,
    }
    df_africa.loc[:, "score"] = df_africa["event_type"].map(score_map).fillna(0)

    # Summera poäng per spelare
    df_summary = df_africa.groupby(["player_id", "player_name"]).agg(
        total_score=("score", "sum"),
        goals=("event_type", lambda x: (x == "goal").sum()),
        assists=("event_type", lambda x: (x == "assist").sum()),
        yellows=("event_type", lambda x: (x == "yellow_card").sum()),
        reds=("event_type", lambda x: (x == "red_card").sum()),
    ).reset_index()

    print(f"[build_match_performance_africa] 🏆 Topplista:\n{df_summary.sort_values('total_score', ascending=False).head(10)}")

    # === Skriv output ===
    out_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 💾 Skriver till {out_path}")
    buf = io.BytesIO()
    df_summary.to_parquet(buf, index=False)
    azure_blob.put_bytes(CONTAINER, out_path, buf.getvalue())

    print("[build_match_performance_africa] ✅ Klar")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True, help="League ID")
    parser.add_argument("--season", required=True, help="Season (e.g. 2025-2026)")
    parser.add_argument("--num-matches", type=int, default=1, help="Antal matcher tillbaka att inkludera")
    args = parser.parse_args()

    build(args.league, args.season, args.num_matches)
