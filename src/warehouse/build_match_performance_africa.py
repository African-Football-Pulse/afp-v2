import os
import io
import pandas as pd
from src.storage import azure_blob
from src.warehouse import utils_mapping

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def build(league: str, season: str):
    print(f"[build_match_performance_africa] ▶️ Startar för liga {league}, säsong {season}")

    # --- Ladda players_flat ---
    players_path = f"warehouse/base/players_flat/{season}/players_flat.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {players_path}")
    players_bytes = azure_blob.get_bytes(CONTAINER, players_path)
    df_players = pd.read_parquet(io.BytesIO(players_bytes))
    african_players = set(df_players[df_players["country"].notna()]["id"].astype(str).unique())
    print(f"[build_match_performance_africa] 👥 Antal afrikanska spelare: {len(african_players)}")

    # --- Ladda events ---
    events_path = f"warehouse/live/events_flat/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {events_path}")
    events_bytes = azure_blob.get_bytes(CONTAINER, events_path)
    df_events = pd.read_parquet(io.BytesIO(events_bytes))

    print(f"[DEBUG] Event-kolumner: {list(df_events.columns)}")
    print("[DEBUG] Exempelrader från events:")
    print(df_events.head(10).to_dict())

    # --- Mappa events till AFP-ID ---
    df_events = utils_mapping.map_events_to_afp(df_events, season)

    # --- Filtrera på afrikanska spelare ---
    df_africa = df_events[df_events["afp_id"].isin(african_players)]
    print(f"[build_match_performance_africa] 🎯 Events efter filter: {len(df_africa)}")

    if df_africa.empty:
        print("[build_match_performance_africa] ⚠️ Inga events för afrikanska spelare hittades.")
        return

    # --- Poängsättning ---
    score_map = {
        "goal": 3,
        "assist": 2,
        "yellow_card": -1,
        "red_card": -3,
    }
    df_africa["score"] = df_africa["event_type"].map(score_map).fillna(0)

    # --- Summera per spelare ---
    df_scores = df_africa.groupby(["afp_id", "player_name"]).agg(
        total_score=("score", "sum"),
        matches=("match_id", "nunique")
    ).reset_index()

    # --- Sortera ---
    df_scores = df_scores.sort_values("total_score", ascending=False)

    # --- Skriv resultat ---
    out_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 💾 Skriver till {out_path}")
    buf = io.BytesIO()
    df_scores.to_parquet(buf, index=False)
    azure_blob.put_bytes(CONTAINER, out_path, buf.getvalue())

    print("[build_match_performance_africa] ✅ Klar")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True, help="League ID (e.g. 228)")
    parser.add_argument("--season", required=True, help="Season (e.g. 2025-2026)")
    args = parser.parse_args()

    build(args.league, args.season)
