import os
import io
import pandas as pd

from src.storage import azure_blob
from src.warehouse.utils_mapping import load_players_flat, map_events_to_afp

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

# Lista över afrikanska länder – kan byggas ut vid behov
AFRICAN_COUNTRIES = {
    "Egypt", "Ghana", "Senegal", "Algeria", "Morocco", "Nigeria",
    "Cameroon", "Ivory Coast", "Mali", "Tunisia", "South Africa",
    "DR Congo", "Zambia", "Burkina Faso", "Guinea", "Cape Verde",
    "Sierra Leone", "Tanzania", "Kenya", "Uganda", "Zimbabwe"
}

def build(league: str, season: str):
    print(f"[build_match_performance_africa] ▶️ Startar för liga {league}, säsong {season}")

    # Ladda spelardata
    print(f"[build_match_performance_africa] 📥 Laddar warehouse/base/players_flat/{season}/players_flat.parquet")
    df_players = load_players_flat(season)
    african_ids = set(df_players[df_players["country"].isin(AFRICAN_COUNTRIES)]["id"].astype(str))
    print(f"[build_match_performance_africa] 👥 Antal afrikanska spelare: {len(african_ids)}")

    # Ladda events
    events_path = f"warehouse/live/events_flat/{season}/{league}.parquet"
    events_bytes = azure_blob.get_bytes(CONTAINER, events_path)
    df_events = pd.read_parquet(io.BytesIO(events_bytes))

    print(f"[build_match_performance_africa] [DEBUG] Event-kolumner: {list(df_events.columns)}")
    if not df_events.empty:
        print(f"[DEBUG] Exempelrader från events:\n{df_events.head(10).to_dict()}")

    # Mappa player_id → AFP-id
    df_events = map_events_to_afp(df_events, df_players)

    # Filtrera på afrikanska spelare
    df_africa = df_events[df_events["afp_id"].astype(str).isin(african_ids)].copy()
    print(f"[build_match_performance_africa] 🎯 Events efter filter: {len(df_africa)}")

    if df_africa.empty:
        print("[build_match_performance_africa] ⚠️ Inga events för afrikanska spelare hittades.")
        return

    # Poängsättning
    score_map = {"goal": 3, "assist": 2, "yellow_card": -1, "red_card": -3}
    df_africa.loc[:, "score"] = df_africa["event_type"].map(score_map).fillna(0)

    # Aggregat per spelare
    df_summary = (
        df_africa.groupby(["afp_id", "player_name"])
        .agg(total_score=("score", "sum"), matches=("match_id", "nunique"))
        .reset_index()
        .sort_values("total_score", ascending=False)
    )

    # Skriv ut
    out_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 💾 Skriver till {out_path}")
    buffer = io.BytesIO()
    df_summary.to_parquet(buffer, index=False)
    azure_blob.put_bytes(CONTAINER, out_path, buffer.getvalue())

    print("[build_match_performance_africa] ✅ Klar")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True, help="League ID (t.ex. 228)")
    parser.add_argument("--season", required=True, help="Season (t.ex. 2025-2026)")
    args = parser.parse_args()

    build(args.league, args.season)
