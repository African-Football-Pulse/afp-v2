import os
import pandas as pd
from src.storage import azure_blob
from src.warehouse.utils_mapping import map_events_to_afp, load_players_flat
from src.warehouse.utils_ids import normalize_ids

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def build(league: str, season: str):
    print(f"[build_match_performance_africa] ▶️ Startar för liga {league}, säsong {season}")

    # Ladda players_flat
    players_path = f"warehouse/base/players_flat/{season}/players_flat.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {players_path}")
    players_bytes = azure_blob.get_bytes(CONTAINER, players_path)
    df_players = pd.read_parquet(pd.io.common.BytesIO(players_bytes))

    # Filtrera fram afrikanska spelare
    african_players = set(df_players[df_players["country"].notna()]["id"].unique())
    print(f"[build_match_performance_africa] 👥 Antal afrikanska spelare: {len(african_players)}")

    # Ladda events
    events_path = f"warehouse/live/events_flat/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {events_path}")
    events_bytes = azure_blob.get_bytes(CONTAINER, events_path)
    df_events = pd.read_parquet(pd.io.common.BytesIO(events_bytes))

    print(f"[DEBUG] Event-kolumner: {list(df_events.columns)}")
    print("[DEBUG] Exempelrader från events:")
    print(df_events.head(10).to_dict())

    # Normalisera ID-kolumner
    df_events = normalize_ids(df_events)

    # Mappa mot AFP-ID
    df_events = map_events_to_afp(df_events)

    # Filtrera på afrikanska spelare
    df_africa = df_events[df_events["afp_id"].isin(african_players)].copy()

    if df_africa.empty:
        print("[build_match_performance_africa] ⚠️ Inga events för afrikanska spelare hittades.")
    else:
        print(f"[build_match_performance_africa] 🎯 Events efter filter: {len(df_africa)}")

        # Scoring-regler
        score_map = {
            "goal": 3,
            "assist": 2,
            "yellow_card": -1,
            "red_card": -3,
        }
        df_africa["score"] = df_africa["event_type"].map(score_map).fillna(0)

    # Skriv output
    out_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 💾 Skriver till {out_path}")
    azure_blob.put_bytes(CONTAINER, out_path, df_africa.to_parquet(index=False))

    print("[build_match_performance_africa] ✅ Klar")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True)
    parser.add_argument("--season", required=True)
    args = parser.parse_args()

    build(args.league, args.season)
