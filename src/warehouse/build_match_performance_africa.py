import os
import argparse
import pandas as pd
from src.storage import azure_blob
from src.warehouse.utils_mapping import map_events_to_afp, load_players_flat

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def build(league: str, season: str):
    print(f"[build_match_performance_africa] ▶️ Startar för liga {league}, säsong {season}")

    # Ladda players_flat
    players_path = f"warehouse/base/players_flat/{season}/players_flat.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {players_path}")
    players_bytes = azure_blob.get_bytes(CONTAINER, players_path)
    df_players = pd.read_parquet(pd.io.common.BytesIO(players_bytes))

    african_players = set(df_players[df_players["nationality_group"] == "Africa"]["id"].astype(str))
    print(f"[build_match_performance_africa] 👥 Antal afrikanska spelare: {len(african_players)}")

    # Ladda events
    events_path = f"warehouse/live/events_flat/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {events_path}")
    events_bytes = azure_blob.get_bytes(CONTAINER, events_path)
    df_events = pd.read_parquet(pd.io.common.BytesIO(events_bytes))

    print(f"[DEBUG] Event-kolumner: {list(df_events.columns)}")
    print(f"[DEBUG] Exempelrader från events:")
    print(df_events.head(10).to_dict())

    # Mappa till AFP-id
    df_events = map_events_to_afp(df_events, season=season)

    # Filtrera på afrikanska spelare
    df_africa = df_events[df_events["afp_id"].astype(str).isin(african_players)].copy()

    # Scoring
    score_map = {
        "goal": 3,
        "assist": 2,
        "yellow_card": -1,
        "red_card": -3,
    }
    df_africa.loc[:, "score"] = df_africa["event_type"].map(score_map).fillna(0)

    print(f"[build_match_performance_africa] 🎯 Events efter filter: {len(df_africa)}")

    # Summera per spelare
    summary = (
        df_africa.groupby(["afp_id", "player_name"])
        .agg({"score": "sum"})
        .reset_index()
        .sort_values("score", ascending=False)
    )

    # Skriv resultat
    out_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 💾 Skriver till {out_path}")
    buf = pd.io.common.BytesIO()
    summary.to_parquet(buf, index=False)
    azure_blob.put_bytes(CONTAINER, out_path, buf.getvalue())

    print(f"[build_match_performance_africa] ✅ Klar")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True)
    parser.add_argument("--season", required=True)
    args = parser.parse_args()

    build(args.league, args.season)
