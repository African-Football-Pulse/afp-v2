import os
import io
import pandas as pd
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
SEASON = "2025-2026"  # default, kan skrivas över via CLI

def build(league: str, season: str):
    print(f"[build_match_performance_africa] ▶️ Startar för liga {league}, säsong {season}")

    # Ladda players_flat (alla afrikanska spelare)
    players_path = f"warehouse/base/players_flat/{season}/players_flat.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {players_path}")
    players_bytes = azure_blob.get_bytes(CONTAINER, players_path)
    df_players = pd.read_parquet(io.BytesIO(players_bytes))
    df_players["id"] = df_players["id"].astype(str)
    african_players = set(df_players["id"].unique())
    print(f"[build_match_performance_africa] 👥 Antal afrikanska spelare: {len(african_players)}")

    # Ladda events för aktuell liga
    events_path = f"warehouse/live/events_flat/{season}/{league}.parquet"
    print(f"[build_match_performance_africa] 📥 Laddar {events_path}")
    events_bytes = azure_blob.get_bytes(CONTAINER, events_path)
    df_events = pd.read_parquet(io.BytesIO(events_bytes))

    # Säkerställ strängformat för player_id
    if "player_id" in df_events.columns:
        df_events["player_id"] = df_events["player_id"].astype(str)
    else:
        print("[build_match_performance_africa] ⚠️ Ingen kolumn 'player_id' hittades i events")
        return

    # Filtrera på afrikanska spelare
    df_events = df_events[df_events["player_id"].isin(african_players)]
    print(f"[build_match_performance_africa] 🎯 Events efter filter: {len(df_events)}")

    if df_events.empty:
        print("[build_match_performance_africa] ⚠️ Inga events för afrikanska spelare hittades.")
        return

    # Poängsätt prestationer
    def score_event(row):
        points = 0
        if row.get("event_type") == "goal":
            points += 3
        if row.get("event_type") == "assist":
            points += 2
        if row.get("event_type") == "yellow_card":
            points -= 1
        if row.get("event_type") == "red_card":
            points -= 3
        if row.get("team_result") == "win":
            points += 1
        return points

    df_events["points"] = df_events.apply(score_event, axis=1)

    # Summera per spelare
    df_perf = (
        df_events.groupby("player_id")["points"]
        .sum()
        .reset_index()
        .sort_values("points", ascending=False)
    )

    # Koppla på namn från players_flat
    df_perf = df_perf.merge(
        df_players[["id", "name", "club", "country"]],
        left_on="player_id",
        right_on="id",
        how="left"
    ).drop(columns=["id"])

    # Output path
    output_path = f"warehouse/metrics/match_performance_africa.parquet"
    print(f"[build_match_performance_africa] 💾 Skriver {output_path}")

    buffer = io.BytesIO()
    df_perf.to_parquet(buffer, engine="pyarrow", index=False)
    azure_blob.put_bytes(CONTAINER, output_path, buffer.getvalue(), content_type="application/octet-stream")

    print("[build_match_performance_africa] ✅ Klar")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", required=True, help="Liga-ID (t.ex. 228)")
    parser.add_argument("--season", default="2025-2026", help="Säsong (t.ex. 2025-2026)")
    args = parser.parse_args()

    build(args.league, args.season)

if __name__ == "__main__":
    main()
