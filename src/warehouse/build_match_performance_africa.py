import os
import pandas as pd
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

SEASON = "2025-2026"
# Här kan vi i framtiden loopa över flera ligor, just nu hårdkodat EPL (228)
LEAGUES = ["228"]

# Poängsystem
POINTS = {
    "goal": 3,
    "assist": 2,
    "yellow_card": -1,
    "red_card": -3,
    "win_bonus": 1,
}

def build():
    all_rows = []

    # Ladda spelardata (för att filtrera på afrikanska spelare)
    players_path = f"warehouse/base/players_flat/{SEASON}/players_flat.parquet"
    players_bytes = azure_blob.get_bytes(CONTAINER, players_path)
    df_players = pd.read_parquet(pd.io.common.BytesIO(players_bytes))
    african_players = set(df_players[df_players["nationality_group"] == "Africa"]["player_id"].unique())

    for league_id in LEAGUES:
        # Läs events
        events_path = f"warehouse/base/events_flat/{SEASON}/{league_id}.parquet"
        events_bytes = azure_blob.get_bytes(CONTAINER, events_path)
        df_events = pd.read_parquet(pd.io.common.BytesIO(events_bytes))

        # Läs matcher
        matches_path = f"warehouse/base/matches_flat/{SEASON}/{league_id}.parquet"
        matches_bytes = azure_blob.get_bytes(CONTAINER, matches_path)
        df_matches = pd.read_parquet(pd.io.common.BytesIO(matches_bytes))

        # Grunddata: endast afrikanska spelare
        df_events = df_events[df_events["player_id"].isin(african_players)]

        # Matchresultat för win bonus
        df_matches["winner_team_id"] = df_matches.apply(
            lambda r: r["home_team_id"] if r["home_score"] > r["away_score"]
            else r["away_team_id"] if r["away_score"] > r["home_score"]
            else None,
            axis=1
        )
        winner_map = df_matches.set_index("match_id")["winner_team_id"].to_dict()

        # Poäng per event
        def event_points(row):
            if row["event_type"] == "goal":
                return POINTS["goal"]
            elif row["event_type"] == "assist":
                return POINTS["assist"]
            elif row["event_type"] == "yellow_card":
                return POINTS["yellow_card"]
            elif row["event_type"] == "red_card":
                return POINTS["red_card"]
            return 0

        df_events["points"] = df_events.apply(event_points, axis=1)

        # Summera per spelare & match
        grouped = df_events.groupby(["match_id", "player_id", "player_name"])["points"].sum().reset_index()

        # Lägg på win bonus
        grouped["team_id"] = grouped["match_id"].map(df_events.set_index("match_id")["team_id"])
        grouped["win_bonus"] = grouped.apply(
            lambda r: POINTS["win_bonus"] if r["team_id"] == winner_map.get(r["match_id"]) else 0,
            axis=1
        )
        grouped["total_points"] = grouped["points"] + grouped["win_bonus"]

        # Lägg på meta
        grouped["season"] = SEASON
        grouped["league_id"] = league_id

        all_rows.append(grouped)

    # Slå ihop alla ligor
    if all_rows:
        df_all = pd.concat(all_rows, ignore_index=True)
    else:
        df_all = pd.DataFrame(columns=["match_id", "player_id", "player_name", "total_points", "season", "league_id"])

    # Spara till metrics
    out_path = f"warehouse/metrics/match_performance_africa.parquet"
    import io
    buf = io.BytesIO()
    df_all.to_parquet(buf, index=False)
    azure_blob.put_bytes(CONTAINER, out_path, buf.getvalue(), content_type="application/octet-stream")

    print(f"[builder] Wrote {len(df_all)} rows → {out_path}")


if __name__ == "__main__":
    build()
