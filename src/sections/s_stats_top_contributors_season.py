# src/sections/s_stats_top_contributors_season.py

import os
import pandas as pd
from datetime import datetime, timezone
from src.sections.utils import write_outputs
from src.gpt import run_gpt
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def build_section(args=None):
    """
    Bygger en sektion som presenterar topp 5 afrikanska spelare i Premier League
    baserat på mål + assist (goal contributions) för hela säsongen.

    Input: warehouse/metrics/toplists_africa.parquet
    Output: section.json, section.md, section_manifest.json
    """

    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    section_id = getattr(args, "section_code", "S.STATS.TOP_CONTRIBUTORS_SEASON")
    top_n = int(getattr(args, "top_n", 5))

    blob_path = "warehouse/metrics/toplists_africa.parquet"

    # Ladda parquet från Azure
    try:
        tmp_path = "/tmp/toplists_africa.parquet"
        with open(tmp_path, "wb") as f:
            f.write(
                azure_blob._client()
                .get_container_client(CONTAINER)
                .get_blob_client(blob_path)
                .download_blob()
                .readall()
            )
        df = pd.read_parquet(tmp_path)
    except Exception as e:
        text = f"Kunde inte läsa toplist-data ({e})"
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 0,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "type": "stats",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_data")

    # Self-check: måste finnas rätt kolumner
    required_cols = {"player_id", "player_name", "club", "goal_contributions"}
    if not required_cols.issubset(set(df.columns)):
        text = f"Saknar förväntade kolumner i toplists_africa.parquet: {required_cols - set(df.columns)}"
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 0,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "type": "stats",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_data")

    # Ta topp N på contributions
    df_top = (
        df.sort_values("goal_contributions", ascending=False)
        .head(top_n)
        .copy()
    )

    # Self-check: contributions ska vara >= goals+assists om dessa finns
    if {"total_goals", "total_assists"}.issubset(df_top.columns):
        for _, row in df_top.iterrows():
            if row["goal_contributions"] != row["total_goals"] + row["total_assists"]:
                print(
                    f"[WARN] Inkonsekvent data för {row['player_name']}: "
                    f"{row['goal_contributions']} vs "
                    f"{row['total_goals']}+{row['total_assists']}"
                )

    if df_top.empty:
        text = "Ingen data hittades för toppspelare baserat på contributions."
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 0,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "type": "stats",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_data")

    # Förbered data till GPT
    players_data = []
    for _, row in df_top.iterrows():
        players_data.append(
            {
                "name": row["player_name"],
                "club": row["club"],
                "goals": int(row.get("total_goals", 0)),
                "assists": int(row.get("total_assists", 0)),
                "contributions": int(row["goal_contributions"]),
            }
        )

    players_str = "\n".join(
        [
            f"- {p['name']} ({p['club']}): {p['contributions']} (Goals: {p['goals']}, Assists: {p['assists']})"
            for p in players_data
        ]
    )

    # GPT-prompt
    prompt_config = {
        "persona": "Ama K – African football storyteller",
        "instructions": f"""
You are Ama K (Ama Kwarteng), a passionate African football storyteller.

Give a lively spoken-style season update (~150 words, ~45–60s) 
for the top {top_n} African players in the Premier League 
based on goal contributions (goals + assists) so far this season.

Here are the stats to use (do not invent numbers):

{players_str}

Mention their clubs and exact totals (goals and assists). 
Make it conversational, engaging, and ready to record.
"""
    }

    generated_text = run_gpt(prompt_config, model="gpt-4o-mini")

    payload = {
        "slug": "top_contributors_season",
        "title": "Top African Contributors this Season",
        "text": generated_text,
        "length_s": 60,
        "sources": {"metrics_input_path": blob_path},
        "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
        "type": "stats",
        "items": players_data,
    }

    return write_outputs(section_id, day, league, payload, status="ok")
