# src/sections/s_stats_top_contributors_season.py

import os
import pandas as pd
from datetime import datetime, timezone
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def build_section(args=None):
    """
    Build a section presenting the top African players in the Premier League
    based on goals + assists (goal contributions) for the season.
    """

    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    pod = getattr(args, "pod", "default")
    lang = getattr(args, "lang", "en")
    section_code = getattr(args, "section", "S.STATS.TOP_CONTRIBUTORS_SEASON")
    top_n = int(getattr(args, "top_n", 5))

    blob_path = "warehouse/metrics/toplists_africa.parquet"

    # Load parquet from Azure
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
        text = f"Could not read toplist data ({e})"
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 0,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "storyteller"},
            "type": "stats",
            "items": [],
        }
        return utils.write_outputs(
            section_code=section_code,
            day=day,
            league=league,
            payload=payload,
            status="no_data",
            lang=lang,
        )

    # Check required columns
    required_cols = {"player_id", "player_name", "club", "goal_contributions"}
    if not required_cols.issubset(set(df.columns)):
        text = f"Missing expected columns in toplists_africa.parquet: {required_cols - set(df.columns)}"
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 0,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "storyteller"},
            "type": "stats",
            "items": [],
        }
        return utils.write_outputs(
            section_code=section_code,
            day=day,
            league=league,
            payload=payload,
            status="no_data",
            lang=lang,
        )

    # Take top N players
    df_top = df.sort_values("goal_contributions", ascending=False).head(top_n).copy()
    if df_top.empty:
        text = "No top contributors data available."
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 0,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "storyteller"},
            "type": "stats",
            "items": [],
        }
        return utils.write_outputs(
            section_code=section_code,
            day=day,
            league=league,
            payload=payload,
            status="no_data",
            lang=lang,
        )

    # Prepare data
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
        [f"- {p['name']} ({p['club']}): {p['contributions']} (Goals: {p['goals']}, Assists: {p['assists']})" for p in players_data]
    )

    # Persona (storyteller from speaking_roles.yaml via pods.yaml)
    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    # GPT prompt
    instructions = (
        f"You are a football storyteller. Write a spoken-style season update (~150 words, ~45–60s) in {lang} "
        f"about the top {top_n} African players in the {league.replace('_',' ').title()} "
        f"based on goal contributions (goals + assists) so far this season.\n\n"
        f"Here are the stats to use (do not invent numbers):\n\n{players_str}\n\n"
        f"Mention their clubs and exact totals. Make it conversational, engaging, and ready to record."
    )
    prompt_config = {"persona": persona_block, "instructions": instructions}
    enriched_text = run_gpt(prompt_config, {"players": players_data}, system_rules=None)

    payload = {
        "slug": "top_contributors_season",
        "title": "Top African Contributors this Season",
        "text": enriched_text,
        "length_s": len(players_data) * 20,
        "sources": {"metrics_input_path": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": players_data,
    }

    return utils.write_outputs(
        section_code=section_code,
        day=day,
        league=league,
        payload=payload,
        status="ok",
        lang=lang,
    )
