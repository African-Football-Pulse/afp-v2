# src/sections/s_stats_top_performers_round.py

import os
from collections import defaultdict
from src.storage import azure_blob
from src.producer import stats_utils
from src.sections import utils
from src.producer.gpt import run_gpt

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")


def build_section(season: str, league_id: int, round_dates: list, output_prefix: str, args=None):
    """
    Build a Top Performers section for one full round (may span multiple dates).
    Called from s_stats_driver.py.
    """

    # Fetch events and save file in Azure
    blob_path = stats_utils.save_african_events(
        season=season, league_id=league_id, round_dates=round_dates, scope="round"
    )
    if not blob_path:
        return None

    events = azure_blob.get_json(CONTAINER, blob_path)
    if not events:
        return None

    # Aggregate stats
    performers = defaultdict(lambda: {"goals": 0, "assists": 0, "cards": 0, "name": ""})
    for ev in events:
        pid = ev["player"]["id"]
        pname = ev["player"]["name"]
        performers[pid]["name"] = pname
        if ev["event_type"] == "goal":
            performers[pid]["goals"] += 1
        elif ev["event_type"] == "assist":
            performers[pid]["assists"] += 1
        elif ev["event_type"] in ["yellow_card", "red_card"]:
            performers[pid]["cards"] += 1

    if not performers:
        text = "No top performers data available for this round."
        payload = {
            "slug": "top_performers_round",
            "title": "Top Performers of the Round",
            "text": text,
            "length_s": 0,
            "sources": {"events_blob": blob_path},
            "meta": {"persona": "storyteller"},
            "type": "stats",
            "items": [],
        }
        return utils.write_outputs(
            section_code="S.STATS.TOP_PERFORMERS_ROUND",
            day=round_dates[-1],
            league=str(league_id),
            payload=payload,
            lang=getattr(args, "lang", "en"),
            status="no_data",
        )

    # Sort top 3
    top_players = sorted(
        performers.values(),
        key=lambda x: (x["goals"] + x["assists"]),
        reverse=True
    )[:3]

    # Lang & pod
    lang = getattr(args, "lang", "en") if args else "en"
    pod = getattr(args, "pod", "default")

    # Persona (storyteller from speaking_roles.yaml)
    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    # Build GPT prompt
    players_str = "\n".join(
        [
            f"- {p['name']}: {p['goals']} goals, {p['assists']} assists, {p['cards']} cards"
            for p in top_players
        ]
    )

    instructions = (
        f"You are a football storyteller. Write a lively spoken-style recap (~120 words, ~40–50s) in {lang} "
        f"about the top African performers this round in league {league_id}, season {season}. "
        f"Here are the stats to use (do not invent numbers):\n\n{players_str}\n\n"
        f"Make it engaging, natural, and record-ready. Mention names and stats clearly."
    )

    prompt_config = {"persona": persona_block, "instructions": instructions}
    enriched_text = run_gpt(prompt_config, {"players": top_players}, system_rules=None)

    # Payload
    payload = {
        "slug": "top_performers_round",
        "title": "Top Performers of the Round",
        "text": enriched_text,
        "length_s": len(top_players) * 20,
        "sources": {"events_blob": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top_players,
    }

    return utils.write_outputs(
        section_code="S.STATS.TOP_PERFORMERS_ROUND",
        day=round_dates[-1],
        league=str(league_id),
        payload=payload,
        lang=lang,
        status="ok",
    )
