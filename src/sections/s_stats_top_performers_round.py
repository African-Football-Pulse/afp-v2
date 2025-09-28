import os
import pandas as pd
from src.sections import utils
from src.producer import gpt, role_utils
from src.storage import azure_blob

def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.PERFORMERS.ROUND")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # Resolve persona
    persona_id, _ = utils.get_persona_block("storyteller", pod)

    # Hämta league_id (tex 228 för Premier League)
    from src.warehouse.utils_ids import LEAGUE_IDS
    league_id = LEAGUE_IDS.get(league)
    if not league_id:
        raise ValueError("league_id is required for Top Performers Round section")

    # Hämta parquet från Azure
    container = os.getenv("AZURE_STORAGE_CONTAINER")
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league_id}.parquet"
    df = pd.read_parquet(azure_blob.get_bytes(container, blob_path))

    # Säkerställ att kolumner finns
    for col in ["player_name", "score"]:
        if col not in df.columns:
            raise KeyError(f"Missing required column: {col}")

    # Ta de fem bästa spelarna
    top_players = (
        df.sort_values("score", ascending=False)
          .head(5)[["player_name", "score"]]
          .to_dict(orient="records")
    )

    # Skapa prompt
    players_str = ", ".join([f"{p['player_name']} ({p['score']})" for p in top_players])
    prompt = (
        f"Highlight the top performers of the round in the {league.replace('_', ' ').title()} "
        f"for African players in the {season} season. "
        f"Focus on their match performance scores. Here are the top players: {players_str}"
    )

    # GPT genererar text
    text = gpt.render_gpt(prompt, role=persona_id)

    # Bygg payload och manifest
    payload = {
        "slug": "stats_top_performers_round",
        "title": "Top Performers of the Round",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt-4",
        "items": top_players,
    }
    manifest = {"script": text, "meta": {"persona": persona_id}}

    # Skriv ut till Azure + lokalt
    outdir = f"sections/{section_code}/{day}/{league}/{pod}"
    utils.write_outputs(
        section_code,
        outdir,
        md_text=text,
        json_data=payload,
        manifest=manifest,
        status="success",
    )

    return payload
