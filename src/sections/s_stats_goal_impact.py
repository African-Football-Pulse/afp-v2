import os
import io
import pandas as pd
from src.storage import azure_blob
from src.sections import utils

def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.GOAL.IMPACT") if args else "S.STATS.GOAL.IMPACT"
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league")) if args else os.getenv("LEAGUE", "premier_league")
    day = getattr(args, "date", os.getenv("DATE", "unknown")) if args else os.getenv("DATE", "unknown")
    lang = getattr(args, "lang", "en") if args else os.getenv("LANG", "en")
    pod = getattr(args, "pod", "default_pod") if args else os.getenv("POD", "default_pod")

    persona_id, _ = utils.get_persona_block("expert", pod)

    container = "afp"
    blob_path = "warehouse/metrics/goals_assists_africa.parquet"

    if not azure_blob.exists(container, blob_path):
        text = "No goal impact data available."
        payload = {
            "slug": "stats_goal_impact",
            "title": "Goal Impact",
            "text": text,
            "length_s": int(round(len(text.split()) / 2.6)),
            "sources": {"warehouse": "metrics"},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)

    # ✅ Läs parquet som binär
    blob_bytes = azure_blob.get_bytes(container, blob_path)
    df = pd.read_parquet(io.BytesIO(blob_bytes))

    if df.empty:
        text = "No goal impact data available."
    else:
        # Sortera på goal_contributions om den finns, annars goals
        sort_col = "goal_contributions" if "goal_contributions" in df.columns else "goals"
        top = df.sort_values(sort_col, ascending=False).head(5)
        players = [f"{row['player_name']} ({row[sort_col]})" for _, row in top.iterrows()]
        text = "Top African players by goal impact: " + ", ".join(players)

    payload = {
        "slug": "stats_goal_impact",
        "title": "Goal Impact",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": "metrics"},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "stats",
        "items": [],
    }
    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
