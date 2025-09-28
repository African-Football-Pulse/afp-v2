import os
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

    # ✅ Input parquet (kan ändras vid behov)
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

    # ✅ Läs in data
    df = pd.read_parquet(azure_blob.get_text(container, blob_path))
    if df.empty:
        text = "No goal impact data available."
    else:
        # Förenklad textgenerering — kan utökas
        top = df.sort_values("goal_contributions", ascending=False).head(5)
        players = [f"{row['player_name']} ({row['goal_contributions']})" for _, row in top.iterrows()]
        text = "Top African players by goal impact: " + ", ".join(players)

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
