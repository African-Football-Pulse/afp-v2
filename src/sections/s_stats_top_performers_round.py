import os
import pandas as pd
from src.sections import utils
from src.producer import gpt
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.PERFORMERS.ROUND")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, _ = utils.get_persona_block("storyteller", pod)

    # 🔹 Läs in data från metrics (match performance)
    container = os.getenv("AZURE_STORAGE_CONTAINER", "warehouse")
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{utils.league_to_id(league)}.parquet"
    df = pd.read_parquet(azure_blob.get_bytes(container, blob_path))

    if df.empty:
        text = "No performance data available for this round."
    else:
        # 🔹 Begränsa till senaste runda om round_dates finns
        round_dates = kwargs.get("round_dates", [])
        if round_dates:
            df = df[df["date"].isin(round_dates)]

        # 🔹 Sortera på rating eller annan prestandamått
        df_sorted = df.sort_values(by="rating", ascending=False).head(5)

        # 🔹 Förbered sammanfattning
        top_players = [
            f"{row['player_name']} ({row['club']}) rating {row['rating']}"
            for _, row in df_sorted.iterrows()
        ]
        summary = "; ".join(top_players)

        # 🔹 Skicka till GPT för textgenerering
        prompt = f"""
Write a short, engaging sports commentary in {lang} about the top 5 African players
from the latest round in the {league} ({season}). Use the following stats:

{summary}
"""
        text = gpt.complete(prompt, role="storyteller")

    payload = {
        "slug": "stats_top_performers_round",
        "title": "Top Performers This Round",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": [],
    }

    manifest = {"script": text, "meta": {"persona": persona_id}}

    return {
        "section": section_code,
        "day": day,
        "league": league,
        "status": "success",
        "lang": lang,
        "pod": pod,
        "path": f"sections/{section_code}/{day}/{league}/{pod}",
        "manifest": manifest,
        "payload": payload,
    }
