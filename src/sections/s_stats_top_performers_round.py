import os
import pandas as pd
import io
from src.sections import utils
from src.producer import gpt
from src.producer import role_utils
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.PERFORMERS.ROUND")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    league_id = getattr(args, "league_id", kwargs.get("league_id", None))
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # 🔑 Fallback-map för league_id
    if not league_id:
        league_map = {
            "premier_league": "228",
            "championship": "229",
            # lägg till fler ligor här vid behov
        }
        league_id = league_map.get(league)

    if not league_id:
        raise ValueError(f"league_id could not be resolved for league={league}")

    persona_id, _ = utils.get_persona_block("storyteller", pod)

    # 🔹 Läs in metrics parquet för vald liga & säsong
    container = os.getenv("AZURE_STORAGE_CONTAINER", "warehouse")
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league_id}.parquet"
    df = pd.read_parquet(io.BytesIO(azure_blob.get_bytes(container, blob_path)))

    if df.empty:
        text = "No performance data available for this round."
    else:
        # Filtrera på round_dates om de skickas in
        round_dates = kwargs.get("round_dates", [])
        if round_dates and "date" in df.columns:
            df = df[df["date"].isin(round_dates)]

        # Sortera efter score och ta top 5
        df_sorted = df.sort_values(by="score", ascending=False).head(5)

        # Bygg sammanfattning för GPT
        top_players = [
            f"{row['player_name']} ({row['team']}) score {row['score']}"
            for _, row in df_sorted.iterrows()
        ]
        summary = "; ".join(top_players)

        # 🔹 Anropa GPT via render_gpt
        prompt_config = {
            "persona": "storyteller",
            "instructions": f"""
Write a short, engaging sports commentary in {lang} about the top 5 African players
from the latest round in the {league} ({season}). Use the following stats:

{summary}
""",
        }
        text = gpt.render_gpt(prompt_config, ctx=None)

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



    return utils.write_outputs(
        section_code,
        f"sections/{section_code}/{day}/{league_id}/{pod}",
        text,
        payload,
        manifest,
        status="success",
    )
