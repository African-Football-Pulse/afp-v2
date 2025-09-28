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

    # ✅ Hämta persona på samma sätt som de andra sektionerna
    persona_id, _ = utils.get_persona_block("storyteller", pod)

    container = os.getenv("AZURE_CONTAINER", "afp")
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"

    # 📥 Läs parquet från Azure
    df = pd.read_parquet(azure_blob.get_bytes(container, blob_path))

    # Bygg contributions (fallback om kolumner saknas)
    goals = df["goals"] if "goals" in df.columns else 0
    assists = df["assists"] if "assists" in df.columns else 0
    df["contributions"] = goals.fillna(0) + assists.fillna(0)

    # Topp 5 spelare
    top_players = (
        df.groupby("player_name")["contributions"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
    )

    # 📝 GPT-prompt
    players_text = ", ".join(
        [f"{row.player_name} ({row.contributions})" for _, row in top_players.iterrows()]
    )
    prompt = f"Give a lively commentary about the top African performers this round: {players_text}"

    text = gpt.run_gpt(prompt, role="storyteller")

    payload = {
        "slug": "stats_top_performers_round",
        "title": "Top Performers This Round",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top_players.to_dict(orient="records"),
    }

    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(
        section_code, league, season, day, pod, payload, manifest, lang
    )
