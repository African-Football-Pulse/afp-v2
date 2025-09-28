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

    persona_id, _ = role_utils.resolve_role("storyteller", pod)

    container = os.getenv("AZURE_CONTAINER", "afp")
    # 🔑 Bygg sökvägen på samma sätt som goals_assists_africa gör
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"

    # 📥 Läs parquet från Azure
    df = pd.read_parquet(azure_blob.get_bytes(container, blob_path))

    # Lite sanity check: ta toppspelare baserat på mål+assist
    if "goals" in df.columns and "assists" in df.columns:
        df["contributions"] = df["goals"].fillna(0) + df["assists"].fillna(0)
    else:
        df["contributions"] = 0

    top_players = (
        df.groupby("player_name")["contributions"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
    )

    # 📝 Prompt för GPT
    players_text = ", ".join(
        [f"{row.player_name} ({row.contributions})" for _, row in top_players.iterrows()]
    )
    prompt = f"Write a short football commentary about the top performers this round: {players_text}"

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
