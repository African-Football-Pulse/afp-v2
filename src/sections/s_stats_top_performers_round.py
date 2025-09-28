import os
import pandas as pd
from src.sections import utils
from src.producer import gpt, role_utils
from src.storage import azure_blob

# Enkel mapping tills vi bygger något smartare
LEAGUE_IDS = {
    "premier_league": "228",
    "championship": "229",
    # lägg till fler ligor här
}

def get_league_id(key: str) -> str:
    return LEAGUE_IDS.get(key, key)

def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.PERFORMERS.ROUND")
    league_key = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    season = getattr(args, "season", os.getenv("SEASON", "2025-2026"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # mappa league_key → league_id
    league_id = get_league_id(league_key)

    container = os.getenv("AZURE_CONTAINER", "afp-data")
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league_id}.parquet"

    # läs parquet från Azure
    df = pd.read_parquet(azure_blob.get_bytes(container, blob_path))

    # enkel ranking: topp 5 spelare efter rating (om kolumn finns)
    if "rating" in df.columns:
        top_df = df.sort_values("rating", ascending=False).head(5)
    else:
        # fallback: sortera på mål om rating saknas
        top_df = df.sort_values("goals", ascending=False).head(5)

    performers = []
    for _, row in top_df.iterrows():
        performers.append(
            {
                "player_name": row.get("player_name", "Unknown"),
                "club": row.get("club", "Unknown"),
                "rating": row.get("rating", None),
                "goals": row.get("goals", 0),
                "assists": row.get("assists", 0),
            }
        )

    # generera text via GPT
    role = role_utils.resolve_role("storyteller")
    prompt = (
        f"Highlight the top performers of the round in the {league_key.replace('_',' ').title()} "
        f"for season {season}. Focus on African players if present. "
        f"Here are the stats:\n{top_df.to_string(index=False)}"
    )
    text = gpt.generate_text(prompt, role=role)

    payload = {
        "slug": "stats_top_performers_round",
        "title": "Top Performers This Round",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": role},
        "type": "stats",
        "model": "gpt",
        "items": performers,
    }
    manifest = {"script": text, "meta": {"persona": role}}

    return utils.write_outputs(
        section_code,
        f"sections/{section_code}/{day}/{league_key}/{pod}",
        text,
        payload,
        manifest,
        status="success",
    )
