# src/sections/s_stats_top_contributors_season.py

import os
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict

from src.sections.utils import write_outputs
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def build_section(args=None):
    """
    Bygg en sektion med topp-afrikanska spelare baserat på goal contributions (mål+assist)
    för hela säsongen i Premier League. Hämtar data från warehouse/metrics/toplists_africa.parquet.
    """
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    section_id = getattr(args, "section_code", "S.STATS.TOP_CONTRIBUTORS_SEASON")
    top_n = int(getattr(args, "top_n", 5))

    # Ladda toplist från Azure
    blob_path = "warehouse/metrics/toplists_africa.parquet"
    try:
        with open("/tmp/toplists_africa.parquet", "wb") as f:
            f.write(azure_blob._client()
                    .get_container_client(CONTAINER)
                    .get_blob_client(blob_path)
                    .download_blob().readall())
        df = pd.read_parquet("/tmp/toplists_africa.parquet")
    except Exception as e:
        text = f"Kunde inte läsa toplist-data ({e})"
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 2,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "type": "stats",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_data")

    if "top_contributions" not in df.columns:
        text = "Ingen kolumn 'top_contributions' hittades i toplists_africa.parquet."
        payload = {
            "slug": "top_contributors_season",
            "title": "Top African Contributors this Season",
            "text": text,
            "length_s": 2,
            "sources": {"metrics_input_path": blob_path},
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "type": "stats",
            "model": "gpt-4o-mini",
            "items": [],
        }
        return write_outputs(section_id, day, league, payload, status="no_data")

    # Sortera topp N spelare
    top_players = (
        df.sort_values("goal_contributions", ascending=False)
          .head(top_n)
          .to_dict(orient="records")
    )

    # Bygg enkel text
    lines = [f"Topp {len(top_players)} afrikanska spelare i Premier League (mål + assist, {day}):"]
    for p in top_players:
        lines.append(
            f"- {p['player_name']} ({p['club']}), {p['goal_contributions']} contributions "
            f"({p.get('total_goals', 0)} mål, {p.get('total_assists', 0)} assist)"
        )
    body = "\n".join(lines)

    payload: Dict[str, Any] = {
        "slug": "top_contributors_season",
        "title": "Top African Contributors this Season",
        "text": body,
        "length_s": len(top_players) * 30,  # antag ca 30 sekunder per spelare
        "sources": {"metrics_input_path": blob_path},
        "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
        "type": "stats",
        "model": "gpt-4o-mini",
        "items": top_players,
    }

    return write_outputs(section_id, day, league, payload, status="ok")
