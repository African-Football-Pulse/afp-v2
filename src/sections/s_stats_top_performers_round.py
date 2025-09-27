import os
import pandas as pd

from src.storage import azure_blob
from src.producer import role_utils
from src.sections import utils
from src.warehouse.utils_ids import normalize_ids


def build_section(section_code, args, library):
    league = args.league
    day = args.date
    lang = args.lang
    pod = args.pod
    season = "2025-2026"  # TODO: dynamiskt om vi vill

    # 🎭 Persona
    persona_id = role_utils.get_persona_block("storyteller")

    # 📥 Läs parquet-fil från warehouse
    path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    try:
        bytes_data = azure_blob.get_bytes(
            os.getenv("AZURE_STORAGE_CONTAINER", "afp"), path
        )
        df = pd.read_parquet(pd.io.common.BytesIO(bytes_data))
    except Exception as e:
        print(f"[{section_code}] ⚠️ Kunde inte läsa parquet {path}: {e}")
        payload = {"text": "No performance data available for this round."}
        manifest = {"error": str(e)}
        return utils.write_outputs(
            section_code, day, league, lang, pod, manifest, "empty", payload
        )

    if df.empty:
        print(f"[{section_code}] ⚠️ Tom DataFrame i {path}")
        payload = {"text": "No performance data available for this round."}
        manifest = {"info": "empty dataframe"}
        return utils.write_outputs(
            section_code, day, league, lang, pod, manifest, "empty", payload
        )

    # 🎯 Bearbeta data: välj topp 5 spelare på score
    df = df.sort_values("score", ascending=False).head(5)

    top_players = []
    for _, row in df.iterrows():
        top_players.append({
            "player": row.get("player_name", "Unknown"),
            "club": row.get("club", "Unknown"),
            "score": row.get("score", 0),
            "goals": row.get("goals", 0),
            "assists": row.get("assists", 0),
            "yellow_cards": row.get("yellow_cards", 0),
            "red_cards": row.get("red_cards", 0),
        })

    # 📝 Text till sektionen
    text_lines = ["Top performing African players this round:"]
    for p in top_players:
        text_lines.append(
            f"- {p['player']} ({p['club']}) – Score {p['score']} "
            f"(Goals: {p['goals']}, Assists: {p['assists']}, "
            f"YC: {p['yellow_cards']}, RC: {p['red_cards']})"
        )
    text = "\n".join(text_lines)

    payload = {"text": text, "players": top_players}
    manifest = {"meta": {"persona": persona_id}, "source": path}

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
