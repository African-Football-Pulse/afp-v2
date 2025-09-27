import os
import pandas as pd

from src.storage import azure_blob
from src.producer import role_utils
from src.sections import utils
from src.warehouse.utils_ids import normalize_ids
from src.warehouse.utils_mapping import map_ids


def build(section_code, args, library):
    day = args.date
    league = args.league
    pod = args.pod
    lang = args.lang if hasattr(args, "lang") else "en"

    # Ladda match performance-data
    season = "2025-2026"
    perf_path = f"warehouse/metrics/match_performance_africa/{season}/228.parquet"
    perf_bytes = azure_blob.get_bytes("afp", perf_path)
    if not perf_bytes:
        return utils.write_outputs(section_code, day, league, lang, pod,
                                   {"script": f"No performance data for {season}"}, "empty", {})

    df_perf = pd.read_parquet(pd.io.common.BytesIO(perf_bytes))

    if df_perf.empty:
        return utils.write_outputs(section_code, day, league, lang, pod,
                                   {"script": "No performance data available for this round"}, "empty", {})

    # Välj top 3 spelare
    df_top = df_perf.sort_values("score", ascending=False).head(3)

    items = []
    for _, row in df_top.iterrows():
        items.append({
            "player": row.get("player_name"),
            "club": row.get("club"),
            "score": row.get("score"),
        })

    # Bygg text
    text_lines = ["Top performing African players this round:"]
    for item in items:
        text_lines.append(f"- {item['player']} ({item['club']}) – {item['score']} pts")

    text = "\n".join(text_lines)

    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    payload = {
        "slug": "top_performers_round",
        "title": "Top Performers of the Round",
        "text": text,
        "length_s": 45,
        "sources": [perf_path],
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": items,
    }

    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(
        section_code, day, league, lang, pod, manifest, "success", payload
    )
