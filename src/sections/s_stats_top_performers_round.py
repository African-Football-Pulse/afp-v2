import os
import pandas as pd

from src.storage import azure_blob
from src.producer import role_utils, stats_utils, utils
from src.warehouse.utils_ids import normalize_ids
from src.warehouse.utils_mapping import map_ids


def build_section(args, library):
    section_code = "S.STATS.TOP.PERFORMERS.ROUND"

    # Metadata
    day = args.date
    league = args.league
    lang = args.lang
    pod = args.pod

    season = "2025-2026"
    league_id = "228"  # Premier League (kan göras dynamiskt senare)

    # Ladda parquet med match-performance
    perf_path = f"warehouse/metrics/match_performance_africa/{season}/{league_id}.parquet"
    try:
        perf_bytes = azure_blob.get_bytes(os.getenv("AZURE_STORAGE_CONTAINER", "afp"), perf_path)
        df_perf = pd.read_parquet(pd.io.common.BytesIO(perf_bytes))
    except Exception as e:
        return utils.write_outputs(
            section_code, day, league, lang, pod,
            {"error": f"Failed to load performance data: {e}"},
            "empty", {}
        )

    if df_perf.empty:
        return utils.write_outputs(
            section_code, day, league, lang, pod,
            {"error": "No performance data available"},
            "empty", {}
        )

    # Ta topp 5 spelare
    top_players = df_perf.sort_values("score", ascending=False).head(5)

    # Bygg en enkel text
    performers_text = ", ".join(
        [f"{row['player_name']} (score {row['score']})" for _, row in top_players.iterrows()]
    )

    # Hämta persona via role_utils (faktiskt existerande kod)
    persona_id = role_utils.get_role(section_code, default="ak")

    # GPT prompt
    prompt = f"Top performing African players in the latest round were: {performers_text}."

    script = stats_utils.call_gpt(
        prompt, lang=lang, persona=persona_id, section=section_code
    )

    manifest = {"script": script, "meta": {"persona": persona_id}}
    payload = {"players": top_players.to_dict(orient="records")}

    return utils.write_outputs(
        section_code, day, league, lang, pod,
        manifest, "success", payload
    )
