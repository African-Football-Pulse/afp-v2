import os
import pandas as pd

from src.storage import azure_blob
from src.producer import role_utils, stats_utils
from src.sections import utils
from src.warehouse.utils_ids import normalize_ids

def build_section(args):
    section_code = "S.STATS.TOP.PERFORMERS.ROUND"
    day = args.date
    league = args.league
    lang = getattr(args, "lang", "en")
    pod = args.pod

    season = "2025-2026"  # kan göras dynamiskt senare
    perf_path = f"warehouse/metrics/match_performance_africa/{season}/228.parquet"

    try:
        # Läs parquet från Blob
        perf_bytes = azure_blob.get_bytes(os.getenv("AZURE_STORAGE_CONTAINER", "afp"), perf_path)
        df_perf = pd.read_parquet(pd.io.common.BytesIO(perf_bytes))
    except Exception as e:
        return utils.write_outputs(
            section_code, day, league, lang, pod,
            {"error": str(e)}, "empty", {}
        )

    if df_perf.empty:
        return utils.write_outputs(
            section_code, day, league, lang, pod,
            {"meta": "No data"}, "empty", {}
        )

    # Ta ut toppspelare i senaste omgången (match_id max)
    latest_match = df_perf["match_id"].max()
    df_latest = df_perf[df_perf["match_id"] == latest_match]

    # Summera poäng
    top_players = (
        df_latest.groupby(["player_id", "player_name"])
        .agg({"score": "sum"})
        .reset_index()
        .sort_values("score", ascending=False)
        .head(3)
    )

    if top_players.empty:
        return utils.write_outputs(
            section_code, day, league, lang, pod,
            {"meta": "No performers"}, "empty", {}
        )

    # Bygg text till GPT
    performers_text = "; ".join(
        f"{row['player_name']} ({int(row['score'])} pts)"
        for _, row in top_players.iterrows()
    )

    persona_id = role_utils.resolve_role("storyteller")
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
