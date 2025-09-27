import os
import pandas as pd

from src.storage import azure_blob
from src.producer import role_utils
from src.sections import utils
from src.warehouse.utils_ids import normalize_ids


def build_section(section_code, args, library):
    """
    Top performing African players in the latest round.
    Data hämtas från warehouse/metrics/match_performance_africa.
    """

    # Parametrar
    day = args.date
    league = args.league
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "_")
    season = os.getenv("SEASON", "2025-2026")

    # Bygg sökväg till parquet
    perf_path = f"warehouse/metrics/match_performance_africa/{season}/{league}.parquet"
    try:
        bytes_data = azure_blob.get_bytes(os.getenv("AZURE_STORAGE_CONTAINER", "afp"), perf_path)
    except Exception as e:
        return utils.write_outputs(
            section_code,
            day,
            league,
            lang,
            pod,
            {"error": str(e)},
            "empty",
            {},
        )

    df = pd.read_parquet(pd.io.common.BytesIO(bytes_data))

    if df.empty:
        return utils.write_outputs(
            section_code,
            day,
            league,
            lang,
            pod,
            {"note": "No performance data available"},
            "empty",
            {},
        )

    # Sortera efter score
    top_players = df.sort_values("score", ascending=False).head(5)

    # Persona
    persona_id = role_utils.resolve_role("storyteller")

    # Bygg text
    lines = ["Top performing African players this round:"]
    for _, row in top_players.iterrows():
        lines.append(f"- {row['player_name']} ({row['club']}): {row['score']} pts")

    text = "\n".join(lines)

    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(
        section_code, day, league, lang, pod, manifest, "success", top_players.to_dict()
    )
