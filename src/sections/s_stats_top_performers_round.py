import os
import io
import pandas as pd
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.PERFORMERS.ROUND")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    blob_path = "warehouse/metrics/match_performance_africa.parquet"
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    try:
        svc = azure_blob._client()
        container_client = svc.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        data = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(data))
    except Exception:
        text = "No performance data available for this round."
        payload = {
            "slug": "stats_top_performers_round",
            "title": "Top Performers This Round",
            "text": text,
            "length_s": 2,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(
            section_code, day, league, lang, pod, manifest, "empty", payload
        )

    if df.empty:
        text = "No data found for this round."
        payload = {
            "slug": "stats_top_performers_round",
            "title": "Top Performers This Round",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(
            section_code, day, league, lang, pod, manifest, "empty", payload
        )

    # Välj topp 5 spelare baserat på rating
    top5 = (
        df.sort_values("rating", ascending=False)
        .head(5)
        .to_dict(orient="records")
    )

    summary_text = "\n".join(
        [f"{p['player_name']} – rating {p['rating']}" for p in top5]
    )

    instructions = (
        f"Write a spoken-style summary in {lang}, highlighting the top 5 African players "
        f"with the highest match ratings in the latest round.\n\nData:\n{summary_text}"
    )

    prompt_config = {"persona": persona_block, "instructions": instructions}
    gpt_output = run_gpt(prompt_config, {"top_performers": top5})

    payload = {
        "slug": "stats_top_performers_round",
        "title": "Top Performers This Round",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top5,
    }
    manifest = {"script": gpt_output, "meta": {"persona": persona_id}}

    return utils.write_outputs(
        section_code, day, league, lang, pod, manifest, "success", payload
    )
