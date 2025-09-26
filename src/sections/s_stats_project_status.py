import os
import io
import pandas as pd
from src.sections import utils
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.PROJECT.STATUS")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    blob_path = "warehouse/metrics/project_status_africa.parquet"
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    try:
        svc = azure_blob._client()
        container_client = svc.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        data = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(data))
    except Exception:
        text = "No project status data available."
        payload = {
            "slug": "stats_project_status",
            "title": "Project Status",
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
        text = "No project updates found."
        payload = {
            "slug": "stats_project_status",
            "title": "Project Status",
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

    # Summering av status
    summary = df.to_dict(orient="records")

    text = f"Project status data contains {len(summary)} items."
    payload = {
        "slug": "stats_project_status",
        "title": "Project Status",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "static",
        "items": summary,
    }
    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(
        section_code, day, league, lang, pod, manifest, "success", payload
    )
