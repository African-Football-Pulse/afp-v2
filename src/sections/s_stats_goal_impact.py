import os
import io
import pandas as pd
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.GOAL.IMPACT")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, persona_block = utils.get_persona_block("expert", pod)

    blob_path = "warehouse/metrics/toplists_africa.parquet"
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    try:
        svc = azure_blob._client()
        container_client = svc.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        data = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(data))
    except Exception:
        text = "No goal impact data available."
        payload = {
            "slug": "stats_goal_impact",
            "title": "Goal Impact Leaders",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    if df.empty:
        text = "No goal impact data found."
        payload = {
            "slug": "stats_goal_impact",
            "title": "Goal Impact Leaders",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    # Filtrera på toplist = "top_contributions"
    df_gc = df[df["toplist"] == "top_contributions"]

    if df_gc.empty:
        text = "No goal contributions data available."
        payload = {
            "slug": "stats_goal_impact",
            "title": "Goal Impact Leaders",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    # Ta topp 5 baserat på goal_contributions
    top5 = (
        df_gc.sort_values("goal_contributions", ascending=False)
        .head(5)
        .to_dict(orient="records")
    )

    summary_text = "\n".join(
        [f"{p['player_name']} ({p['club']}) – {p['goal_contributions']} goal contributions"
         for p in top5]
    )

    instructions = (
        f"Write a spoken-style summary in {lang}, highlighting the top 5 African players "
        f"with the most combined goals and assists this season.\n\nData:\n{summary_text}"
    )

    prompt_config = {"persona": persona_block, "instructions": instructions}
    gpt_output = run_gpt(prompt_config, {"goal_impact_leaders": top5})

    payload = {
        "slug": "stats_goal_impact",
        "title": "Goal Impact Leaders",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top5,
    }
    manifest = {"script": gpt_output, "meta": {"persona": persona_id}}

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
