import os
import io
import pandas as pd
from src.storage import azure_blob
from src.sections import utils
from src.producer.gpt import run_gpt

def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.GOAL.IMPACT") if args else "S.STATS.GOAL.IMPACT"
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league")) if args else os.getenv("LEAGUE", "premier_league")
    day = getattr(args, "date", os.getenv("DATE", "unknown")) if args else os.getenv("DATE", "unknown")
    lang = getattr(args, "lang", "en") if args else os.getenv("LANG", "en")
    pod = getattr(args, "pod", "default_pod") if args else os.getenv("POD", "default_pod")

    persona_id, _ = utils.get_persona_block("expert", pod)

    container = "afp"
    blob_path = "warehouse/metrics/goals_assists_africa.parquet"

    if not azure_blob.exists(container, blob_path):
        text = "No goal impact data available."
        payload = {
            "slug": "stats_goal_impact",
            "title": "Goal Impact",
            "text": text,
            "length_s": int(round(len(text.split()) / 2.6)),
            "sources": {"warehouse": "metrics"},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)

    # ✅ Läs parquet
    blob_bytes = azure_blob.get_bytes(container, blob_path)
    df = pd.read_parquet(io.BytesIO(blob_bytes))

    if df.empty:
        text = "No goal impact data available."
    else:
        sort_col = "goal_contributions" if "goal_contributions" in df.columns else "goals"
        top = df.sort_values(sort_col, ascending=False).head(5)

        # Skapa en enkel summering som GPT får använda som input
        summary_text = "\n".join(
            [f"{row['player_name']} – {row[sort_col]} {sort_col}" for _, row in top.iterrows()]
        )

        instructions = (
            f"Write a spoken-style summary in {lang}, focusing on the top African players "
            f"by goal impact this season. Highlight their names and numbers, make it "
            f"engaging but concise.\n\nData:\n{summary_text}"
        )

        prompt_config = {"persona": persona_id, "instructions": instructions}
        gpt_output = run_gpt(prompt_config, {"goal_impact": top.to_dict(orient='records')})
        text = gpt_output.strip() if gpt_output else "No goal impact commentary available."

    payload = {
        "slug": "stats_goal_impact",
        "title": "Goal Impact",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": "metrics"},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": [],
    }
    manifest = {"script": text, "meta": {"persona": persona_id}}

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
