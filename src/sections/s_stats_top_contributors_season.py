# src/sections/s_stats_top_contributors_season.py
import os
import pandas as pd
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob


def build_section(args, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.CONTRIBUTORS.SEASON")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # Hämta storyteller-roll
    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    # Blob-sökväg till warehouse
    blob_path = "warehouse/metrics/goals_assists_africa.parquet"

    # Ladda parquet från Azure Blob
    local_tmp = "/tmp/goals_assists_africa.parquet"
    try:
        blob_client = azure_blob._client().get_blob_client(blob_path)
        with open(local_tmp, "wb") as f:
            f.write(blob_client.download_blob().readall())
        df = pd.read_parquet(local_tmp)
    except Exception as e:
        print(f"[{section_code}] ❌ No warehouse data: {e}")
        text = "No data available for top contributors this season."
        payload = {
            "slug": "stats_top_contributors_season",
            "title": "Top Contributors This Season",
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
            section_code=section_code,
            day=day,
            league=league,
            lang=lang,
            pod=pod,
            manifest=manifest,
            status="empty",
            payload=payload,
        )

    # Senaste säsongen
    if "season" in df.columns:
        latest_season = sorted(df["season"].unique())[-1]
        df = df[df["season"] == latest_season]
    else:
        latest_season = "current"

    # Säkerställ kolumn för goal_contributions
    if "goal_contributions" not in df.columns:
        df["goal_contributions"] = df.get("total_goals", 0) + df.get("total_assists", 0)

    # Top 5 contributors
    top5 = (
        df.sort_values("goal_contributions", ascending=False)
        .head(5)
        .to_dict(orient="records")
    )

    # Skapa summeringstext
    summary_text = "\n".join(
        [
            f"{p['player_name']} ({p.get('country', 'N/A')}) – "
            f"{int(p['goal_contributions'])} contributions "
            f"({int(p.get('total_goals', 0))} goals, {int(p.get('total_assists', 0))} assists)"
            for p in top5
        ]
    )

    instructions = (
        f"Write a spoken-style summary in {lang}, highlighting the top 5 African players "
        f"with most goal contributions this season ({latest_season}). "
        f"Make it engaging but concise.\n\nData:\n{summary_text}"
    )

    prompt_config = {"persona": persona_block, "instructions": instructions}
    gpt_output = run_gpt(prompt_config, {"top_contributors": top5})

    payload = {
        "slug": "stats_top_contributors_season",
        "title": f"Top Contributors – {latest_season}",
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
        section_code=section_code,
        day=day,
        league=league,
        lang=lang,
        pod=pod,
        manifest=manifest,
        status="success",
        payload=payload,
    )
