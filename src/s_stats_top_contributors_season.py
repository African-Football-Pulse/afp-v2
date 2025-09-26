import os
import pandas as pd
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob


def build_section(args, **kwargs):
    section_code = "S.STATS.TOP.CONTRIBUTORS.SEASON"
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")

    # Hämta persona (storyteller används för stats)
    persona_id, persona_block = utils.get_persona_block("storyteller", args.pod)

    # Blob-sökväg för warehouse
    blob_path = "warehouse/metrics/goals_assists_africa.parquet"

    # Hämta parquet från Azure Blob
    local_tmp = "/tmp/goals_assists_africa.parquet"
    try:
        blob_client = azure_blob._client().get_blob_client(blob_path)
        with open(local_tmp, "wb") as f:
            f.write(blob_client.download_blob().readall())
        df = pd.read_parquet(local_tmp)
    except Exception as e:
        print(f"[{section_code}] ❌ No warehouse data: {e}")
        payload = {
            "slug": "stats_top_contributors_season",
            "title": "Top Contributors This Season",
            "text": "No data available for top contributors this season.",
            "length_s": 2,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        return utils.write_outputs(section_code, day, league, payload, status="no_data", lang=lang)

    # Använd senaste säsongen i datan
    if "season" in df.columns:
        latest_season = sorted(df["season"].unique())[-1]
        df = df[df["season"] == latest_season]
    else:
        latest_season = "current"

    # Säkerställ rätt kolumner
    if "goal_contributions" not in df.columns:
        df["goal_contributions"] = df.get("total_goals", 0) + df.get("total_assists", 0)

    # Ta topp 5 contributors
    top5 = (
        df.sort_values("goal_contributions", ascending=False)
        .head(5)
        .to_dict(orient="records")
    )

    # Bygg GPT-prompt
    summary_text = "\n".join(
        [
            f"{p['player_name']} ({p['country']}) – {p['goal_contributions']} goal contributions "
            f"({p['total_goals']} goals, {p['total_assists']} assists)"
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
        "length_s": int(round(len(gpt_output.split()) / 2.6)),  # ungefär sekunder
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top5,
    }

    return utils.write_outputs(section_code, day, league, payload, status="ok", lang=lang)
