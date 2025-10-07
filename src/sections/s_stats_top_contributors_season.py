import os
import io
import pandas as pd
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.CONTRIBUTORS.SEASON")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, persona_block = utils.get_persona_block("storyteller", pod)

    # 🧩 Aktuell säsong
    season = utils.current_season()
    print(f"[stats_top_contributors] Using season={season}")

    # Blob-sökväg till warehouse
    blob_path = "warehouse/metrics/goals_assists_africa.parquet"
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    try:
        svc = azure_blob._client()
        container_client = svc.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        data = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        print(f"[stats_top_contributors] Error loading {blob_path}: {e}")
        text = f"No data available for top contributors in season {season}."
        payload = {
            "slug": "stats_top_contributors_season",
            "title": f"Top Contributors – {season}",
            "text": text,
            "length_s": 2,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}, "season": season}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    print(f"[stats_top_contributors] Total rows before filter: {len(df)}")

    # 🧩 Filtrera på aktuell säsong
    if "season" in df.columns:
        df = df[df["season"] == season]
        print(f"[stats_top_contributors] Rows after season filter: {len(df)}")
    else:
        print("[stats_top_contributors] No 'season' column found — using full dataset.")

    if df.empty:
        text = f"No contributors found for {season}."
        payload = {
            "slug": "stats_top_contributors_season",
            "title": f"Top Contributors – {season}",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}, "season": season}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    # Säkerställ kolumn för goal_contributions
    if "goal_contributions" not in df.columns:
        df["goal_contributions"] = df.get("total_goals", 0) + df.get("total_assists", 0)

    # Top 5 contributors
    top5 = df.sort_values("goal_contributions", ascending=False).head(5).to_dict(orient="records")

    if not top5:
        text = f"No contributors found for {season}."
        payload = {
            "slug": "stats_top_contributors_season",
            "title": f"Top Contributors – {season}",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}, "season": season}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

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
        f"with most goal contributions this season ({season}). "
        f"Make it engaging but concise.\n\nData:\n{summary_text}"
    )

    prompt_config = {"persona": persona_block, "instructions": instructions}
    gpt_output = run_gpt(prompt_config, {"top_contributors": top5})

    payload = {
        "slug": "stats_top_contributors_season",
        "title": f"Top Contributors – {season}",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top5,
    }
    manifest = {"script": gpt_output, "meta": {"persona": persona_id}, "season": season}

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
