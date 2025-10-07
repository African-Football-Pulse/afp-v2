import os
import io
import pandas as pd
from datetime import datetime
from src.sections import utils
from src.producer.gpt import run_gpt
from src.storage import azure_blob


def current_season(today=None):
    """
    Returnerar säsong i formatet 'YYYY-YYYY' baserat på dagens datum.
    Premier League-säsonger börjar i augusti.
    """
    if today is None:
        today = datetime.utcnow()
    year = today.year
    if today.month < 8:  # innan augusti → fortfarande förra säsongen
        return f"{year-1}-{year}"
    else:
        return f"{year}-{year+1}"


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.DISCIPLINE")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    persona_id, persona_block = utils.get_persona_block("expert", pod)

    blob_path = "warehouse/metrics/cards_africa.parquet"
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    # Beräkna aktuell säsong
    season = current_season()
    print(f"[stats_discipline] Using season={season}")

    try:
        svc = azure_blob._client()
        container_client = svc.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        data = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        text = f"No discipline data available (failed to load {blob_path})."
        payload = {
            "slug": "stats_discipline",
            "title": "Discipline Leaders",
            "text": text,
            "length_s": 0,
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}}
        print(f"[stats_discipline] Error loading blob: {e}")
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    if df.empty:
        text = "No discipline data found."
        payload = {
            "slug": "stats_discipline",
            "title": "Discipline Leaders",
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

    # 🧩 Filtrera på aktuell säsong (om kolumnen finns)
    if "season" in df.columns:
        df = df[df["season"] == season]
        print(f"[stats_discipline] Filtered rows for season={season} → {len(df)}")

    if df.empty:
        text = f"No discipline data found for season {season}."
        payload = {
            "slug": "stats_discipline",
            "title": "Discipline Leaders",
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

    # Top 5 spelare med flest kort
    top5 = (
        df.sort_values("total_cards", ascending=False)
        .head(5)
        .to_dict(orient="records")
    )

    summary_text = "\n".join(
        [f"{p['player_name']} ({p['country']}) – {p['total_yellow']} yellow, {p['total_red']} red"
         for p in top5]
    )

    instructions = (
        f"Write a spoken-style summary in {lang}, highlighting the top 5 African players "
        f"with the most yellow and red cards this season ({season}).\n\nData:\n{summary_text}"
    )

    prompt_config = {"persona": persona_block, "instructions": instructions}
    gpt_output = run_gpt(prompt_config, {"discipline_leaders": top5})

    payload = {
        "slug": "stats_discipline",
        "title": "Discipline Leaders",
        "text": gpt_output,
        "length_s": int(round(len(gpt_output.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": top5,
    }
    manifest = {
        "script": gpt_output,
        "meta": {"persona": persona_id},
        "season": season
    }

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
