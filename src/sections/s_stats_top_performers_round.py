import os
import pandas as pd
import io
from src.sections import utils
from src.producer import gpt
from src.storage import azure_blob


def build_section(args=None, **kwargs):
    section_code = getattr(args, "section", "S.STATS.TOP.PERFORMERS.ROUND")
    league = getattr(args, "league", os.getenv("LEAGUE", "premier_league"))
    league_id = getattr(args, "league_id", kwargs.get("league_id", None))
    day = getattr(args, "date", os.getenv("DATE", "unknown"))
    lang = getattr(args, "lang", "en")
    pod = getattr(args, "pod", "default_pod")

    # 🧩 Hämta aktuell säsong
    season = utils.current_season()
    print(f"[stats_top_performers_round] Using season={season}")

    # 🔑 Fallback-map för league_id
    if not league_id:
        league_map = {
            "premier_league": "228",
            "championship": "229",
            # lägg till fler ligor här vid behov
        }
        league_id = league_map.get(league)

    if not league_id:
        raise ValueError(f"league_id could not be resolved for league={league}")

    persona_id, _ = utils.get_persona_block("storyteller", pod)

    # 🔹 Läs in metrics parquet för vald liga & säsong
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league_id}.parquet"

    print(f"[stats_top_performers_round] Loading blob: {blob_path}")

    try:
        blob_bytes = azure_blob.get_bytes(container, blob_path)
        df = pd.read_parquet(io.BytesIO(blob_bytes))
    except Exception as e:
        print(f"[stats_top_performers_round] Error loading {blob_path}: {e}")
        text = f"No performance data available for the current round ({season})."
        payload = {
            "slug": "stats_top_performers_round",
            "title": "Top Performers This Round",
            "text": text,
            "length_s": int(round(len(text.split()) / 2.6)),
            "sources": {"warehouse": blob_path},
            "meta": {"persona": persona_id},
            "type": "stats",
            "model": "static",
            "items": [],
        }
        manifest = {"script": text, "meta": {"persona": persona_id}, "season": season}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    if df.empty:
        print(f"[stats_top_performers_round] Empty dataframe for {blob_path}")
        text = f"No performance data available for this round ({season})."
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
        manifest = {"script": text, "meta": {"persona": persona_id}, "season": season}
        return utils.write_outputs(section_code, day, league, lang, pod, manifest, "empty", payload)

    # Filtrera på round_dates om de skickas in
    round_dates = kwargs.get("round_dates", [])
    if round_dates and "date" in df.columns:
        df = df[df["date"].isin(round_dates)]
        print(f"[stats_top_performers_round] Filtered on round_dates → {len(df)} rows left")

    # Sortera efter score och ta top 5
    sort_col = "score" if "score" in df.columns else "rating"
    df_sorted = df.sort_values(by=sort_col, ascending=False).head(5)

    # Bygg sammanfattning för GPT
    top_players = [
        f"{row['player_name']} ({row['team']}) score {row[sort_col]}"
        for _, row in df_sorted.iterrows()
    ]
    summary = "; ".join(top_players)

    # 🔹 Anropa GPT via render_gpt
    prompt_config = {
        "persona": "storyteller",
        "instructions": f"""
Write a short, engaging sports commentary in {lang} about the top 5 African players
from the latest round in the {league} ({season}). Use the following stats:

{summary}
""",
    }

    text = gpt.render_gpt(prompt_config, ctx=None)
    text = text.strip() if text else f"No commentary available for {season}."

    payload = {
        "slug": "stats_top_performers_round",
        "title": "Top Performers This Round",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {"warehouse": blob_path},
        "meta": {"persona": persona_id},
        "type": "stats",
        "model": "gpt",
        "items": df_sorted.to_dict(orient="records"),
    }

    manifest = {"script": text, "meta": {"persona": persona_id}, "season": season}

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
