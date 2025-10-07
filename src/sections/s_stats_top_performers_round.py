import os
import io
import pandas as pd
from datetime import datetime, timedelta
from src.sections import utils
from src.producer import gpt
from src.storage import azure_blob


def detect_latest_round_dates(container: str, league_id: str, season: str) -> list:
    """
    Identifierar senaste spelomgångens datum baserat på warehouse/match_results_africa.parquet.
    Returnerar en lista av datum (ISO-strängar) som tillhör senaste omgången.
    """
    try:
        blob_path = f"warehouse/match_results_africa/{season}/{league_id}.parquet"
        blob_bytes = azure_blob.get_bytes(container, blob_path)
        df = pd.read_parquet(io.BytesIO(blob_bytes))

        if df.empty or "date" not in df.columns or "round" not in df.columns:
            print(f"[detect_latest_round] Invalid or empty data in {blob_path}")
            return []

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        today = datetime.utcnow().date()

        # Filtrera endast matcher som spelats (datum ≤ idag)
        df_past = df[df["date"].dt.date <= today]

        if df_past.empty:
            print("[detect_latest_round] No past matches found")
            return []

        # Hitta senaste omgång (max round eller senast spelade datum)
        latest_round = df_past["round"].max()
        latest_dates = sorted(df_past[df_past["round"] == latest_round]["date"].dt.date.unique())

        print(f"[detect_latest_round] Latest round={latest_round}, dates={latest_dates}")
        return [d.isoformat() for d in latest_dates]
    except Exception as e:
        print(f"[detect_latest_round] Failed to detect round: {e}")
        return []


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
        }
        league_id = league_map.get(league)

    if not league_id:
        raise ValueError(f"league_id could not be resolved for league={league}")

    persona_id, _ = utils.get_persona_block("storyteller", pod)
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    # 🧩 Identifiera senaste round_dates automatiskt
    round_dates = detect_latest_round_dates(container, league_id, season)

    blob_path = f"warehouse/metrics/match_performance_africa/{season}/{league_id}.parquet"
    print(f"[stats_top_performers_round] Loading metrics from: {blob_path}")

    try:
        blob_bytes = azure_blob.get_bytes(container, blob_path)
        df = pd.read_parquet(io.BytesIO(blob_bytes))
    except Exception as e:
        print(f"[stats_top_performers_round] Error loading {blob_path}: {e}")
        text = f"No performance data available for the latest round ({season})."
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
        text = f"No performance data available for the current round ({season})."
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

    # 🔍 Filtrera på senaste omgång (round_dates)
    if round_dates and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].dt.date.astype(str).isin(round_dates)]
        print(f"[stats_top_performers_round] Filtered to {len(df)} records for round_dates={round_dates}")

    # Sortera efter score/rating och ta top 5
    sort_col = "score" if "score" in df.columns else "rating"
    df_sorted = df.sort_values(by=sort_col, ascending=False).head(5)

    # Bygg sammanfattning för GPT
    top_players = [
        f"{row['player_name']} ({row['team']}) score {row[sort_col]}"
        for _, row in df_sorted.iterrows()
    ]
    summary = "; ".join(top_players)

    # 🔹 Anropa GPT
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

    manifest = {
        "script": text,
        "meta": {"persona": persona_id},
        "season": season,
        "round_dates": round_dates,
    }

    return utils.write_outputs(section_code, day, league, lang, pod, manifest, "success", payload)
