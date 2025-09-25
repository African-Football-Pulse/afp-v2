# src/producer/produce_candidates.py
import os
import sys
import json
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from src.storage import azure_blob
from src.producer import news_utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def log(msg: str):
    """Standardiserad loggning"""
    print(f"[produce_candidates] {msg}", flush=True)


def load_master_players():
    """Ladda masterlistan med afrikanska spelare (lista, inte dict)"""
    blob_path = "players/africa/players_africa_master.json"
    log(f"Loading master players from blob: {blob_path}")

    data = azure_blob.get_json(CONTAINER, blob_path)
    if isinstance(data, dict) and "players" in data:
        players = data["players"]
    elif isinstance(data, list):
        players = data
    else:
        players = []

    log(f"Loaded {len(players)} master players")
    if players:
        sample = [p.get("name") for p in players[:3]]
        log(f"Sample players: {sample}")
    return players


def normalize_date(value: str):
    """Normalisera publiceringsdatum till ISO8601"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).astimezone(timezone.utc).isoformat()
    except Exception:
        try:
            return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
        except Exception:
            return None


def candidate_from_news(item, master_players):
    """Bygg en kandidat från en nyhetsitem"""
    title = item.get("title") or ""
    summary = item.get("summary") or item.get("description") or ""
    src = item.get("source", "unknown")
    published_iso = normalize_date(item.get("published"))
    url = item.get("link")

    text_blob = f"{title} {summary}".lower()

    player = None
    direct_mention = 0.0

    for mp in master_players:
        name = (mp.get("name") or "").lower()
        club = (mp.get("club") or "").lower()

        if name and name in text_blob:
            player = mp
            direct_mention = 1.0
            break
        if club and club in text_blob:
            player = mp
            direct_mention = 0.4
            break

    if not player:
        return None

    return {
        "id": str(uuid.uuid4()),
        "player": {
            "name": player.get("name"),
            "club": player.get("club"),
            "id": player.get("id"),
        },
        "player_id": player.get("id"),
        "club_id": player.get("club_id"),
        "event": {"type": "news"},
        "source": {
            "name": src,
            "url": url,
        },
        "title": title,
        "summary": summary,
        "published_iso": published_iso,
        "direct_player_mention": direct_mention,
        "event_importance": 0.0,
        "recency_score": None,
        "novelty_24h": None,
        "language_match": None,
    }


def main():
    day = datetime.utcnow().strftime("%Y-%m-%d")
    log(f"START day={day} (UTC)")

    master_players = load_master_players()

    news_items = news_utils.load_curated_news(day)
    log(f"Loaded {len(news_items)} news items (curated)")

    candidates = []
    for item in news_items:
        cand = candidate_from_news(item, master_players)
        if cand:
            candidates.append(cand)

    log(f"Built {len(candidates)} raw candidates")

    # 🔑 Ny debug-statistik
    with_player = sum(1 for c in candidates if c.get("player_id"))
    log(f"Stats → total={len(candidates)}, with_player_id={with_player}, without={len(candidates)-with_player}")

    # Skriv till Azure
    out_path = f"producer/candidates/{day}/candidates.jsonl"
    text = "\n".join(json.dumps(c, ensure_ascii=False) for c in candidates)
    azure_blob.put_text(CONTAINER, out_path, text, content_type="application/json; charset=utf-8")
    log(f"Wrote {out_path}")
    log("DONE")


if __name__ == "__main__":
    main()
