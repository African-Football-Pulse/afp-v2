# src/producer/produce_scoring.py
import os, json
from datetime import datetime, timezone
from collections import Counter

from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


SOURCE_AUTHORITY = {
    "bbc_football": 0.85,
    "guardian_football": 0.85,
    "sky_sports_premier_league": 0.85,
    "independent_football": 0.7,
    # klubbfeeds kan få 0.9
}


def compute_score(c):
    return round(
        0.35 * c.get("direct_player_mention", 0)
        + 0.25 * c.get("event_importance", 0)
        + 0.15 * c.get("source", {}).get("authority", 0.5)
        + 0.15 * c.get("recency_score", 0)
        + 0.05 * c.get("novelty_24h", 0)
        + 0.05 * c.get("language_match", 1.0),
        3,
    )


def main():
    day = today_str()
    in_path = f"producer/candidates/{day}/candidates.jsonl"
    out_path = f"producer/candidates/{day}/scored.jsonl"

    text = azure_blob.get_text(CONTAINER, in_path)
    candidates = [json.loads(line) for line in text.splitlines() if line.strip()]

    print(f"[produce_scoring] Loaded {len(candidates)} candidates")

    now = datetime.now(timezone.utc)
    scored = []

    for c in candidates:
        # recency
        recency_score = 0
        if c.get("published_iso"):
            try:
                dt = datetime.fromisoformat(c["published_iso"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                hours = (now - dt).total_seconds() / 3600
                recency_score = max(0, 1 - hours / 48)
            except Exception:
                pass

        c["recency_score"] = recency_score
        c["novelty_24h"] = 1   # TODO: riktig dedupe/novelty
        c["language_match"] = 1.0
        c["source"]["authority"] = SOURCE_AUTHORITY.get(c["source"]["name"], 0.5)

        c["score"] = compute_score(c)
        scored.append(c)

    # statistik
    if scored:
        scores = [c["score"] for c in scored]
        players = [c["player"]["name"] for c in scored if c.get("player")]
        clubs = [c["player"].get("club") for c in scored if c.get("player")]

        print(
            f"[produce_scoring] Stats → "
            f"candidates={len(scored)}, "
            f"unique_players={len(set(players))}, "
            f"unique_clubs={len(set(clubs))}"
        )
        print(
            f"[produce_scoring] Score distribution → "
            f"min={min(scores):.2f}, max={max(scores):.2f}, avg={sum(scores)/len(scores):.2f}"
        )

    text_out = "\n".join(json.dumps(s, ensure_ascii=False) for s in scored)
    azure_blob.put_text(CONTAINER, out_path, text_out, content_type="application/jsonl")

    print(f"[produce_scoring] Wrote {out_path}")
    print("[produce_scoring] DONE")


if __name__ == "__main__":
    main()
