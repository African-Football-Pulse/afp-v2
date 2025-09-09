from datetime import datetime, UTC

def build_section(
    *,
    section_code: str,
    date: str,
    league: str = "_",
    topic: str = "_",
    layout: str = "alias-first",
    path_scope: str = "single",
    write_latest: bool = True,
    outdir: str = "outputs/sections",
    model: str = "gpt-4o-mini",
    type: str = "generic",
    dry_run: bool = False,
) -> dict:
    """
    Generate a static Daily Intro section for the podcast.
    Always includes the current date (UTC).
    """

    # Datum i "Month DD, YYYY" format (UTC)
    today_str = datetime.now(UTC).strftime("%B %d, %Y")

    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {today_str}, and this is your daily Premier League update. "
        "We’ll bring you the latest headlines, key talking points, "
        "and stories that matter most to African fans."
    )

    # Bygg manifest
    manifest = {
        "ok": True,
        "section_code": section_code,
        "type": type,
        "model": model,
        "created_utc": datetime.now(UTC).strftime("%Y%m%d_%H%M%S"),
        "league": league,
        "topic": topic,
        "date": date,
        "payload": {
            "slug": "intro_daily",
            "title": "Daily Intro",
            "text": text,
            "length_s": 15,
            "sources": [],
            "meta": {},
        },
        "outdir": outdir,
    }

    return manifest
