# src/sections/s_generic_intro_daily.py
from datetime import datetime
from src.sections.utils import write_outputs


def build_section(args) -> dict:
    """Produce a daily intro section"""

    # Format date
    try:
        dt = datetime.strptime(args.date, "%Y-%m-%d")
        date_str = dt.strftime("%B %d, %Y")
    except Exception:
        date_str = args.date

    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {date_str}, and this is your daily Premier League update. "
        f"We’ll bring you the latest headlines, key talking points, "
        f"and stories that matter most to African fans."
    )

    payload = {
        "slug": "intro_daily",
        "title": "Daily Intro",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {},
        "meta": {"persona": "System"},
        "type": "generic",
        "model": "static",
    }

    return write_outputs(
        section_id=args.section,
        day=args.date,
        league=args.league or "_",
        payload=payload,
        status="ok",
        lang="en",
    )
