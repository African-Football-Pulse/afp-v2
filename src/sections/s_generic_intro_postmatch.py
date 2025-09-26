# src/sections/s_generic_intro_postmatch.py
from datetime import datetime
from src.sections import utils
from src.sections.utils import write_outputs


def build_section(args) -> dict:
    """Produce a postmatch intro section"""

    # Get persona dynamically (news anchor)
    persona_id, persona_block = utils.get_persona_block("news_anchor", args.pod)

    try:
        dt = datetime.strptime(args.date, "%Y-%m-%d")
        date_str = dt.strftime("%B %d, %Y")
    except Exception:
        date_str = args.date

    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {date_str}, and we’re coming off a full round of Premier League action. "
        f"Stay tuned as we bring you the biggest results, standout performances, "
        f"and stories that matter most to fans across Africa."
    )

    payload = {
        "slug": "intro_postmatch",
        "title": "Postmatch Intro",
        "text": text,
        "length_s": int(round(len(text.split()) / 2.6)),
        "sources": {},
        "meta": {"persona": persona_id},
        "type": "generic",
        "model": "static",
    }

    return write_outputs(
        section_code=args.section,
        day=args.date,
        league=args.league or "_",
        payload=payload,
        status="ok",
        lang="en",
    )
