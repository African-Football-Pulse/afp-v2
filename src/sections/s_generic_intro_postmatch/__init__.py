from datetime import datetime

def build_section(context=None, **kwargs):
    """
    Build the Postmatch intro section.
    Generates a static but date-aware intro for full matchday episodes.
    """

    today = datetime.utcnow().strftime("%B %d, %Y")

    text = (
        f"Welcome to African Football Pulse! "
        f"It’s {today}, and we’re coming off a full round of Premier League action. "
        f"Stay tuned as we bring you the biggest results, standout performances, "
        f"and stories that matter most to fans across Africa."
    )

    manifest = {
        "ok": True,
        "manifest": {
            "section_code": "S.GENERIC.INTRO_POSTMATCH",
            "date": today,
            "league": "premier_league",
            "payload": {
                "slug": "generic_intro_postmatch",
                "title": "Episode Introduction (Postmatch)",
                "text": text,
            },
        },
    }

    return manifest
