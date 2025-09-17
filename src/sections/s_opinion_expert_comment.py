import json
from pathlib import Path
from datetime import datetime
from src.lib.base_section import SectionManifest, register_section

SECTION_ID = "S.OPINION.EXPERT_COMMENT"

def _load_news(date: str, league: str, sources: list[str]) -> list[str]:
    """Laddar topp-rubriker från flera nyhetskällor."""
    items = []
    for src in sources:
        p = Path(f"collector/curated/news/{src}/{league}/{date}/items.json")
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict) and "items" in data:
                    data = data["items"]
                if isinstance(data, list):
                    for item in data[:2]:  # ta max 2 per källa
                        if "title" in item:
                            items.append(item["title"])
            except Exception as e:
                print(f"[WARN] Failed to parse {p}: {e}")
    return items

@register_section(SECTION_ID)
def build_section(*, date: str, league: str, **kwargs) -> SectionManifest:
    sources = [
        "bbc_football",
        "guardian_football",
        "independent_football",
        "sky_sports_premier_league",
    ]
    headlines = _load_news(date, league, sources)

    # Bygg expert-kommentaren
    intro = f"As reported by BBC and Guardian today, here are the key stories shaping the Premier League:"
    body_lines = [f"- {h}" for h in headlines] if headlines else ["No fresh headlines available today."]
    outro = "These developments continue to influence the dynamics across teams and players."

    text = "\n".join([intro, *body_lines, outro])

    return SectionManifest(
        section_code=SECTION_ID,
        text=text,
        persona="AK",
        date=date,
        league=league,
    )
