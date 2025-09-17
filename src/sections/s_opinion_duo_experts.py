import json
from pathlib import Path
from src.lib.base_section import SectionManifest, register_section

SECTION_ID = "S.OPINION.DUO_EXPERTS"

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
                    for item in data[:2]:  # topp 2 per källa
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

    dialogue = []
    if headlines:
        dialogue.append({"speaker": "AK", "text": f"Let’s look at today’s headlines: {headlines[0]}"})
        if len(headlines) > 1:
            dialogue.append({"speaker": "JJK", "text": f"Absolutely, another big talking point is: {headlines[1]}"})
        if len(headlines) > 2:
            dialogue.append({"speaker": "AK", "text": f"And {headlines[2]} is definitely stirring up debate among fans."})
    else:
        dialogue.append({"speaker": "AK", "text": "There are no major new headlines today, so let’s reflect on recent matches instead."})

    return SectionManifest(
        section_code=SECTION_ID,
        dialogue=dialogue,
        speakers=["AK", "JJK"],
        date=date,
        league=league,
    )
