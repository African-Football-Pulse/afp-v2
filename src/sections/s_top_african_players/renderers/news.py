# sections/s_top_african_players/renderers/news.py
from typing import Any, Dict, List, Tuple

def render_news(players: List[Dict[str, Any]], lang: str="en", target_sec: int=50, ctx: Dict[str, Any]=None) -> Tuple[str, List[str]]:
    if not players:
        return ("No clear African standouts from the news this week." if lang.startswith("en")
                else "Inga tydliga afrikanska toppnamn från nyheterna denna vecka."), []
    lines = ["Top African names in the headlines:" if lang.startswith("en") else "Afrikanska toppnamn i rubrikerna:"]
    sources: List[str] = []
    for p in players:
        club = f" ({p.get('club')})" if p.get("club") else ""
        snippet = p.get("sample_title") or ("headline-driven mention" if lang.startswith("en") else "rubrikdriven notis")
        if lang.startswith("en"):
            lines.append(f"- {p['name']}{club} — {p.get('num_sources',1)} source(s), score {p.get('score')}: {snippet}")
        else:
            lines.append(f"- {p['name']}{club} — {p.get('num_sources',1)} källa(or), score {p.get('score')}: {snippet}")
        sources.extend(p.get("item_ids", []))
    lines.append("More next time." if lang.startswith("en") else "Mer nästa gång.")
    return "\n".join(lines), sources
