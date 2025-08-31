# src/sections/s_top_african_players/renderers/news.py
from typing import Dict, List, Tuple

def _dedupe(seq):
    seen = set(); out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def _first_sentence(text: str) -> str:
    if not text: return ""
    t = text.strip()
    for stop in [". ", " – ", " — ", " | "]:
        if stop in t:
            return t.split(stop, 1)[0].rstrip(".") + "."
    return (t[:140].rstrip(" .") + ".") if len(t) > 140 else (t.rstrip(".") + ".")

def _line_en(p: Dict[str, any]) -> str:
    name = p.get("name") or "—"
    club = p.get("club")
    title = p.get("sample_title") or ""
    lead = _first_sentence(title)
    return f"{name} ({club}): {lead}" if club else f"{name}: {lead}"

def render_news(players: List[Dict[str, any]],
                lang: str = "en",
                target_sec: int = 50,
                ctx: Dict[str, any] | None = None) -> Tuple[str, List[str]]:
    ctx = ctx or {}
    items = ctx.get("items") or []
    id_to_link = {it.get("id"): (it.get("link") or it.get("url")) for it in items}

    links: List[str] = []
    for p in players:
        for iid in p.get("item_ids", []):
            link = id_to_link.get(iid)
            if link: links.append(link)
    links = _dedupe(links)

    if not players:
        return ("No clear African standouts in today’s headlines.", links)

    lines = [_line_en(p) for p in players]
    text = "Top African names this week:\n" + "\n".join(f"- {ln}" for ln in lines)
    return (text, links)
