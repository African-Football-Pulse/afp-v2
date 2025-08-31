# src/sections/s_top_african_players/renderers/gpt.py
from typing import Dict, List, Tuple
import os

def _dedupe(seq):
    seen, out = set(), []
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def _first_sentence(t: str) -> str:
    if not t: return ""
    t = t.strip()
    for stop in [". ", " – ", " — ", " | "]:
        if stop in t:
            return t.split(stop, 1)[0].rstrip(".")
    return t[:160].rstrip(" .")

def _collect_links(players: List[Dict[str, any]], items: List[Dict[str, any]]) -> List[str]:
    id_to_link = {it.get("id"): (it.get("link") or it.get("url")) for it in (items or [])}
    links: List[str] = []
    for p in (players or []):
        for iid in p.get("item_ids", []):
            link = id_to_link.get(iid)
            if link: links.append(link)
    return _dedupe(links)

def _build_prompt(players: List[Dict[str, any]]) -> List[Dict[str, any]]:
    facts = []
    for p in players:
        facts.append({
            "name": p.get("name"),
            "club": p.get("club"),
            "headline": _first_sentence(p.get("sample_title") or ""),
            "mentions": p.get("freq"),
            "sources": p.get("num_sources"),
        })
    system = (
        "You are a sports journalist. Write concise, fact-based copy.\n"
        "Use only the facts provided (no speculation).\n"
        "Output a title on the first line, then exactly N bullets—one sentence per player—"
        "formatted as: '- Name (Club): tight takeaway from the headline.'\n"
        "No scores, no links, no emojis."
    )
    title = "Top African names this week"
    user = {
        "role": "user",
        "content": [
            {"type": "input_text", "text":
                f"TITLE={title}\nN={len(players)}\nFACTS={facts}\n"
                "Write only the title and N bullets."
            }
        ],
    }
    return [{"role": "system", "content": system}, user]

def render_gpt(players: List[Dict[str, any]],
               lang: str = "en",
               target_sec: int = 50,
               ctx: Dict[str, any] | None = None) -> Tuple[str, List[str]]:
    items = (ctx or {}).get("items") or []
    links = _collect_links(players, items)
    if not players:
        return ("No clear African standouts in today’s headlines.", links)

    try:
        from openai import OpenAI  # pip install openai>=1.0
        model = (ctx or {}).get("config", {}).get("top_african_players", {}) \
                    .get("nlg", {}).get("model", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        client = OpenAI()
        messages = _build_prompt(players)
        resp = client.responses.create(model=model, input=messages)
        text = resp.output_text.strip()
        # ensure max N bullets
        N = len(players)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if lines:
            title = lines[0].strip()
            bullets = [ln for ln in lines[1:] if ln.lstrip().startswith(("-", "–", "—"))]
            if len(bullets) > N: bullets = bullets[:N]
            cleaned = [title] + [b if b.startswith("-") else f"- {b.lstrip('—– ')}" for b in bullets]
            text = "\n".join(cleaned).strip()
        return (text, links)
    except Exception:
        from .news import render_news
        return render_news(players, lang="en", target_sec=target_sec, ctx=ctx)
