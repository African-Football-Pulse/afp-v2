# sections/s_top_african_players/__init__.py
import json
from typing import Any, Dict, List
from urllib.request import urlopen
from .providers.news_items import load_items, coerce_item
from .providers.stats import load_stats
from .logic.select import pick_top_from_stats, pick_top_from_news
from .renderers.stats import render_stats
from .renderers.news import render_news

def _fetch_json(path: str) -> List[Dict[str, Any]]:
    # path kan vara SAS-URL eller lokal fil
    if path.startswith("http"):
        with urlopen(path) as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build(ctx: Dict[str, Any]) -> Dict[str, Any]:
    # ren, intern API – används om ni vill anropa direkt
    lang = ctx.get("lang", "en")
    top_n = ctx.get("config", {}).get("top_african_players", {}).get("top_n", 3)
    target_len = int(ctx.get("target_length_s", 50))

    stats = load_stats(ctx)
    if stats:
        top = pick_top_from_stats(stats, top_n=top_n, ctx=ctx)
        if top:
            text, sources = render_stats(top, lang=lang, target_sec=target_len)
            return _payload(text, target_len, sources)

    items = load_items(ctx)
    top = pick_top_from_news(items, top_n=top_n, ctx=ctx)
    text, sources = render_news(top, lang=lang, target_sec=target_len, ctx=ctx)
    return _payload(text, target_len, sources)

def build_section(section_code: str, news_path: str, date: str, league: str,
                  outdir: str, layout: str=None, path_scope: str=None,
                  personas_path: str=None, model: str=None, speaker: str=None,
                  write_latest: bool=True, **_):
    # runner för produce_section.py → laddar items.json från collect och kör build()
    raw = _fetch_json(news_path)
    items = [coerce_item(r) for r in raw if isinstance(r, dict)]
    ctx = {
        "league": league,
        "lang": "en",
        "items": items,
        "config": {"top_african_players": {"top_n": 3}},
    }
    payload = build(ctx)
    # skriv ut till stdout så produce_section kan fånga manifest
    return {
        "section_code": section_code,
        "date": date,
        "league": league,
        "payload": payload,
        "outdir": outdir,
    }

def _payload(text: str, target_len: int, sources: List[str]) -> Dict[str, Any]:
    def _estimate_len(t: str, fallback: int) -> int:
        words = max(1, len(t.split()))
        sec = int(round(words / 2.5))
        return sec if abs(sec - fallback) > 5 else fallback
    return {
        "slug": "top_african_players",
        "title": "Top African Players this week",
        "text": text.strip(),
        "length_s": _estimate_len(text, target_len),
        "sources": sorted(set(s for s in sources if s)),
        "meta": {"persona": "JJ"},
    }
