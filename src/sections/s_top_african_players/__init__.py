# sections/s_top_african_players/__init__.py
from typing import Any, Dict
from .providers.stats import load_stats
from .providers.news_items import load_items
from .logic.select import pick_top_from_stats, pick_top_from_news
from .renderers.stats import render_stats
from .renderers.news import render_news

def build(ctx: Dict[str, Any]) -> Dict[str, Any]:
    lang = ctx.get("lang", "en")
    top_n = ctx.get("config", {}).get("top_african_players", {}).get("top_n", 3)
    target_len = int(ctx.get("target_length_s", 50))

    stats = load_stats(ctx)               # strikt gränssnitt
    if stats:
        top = pick_top_from_stats(stats, top_n=top_n, ctx=ctx)
        if top:
            text, sources = render_stats(top, lang=lang, target_sec=target_len)
            return _payload(text, target_len, sources)

    items = load_items(ctx)
    top = pick_top_from_news(items, top_n=top_n, ctx=ctx)
    text, sources = render_news(top, lang=lang, target_sec=target_len, ctx=ctx)
    return _payload(text, target_len, sources)

def _payload(text, target_len, sources):
    return {
        "slug": "top_african_players",
        "title": "Top African Players this week",
        "text": text.strip(),
        "length_s": _estimate_len(text, target_len),
        "sources": sorted(set(s for s in sources if s)),
        "meta": {"persona": "JJ"},
    }

def _estimate_len(text: str, fallback: int) -> int:
    words = max(1, len(text.split()))
    sec = int(round(words / 2.5))
    return sec if abs(sec - fallback) > 5 else fallback
