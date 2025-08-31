# sections/s_top_african_players/__init__.py
from typing import Any, Dict, List
import json, re
from urllib.request import urlopen, Request

from .providers.news_items import coerce_item
from .providers.stats import load_stats
from .logic.select import pick_top_from_stats, pick_top_from_news
from .renderers.stats import render_stats
from .renderers.news import render_news

# --- Enkel regex-baserad enrichment för gamla items (utan entities/published_at) ---
NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b")
STOPWORDS = {
    "The","A","An","And","Or","But","If","Of","In","On","At","To","For","With","From","By","As",
    "Man","City","United","FC","CF","SC","AC","Cup","League","Premier","La","Liga","Serie","Bundesliga",
    "Goal","Goals","Assist","Assists","Wins","Win","Draw","Loss","Match","Derby","Coach","Boss",
    "Liverpool","Arsenal","Chelsea","Tottenham","Spurs","Manchester","Newcastle","Everton","Aston","Villa",
    "Real","Barcelona","Bayern","Dortmund","PSG","Marseille","Roma","Inter","Milan",
    "Africa","African","Nigeria","Ghana","Senegal","Egypt","Morocco","Algeria","Tunisia","Ivory","Coast",
}
def _extract_candidates(text: str) -> List[str]:
    if not text: return []
    seen, out = set(), []
    for m in NAME_RE.finditer(text):
        name = m.group(1).strip()
        parts = name.split()
        if len(parts) == 1 or any(p in STOPWORDS for p in parts):
            continue
        if name not in seen:
            seen.add(name); out.append(name)
    return out

def _inline_enrich_players(item: dict) -> dict:
    title = (item.get("title") or "").strip()
    summary = (item.get("summary") or item.get("description") or "").strip()
    if not item.get("published_at"):
        item["published_at"] = item.get("published_iso") or item.get("published")
    entities = item.get("entities") or {}
    players = entities.get("players") or []
    if not players:
        players = _extract_candidates(f"{title} {summary}")
        if players:
            entities["players"] = players
            item["entities"] = entities
    return item

# --- Små helpers ---
def _fetch_json(path: str):
    if path.startswith("http"):
        req = Request(path, headers={"User-Agent": "afp-producer/1.0"})
        with urlopen(req) as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _payload(text: str, target_len: int, sources: List[str]) -> Dict[str, Any]:
    words = max(1, len(text.split()))
    sec = int(round(words / 2.5))
    if abs(sec - target_len) <= 5:
        sec = target_len
    return {
        "slug": "top_african_players",
        "title": "Top African Players this week",
        "text": text.strip(),
        "length_s": sec,
        "sources": sorted({s for s in (sources or []) if s}),
        "meta": {"persona": "JJ"},
    }

# --- Primärt API (direktanrop om ni vill) ---
def build(ctx: Dict[str, Any]) -> Dict[str, Any]:
    lang = ctx.get("lang", "en")
    top_n = int(ctx.get("config", {}).get("top_african_players", {}).get("top_n", 3))
    target_len = int(ctx.get("target_length_s", 50))

    stats = load_stats(ctx)
    if stats:
        top = pick_top_from_stats(stats, top_n=top_n, ctx=ctx)
        if top:
            text, sources = render_stats(top, lang=lang, target_sec=target_len)
            return _payload(text, target_len, sources)

    items = ctx.get("items") or []
    if not items:
        return _payload("No news items available.", target_len, [])

    top = pick_top_from_news(items, top_n=top_n, ctx=ctx)
    text, sources = render_news(top, lang=lang, target_sec=target_len, ctx=ctx)
    return _payload(text, target_len, sources)

# --- Runner som produce_section.py anropar ---
def build_section(section_code: str, news_path: str, date: str, league: str,
                  outdir: str, layout: str=None, path_scope: str=None,
                  personas_path: str=None, model: str=None, speaker: str=None,
                  write_latest: bool=True, **_):
    raw = _fetch_json(news_path)
    raw_items = raw["items"] if isinstance(raw, dict) and "items" in raw else raw

    items = []
    for r in raw_items:
        c = coerce_item(r)        # tolerant mapping (title/summary/source/published_at/players)
        c = _inline_enrich_players(c)  # fyll entities + published_at om saknas
        items.append(c)

    ctx = {
        "league": league,
        "lang": "en",
        "items": items,
        "config": {"top_african_players": {"top_n": 3}},
    }
    payload = build(ctx)
    return {
        "section_code": section_code,
        "date": date,
        "league": league,
        "payload": payload,
        "outdir": outdir,
    }
