# src/sections/s_top_african_players/__init__.py
from typing import Any, Dict, List
import json, re
from urllib.request import urlopen, Request

from .providers.news_items import coerce_item
from .providers.stats import load_stats
from .logic.select import pick_top_from_stats, pick_top_from_news
from .renderers.stats import render_stats
from .renderers.news import render_news
from .renderers.gpt import render_gpt  # <— NY

NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b")
STOPWORDS = {
    "The","A","An","And","Or","But","If","Of","In","On","At","To","For","With","From","By","As",
    "Man","City","United","FC","CF","SC","AC","AFC","BC","Cup","League","Premier","La","Liga","Serie","Bundesliga",
    "Goal","Goals","Assist","Assists","Wins","Win","Draw","Loss","Match","Derby","Coach","Boss","Transfer","Rumours","Rumors",
    "Liverpool","Arsenal","Chelsea","Tottenham","Spurs","Manchester","Newcastle","Everton","Aston","Villa","Forest","Palace",
    "Real","Barcelona","Bayern","Dortmund","PSG","Marseille","Roma","Inter","Milan",
    "Old","Firm","Etihad","Emirates","Anfield","St","James","Park","Bridge","Weekly","Football","Guardian","Independent","Sky","Podcast",
    "Africa","African","Nigeria","Ghana","Senegal","Egypt","Morocco","Algeria","Tunisia","Ivory","Coast",
}
def _extract_candidates(text: str) -> List[str]:
    if not text: return []
    seen, out = set(), []
    for m in NAME_RE.finditer(text):
        name = m.group(1).strip(); parts = name.split()
        if len(parts) < 2 or any(p in STOPWORDS for p in parts): continue
        if name not in seen: seen.add(name); out.append(name)
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

def _fetch_json(path: str):
    if path.startswith("http"):
        req = Request(path, headers={"User-Agent": "afp-producer/1.0"})
        with urlopen(req) as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _payload(text: str, target_len: int, sources: List[str]) -> Dict[str, Any]:
    words = max(1, len(text.split())); sec = int(round(words / 2.5))
    if abs(sec - target_len) <= 5: sec = target_len
    return {"slug":"top_african_players","title":"Top African Players this week",
            "text":text.strip(),"length_s":sec,"sources":sorted({s for s in (sources or []) if s}),
            "meta":{"persona":"JJ"}}

def build(ctx: Dict[str, Any]) -> Dict[str, Any]:
    lang = ctx.get("lang", "en")
    top_n = int(ctx.get("config",{}).get("top_african_players",{}).get("top_n",3))
    target_len = int(ctx.get("target_length_s",50))

    stats = load_stats(ctx)
    if stats:
        top = pick_top_from_stats(stats, top_n=top_n, ctx=ctx)
        if top:
            text, sources = render_stats(top, lang=lang, target_sec=target_len)
            return _payload(text, target_len, sources)

    items = ctx.get("items") or []
    if not items:
        return _payload("No news items available." if lang!="sv" else "Inga nyheter tillgängliga.", target_len, [])

    top = pick_top_from_news(items, top_n=top_n, ctx=ctx)

    # === GPT först, annars fallback till regelbaserad ===
    nlg_provider = ctx.get("config",{}).get("top_african_players",{}).get("nlg",{}).get("provider") \
                   or os.getenv("AFP_NLG", "").lower()
    if nlg_provider == "gpt":
        text, sources = render_gpt(top, lang=lang, target_sec=target_len, ctx=ctx)
    else:
        text, sources = render_news(top, lang=lang, target_sec=target_len, ctx=ctx)

    return _payload(text, target_len, sources)

def build_section(section_code: str, news_path: str, date: str, league: str,
                  outdir: str, layout: str=None, path_scope: str=None,
                  personas_path: str=None, model: str=None, speaker: str=None,
                  write_latest: bool=True, **kwargs):
    import re, os
    def _swap_source(p: str, new_src: str) -> str:
        return re.sub(r'(/news/)([^/]+)(/)', r'\1' + new_src + r'\3', p, count=1)
    def _load_items_from(p: str) -> list:
        raw = _fetch_json(p); return raw["items"] if isinstance(raw, dict) and "items" in raw else (raw or [])

    lang = kwargs.get("lang","en")
    news_paths: list[str] = []
    news_multi = kwargs.get("news_multi")
    extras = kwargs.get("extra_sources") or kwargs.get("sources")
    if isinstance(news_multi, list) and news_multi:
        news_paths = [p for p in news_multi if isinstance(p, str)]
        if news_path and all(news_path != p for p in news_paths): news_paths.insert(0, news_path)
    elif isinstance(extras, list) and extras:
        news_paths = [news_path] if news_path else []
        for src in extras: news_paths.append(_swap_source(news_path, src))
    else:
        news_paths = [news_path] if news_path else []

    seen_id, seen_link, seen_title = set(), set(), set()
    raw_items: list[dict] = []
    for p in news_paths:
        try: arr = _load_items_from(p)
        except Exception: continue
        for r in arr:
            rid, link, title = r.get("id"), r.get("link"), r.get("title")
            if rid and rid in seen_id: continue
            if link and link in seen_link: continue
            if (not rid and not link) and title and title in seen_title: continue
            if rid: seen_id.add(rid)
            if link: seen_link.add(link)
            if title: seen_title.add(title)
            raw_items.append(r)

    items = []
    for r in raw_items:
        c = coerce_item(r); c = _inline_enrich_players(c); items.append(c)

    ctx = {
        "league": league, "lang": lang, "items": items,
        "config": {
            "top_african_players": {
                "top_n": 3,
                "africa": {"whitelist_only": True, "whitelist_path": "config/player_lexicon_africa.txt", "boost": 0.3},
                # Slå på GPT här:
                "nlg": {"provider": os.getenv("AFP_NLG", "gpt"), "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini")}
            }
        },
    }
    payload = build(ctx)
    return {"section_code": section_code, "date": date, "league": league, "payload": payload, "outdir": outdir}
