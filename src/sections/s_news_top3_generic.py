# src/sections/s_news_top3_generic.py
import os, json
from datetime import datetime, timezone
from typing import Any, Dict, List
from pathlib import Path

from src.storage import azure_blob

CONTAINER = os.getenv("BLOB_CONTAINER", "afp")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def _load_items_for_feed(feed: str, league: str, day: str) -> List[Dict[str, Any]]:
    path = f"curated/news/{feed}/{league}/{day}/items.json"
    try:
        return azure_blob.get_json(CONTAINER, path)
    except Exception:
        return []


def _sort_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda x: x.get("published_iso") or "", reverse=True)


def _pick_topn_diverse(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    seen_players, picked = set(), []
    for it in items:
        players = it.get("entities", {}).get("players", [])
        if not players:
            continue
        if any(p in seen_players for p in players):
            continue
        seen_players.update(players)
        picked.append(it)
        if len(picked) >= n:
            break
    return picked


def _render_section(league: str, day: str, items: List[Dict[str, Any]]) -> str:
    lines = [f"Top {len(items)} African player news for {league} ({day}):"]
    for it in items:
        title = it.get("title") or ""
        src = it.get("source") or ""
        lines.append(f"- {title} ({src})")
    return "\n".join(lines)


def build_section(args=None):
    """Bygg en sektion för topp 3 nyheter, skriver ut mappstruktur."""
    league = getattr(args, "league", "premier_league")
    day = getattr(args, "date", today_str())
    feeds_csv = getattr(
        args,
        "feeds",
        "guardian_football,bbc_football,sky_sports_premier_league,independent_football",
    )
    top_n = int(getattr(args, "top_n", 3))
    lang = getattr(args, "lang", "en")
    section_id = getattr(args, "section_code", "S.NEWS.TOP3")

    all_items: List[Dict[str, Any]] = []
    feeds = [f.strip() for f in feeds_csv.split(",") if f.strip()]
    for feed in feeds:
        feed_items = _load_items_for_feed(feed, league, day)
        print(f"[s_news_top3_generic] {feed} → {len(feed_items)} items")
        all_items.extend(feed_items)

    if not all_items:
        print("[s_news_top3_generic] Inga items hittades totalt")
        payload = {
            "slug": "top3_news",
            "title": "Top 3 African Player News",
            "text": "No news items available.",
            "length_s": 2,
            "sources": feeds,
            "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
            "items": [],
        }
        return _write_outputs(section_id, day, league, payload, feeds, lang, status="no_items")

    items_sorted = _sort_items(all_items)
    picked = _pick_topn_diverse(items_sorted, top_n)
    body = _render_section(league, day, picked)

    print(f"[s_news_top3_generic] Totalt {len(all_items)} items → valde {len(picked)}")

    payload = {
        "slug": "top3_news",
        "title": "Top 3 African Player News",
        "text": body,
        "length_s": len(picked) * 30,  # antag 30 sek per nyhet
        "sources": feeds,
        "meta": {"persona": "Ama K (Amarachi Kwarteng)"},
        "items": picked,
    }

    return _write_outputs(section_id, day, league, payload, feeds, lang, status="ok")


def _write_outputs(section_id: str, day: str, league: str, payload: Dict[str, Any],
                   feeds: List[str], lang: str, status: str) -> Dict[str, Any]:
    """Skriv ut section.json, section.md, section_manifest.json"""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    base = f"sections/{section_id}/{day}/{league}/_"
    outdir = Path(base)
    outdir.mkdir(parents=True, exist_ok=True)

    # section.json
    (outdir / "section.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # section.md
    (outdir / "section.md").write_text(f"### {payload['title']}\n\n{payload['text']}\n", encoding="utf-8")

    # manifest
    manifest = {
        "section_code": section_id,
        "type": "news",
        "model": "gpt-4o-mini",
        "created_utc": ts,
        "league": league,
        "topic": "_",
        "date": day,
        "blobs": {"json": f"{base}/section.json", "md": f"{base}/section.md"},
        "metrics": {"length_s": payload.get("length_s", 0)},
        "sources": {"feeds": feeds},
        "lang": lang,
        "status": status,
    }
    (outdir / "section_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return manifest
