# afp-v2/src/tools/used_news_filter.py
import json
from pathlib import Path
from typing import List, Dict, Union


def _used_file(date: str, league: str) -> Path:
    """
    Returnerar path till used_news.jsonl för given dag/ligga.
    Skapar katalogen om den inte finns.
    """
    base = Path("collector/curated/used_news") / league / date
    base.mkdir(parents=True, exist_ok=True)
    return base / "used_news.jsonl"


def mark_as_used(
    items: List[Union[str, Dict[str, str]]], date: str, league: str, section: str
) -> None:
    """
    Markera artiklar som använda.
    - items kan vara en lista med strängar (titlar) eller dicts med metadata.
    - Sparas i JSONL-format: {"section": str, "title": str, "id": str (valfritt), "url": str (valfritt)}
    """
    if not items:
        return

    fpath = _used_file(date, league)
    with fpath.open("a", encoding="utf-8") as f:
        for item in items:
            if isinstance(item, str):
                record = {"section": section, "title": item}
            elif isinstance(item, dict):
                record = {
                    "section": section,
                    "title": item.get("title", ""),
                    "id": item.get("id"),
                    "url": item.get("url"),
                }
            else:
                continue
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_used(date: str, league: str) -> List[Dict[str, str]]:
    """
    Läs alla tidigare använda artiklar för given dag/ligga.
    Returnerar en lista av dicts med title/id/url.
    """
    fpath = _used_file(date, league)
    if not fpath.exists():
        return []

    used = []
    with fpath.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                used.append(rec)
            except Exception:
                continue
    return used


def filter_used(
    candidates: List[Dict[str, str]], date: str, league: str
) -> List[Dict[str, str]]:
    """
    Filtrerar bort kandidater som redan är använda.
    - Kandidater förväntas vara dicts med minst 'title' (och gärna 'id' eller 'url').
    """
    used_records = load_used(date, league)
    used_ids = {u.get("id") for u in used_records if u.get("id")}
    used_urls = {u.get("url") for u in used_records if u.get("url")}
    used_titles = {u.get("title") for u in used_records if u.get("title")}

    filtered = []
    dropped = 0

    for c in candidates:
        cid, curl, ctitle = c.get("id"), c.get("url"), c.get("title")

        if (cid and cid in used_ids) or (curl and curl in used_urls) or (
            ctitle and ctitle in used_titles
        ):
            dropped += 1
            continue
        filtered.append(c)

    if dropped > 0:
        print(
            f"[INFO] Filtered out {dropped} already-used items "
            f"(date={date}, league={league})"
        )

    return filtered
