# afp-v2/src/tools/used_news_filter.py
import json
from pathlib import Path
from typing import List


def _used_file(date: str, league: str) -> Path:
    """
    Returnerar path till used_news.jsonl för given dag/ligga.
    Skapar katalogen om den inte finns.
    """
    base = Path("collector/curated/used_news") / league / date
    base.mkdir(parents=True, exist_ok=True)
    return base / "used_news.jsonl"


def mark_as_used(items: List[str], date: str, league: str, section: str) -> None:
    """
    Markera artiklar som använda.
    Sparar i JSONL-format: {"section": str, "item": str}
    """
    if not items:
        return

    fpath = _used_file(date, league)
    with fpath.open("a", encoding="utf-8") as f:
        for item in items:
            record = {"section": section, "item": item}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_used(date: str, league: str) -> List[str]:
    """
    Läs alla tidigare använda artiklar för given dag/ligga.
    """
    fpath = _used_file(date, league)
    if not fpath.exists():
        return []

    used = []
    with fpath.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                used.append(rec["item"])
            except Exception:
                continue
    return used


def filter_used(candidates: List[str], date: str, league: str) -> List[str]:
    """
    Filtrerar bort kandidater som redan är använda.
    Returnerar en lista med unika kandidater.
    """
    used = set(load_used(date, league))
    filtered = [c for c in candidates if c not in used]

    dropped = len(candidates) - len(filtered)
    if dropped > 0:
        print(
            f"[INFO] Filtered out {dropped} already-used items "
            f"(date={date}, league={league})"
        )

    return filtered
