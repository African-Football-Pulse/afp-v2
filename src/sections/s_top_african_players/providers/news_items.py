# sections/s_top_african_players/providers/news_items.py
from typing import Any, Dict, List
from datetime import datetime, timezone

FIELD_MAP = {
    "id": ["id"],
    "title": ["title"],
    "summary": ["summary", "description"],
    "published_at": ["published_at", "pub_date"],
    "source": ["source", "publisher", "domain"],
    "club": ["club"],
    "players": ["entities.players", "players"],  # stöd för båda
}

def load_items(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = ctx.get("items") or []
    return [coerce_item(r) for r in rows if isinstance(r, dict)]

def coerce_item(r: Dict[str, Any]) -> Dict[str, Any]:
    def getdot(d, dotted, default=None):
        cur = d
        for p in dotted.split("."):
            if not isinstance(cur, dict) or p not in cur: return default
            cur = cur[p]
        return cur
    out = {
        "id": r.get("id"),
        "title": r.get("title"),
        "summary": r.get("summary") or r.get("description"),
        "published_at": (r.get("published_at") or r.get("pub_date")),
        "source": r.get("source") or r.get("publisher") or r.get("domain"),
        "club": r.get("club"),
        "players": getdot(r, "entities.players") or r.get("players") or [],
    }
    return out
