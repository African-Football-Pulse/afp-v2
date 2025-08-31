# sections/s_top_african_players/providers/stats.py
from typing import Any, Dict, List, Optional

def load_stats(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = ctx.get("stats") or []
    return [coerce_stat(r) for r in rows if isinstance(r, dict)]

def coerce_stat(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": r.get("name") or r.get("player"),
        "club": r.get("club"),
        "minutes": _safe_int(r.get("minutes")),
        "xg": _safe_float(r.get("xg")),
        "xa": _safe_float(r.get("xa")),
        "goals": _safe_int(r.get("goals")),
        "assists": _safe_int(r.get("assists")),
        "shots": _safe_int(r.get("shots")),
        "nationality": (r.get("nationality") or r.get("nation") or "").lower() or None,
        "source_ref": r.get("source") or r.get("ref"),
    }

def _safe_int(v): 
    try: return int(v) if v is not None else None
    except: return None

def _safe_float(v):
    try: return float(v) if v is not None else None
    except: return None
