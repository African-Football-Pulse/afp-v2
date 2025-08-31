# sections/s_top_african_players/logic/scoring.py
from typing import Dict, Optional
from datetime import datetime, timezone

DEFAULT_SOURCE_WEIGHTS = {
    "guardian": 1.0, "bbc": 1.0, "sky": 0.9, "espn": 0.8,
    "telegraph": 0.7, "independent": 0.7, "mirror": 0.6,
    "goal": 0.6, "talksport": 0.5,
}
DEFAULT_EVENT_KEYWORDS = {
    "hat-trick":1.0,"hattrick":1.0,"brace":0.8,"goal":0.6,"goals":0.6,
    "assist":0.4,"assists":0.4,"clean sheet":0.5,"motm":0.7,
    "man of the match":0.7,"debut":0.5,"winner":0.4,"equaliser":0.3,"equalizer":0.3
}

def cfg(ctx, key, default):  # dotted get
    cur = ctx.get("config", {})
    for p in ("top_african_players."+key).split("."):
        if not isinstance(cur, dict) or p not in cur: return default
        cur = cur[p]
    return cur

def recency_weight(published_at: Optional[str], ctx) -> float:
    half_life = float(cfg(ctx, "recency.half_life_hours", 24))
    window_h = float(cfg(ctx, "news_window_hours", 48))
    now = datetime.fromisoformat(
        (ctx.get("now_iso") or datetime.now(timezone.utc).isoformat()).replace("Z","+00:00")
    )
    try:
        ts = datetime.fromisoformat(published_at.replace("Z","+00:00")) if published_at else None
    except:
        ts = None
    if not ts: return 0.7
    age_h = max(0.0, (now - ts).total_seconds()/3600)
    if window_h and age_h > window_h: return 0.1
    if half_life <= 0: return 1.0
    w = 2 ** (-(age_h/half_life))
    return max(0.1, w)

def event_boost(text: str, ctx) -> float:
    kws = {**DEFAULT_EVENT_KEYWORDS, **(cfg(ctx, "event_keywords", {}) or {})}
    t = (text or "").lower()
    best = 0.0
    for k,w in kws.items():
        if k in t: best = max(best, float(w) or 0.0)
    return min(1.0, best)

def source_weight(name: Optional[str], ctx) -> float:
    sw = {**DEFAULT_SOURCE_WEIGHTS, **(cfg(ctx, "source_weights", {}) or {})}
    if not name: return 0.5
    s = name.lower()
    for key,w in sw.items():
        if key in s: return float(w) or 0.5
    return float(cfg(ctx, "source_weight_default", 0.5))
