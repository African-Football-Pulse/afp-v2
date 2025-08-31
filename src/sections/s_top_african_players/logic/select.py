# sections/s_top_african_players/logic/select.py
from typing import Any, Dict, List, Tuple
from .scoring import recency_weight, event_boost, source_weight

AFRICAN = {
  "nigeria","ghana","senegal","egypt","morocco","algeria","tunisia","ivory coast","cote d'ivoire",
  "cameroon","south africa","mali","burkina faso","guinea","congo","dr congo","gambia",
  "tanzania","uganda","kenya","zambia","zimbabwe","angola","benin","ethiopia","sudan","somalia"
}
TOP6_DEFAULT = {"Arsenal","Manchester City","Liverpool","Manchester United","Chelsea","Tottenham","Spurs","Man City","Man United"}

def pick_top_from_stats(stats: List[Dict[str, Any]], top_n: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    def is_african(nat): return (not nat) or nat in AFRICAN
    def score(p):
        return (p.get("goals",0)*4)+(p.get("assists",0)*3)+(p.get("xg",0.0)*1.5)+(p.get("xa",0.0)*1.0)+min(p.get("minutes",0),90)/90.0
    cand = [p for p in stats if (p.get("minutes") or 0) >= int(ctx.get("config",{}).get("top_african_players",{}).get("min_minutes",30)) and is_african(p.get("nationality"))]
    ranked = sorted(cand, key=score, reverse=True)[:top_n]
    return ranked

def pick_top_from_news(items: List[Dict[str, Any]], top_n: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    from collections import defaultdict
    top6 = set(ctx.get("config",{}).get("top_african_players",{}).get("top6_clubs", list(TOP6_DEFAULT)))
    agg = defaultdict(lambda: {"score":0.0,"freq":0,"club":None,"item_ids":[],"sources":set(),"sample_title":None,"last_seen":None})
    for it in items:
        plist = it.get("players") or []
        if not plist: continue
        sid, src = it.get("id"), it.get("source")
        title = it.get("title") or ""
        summary = it.get("summary") or ""
        published_at = it.get("published_at")
        r_w = recency_weight(published_at, ctx)
        s_w = source_weight(src, ctx)
        e_b = event_boost(f"{title} {summary}", ctx)
        mention = 1.0 + 0.3*s_w + 0.2*r_w + 0.5*e_b
        for name in plist:
            a = agg[name.strip()]
            a["score"] += mention
            a["freq"] += 1
            a["club"] = a["club"] or it.get("club")
            if sid: a["item_ids"].append(sid)
            if src: a["sources"].add(str(src))
            a["sample_title"] = title or a["sample_title"]
    ranked = sorted(
        agg.items(),
        key=lambda kv: (-round(kv[1]["score"],6), -len(kv[1]["sources"]), -(1 if (kv[1].get("club") in top6) else 0), kv[0].lower())
    )[:top_n]
    return [
      {"name": n, "club": m.get("club"), "score": round(m["score"],3), "freq": m["freq"], "item_ids": m["item_ids"], "num_sources": len(m["sources"]), "sample_title": m["sample_title"]}
      for n,m in ranked
    ]
