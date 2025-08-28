# src/sections/s_news_top3_generic.py
"""
S.NEWS.TOPN – Generic Top-N över flera källor.

- Läser curated/news/{feed}/{league}/{day}/items.json för valda FEEDS
- Sorterar på published_iso (fallback: published) fallande
- Väljer max 1 per källa tills TOP_N uppnås; fyller på med mest färska
- Skriver till producer/sections/{day}/{league}/_/S.NEWS.TOPN/{lang}/section.txt

Stöd:
- Azure Blob via Managed Identity (DefaultAzureCredential)
- Local Mode (STORAGE_MODE=local) -> ./_out
Env:
  LEAGUE=premier_league
  DAY=YYYY-MM-DD (default: idag, Europe/Stockholm)
  FEEDS=guardian_football,bbc_football,sky_sports_premier_league,independent_football
  TOP_N=3
  LANG=en
  SECTION_ID=S.NEWS.TOPN
  STORAGE_MODE=local|"" (local = läs/skriv i ./_out)
  LOCAL_OUT_DIR=_out
  (Azure) AZURE_STORAGE_ACCOUNT, AZURE_CONTAINER
"""
import os, json, sys
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Any

# --- Local Mode helpers ---
USE_LOCAL = os.environ.get("STORAGE_MODE", "").lower() == "local"
LOCAL_ROOT = os.environ.get("LOCAL_OUT_DIR", "_out")

def _ensure_parent(fp: str):
    os.makedirs(os.path.dirname(fp), exist_ok=True)

def _upload_text_local(path: str, text: str):
    fp = os.path.join(LOCAL_ROOT, path)
    _ensure_parent(fp)
    with open(fp, "w", encoding="utf-8") as f:
        f.write(text)
    return path

def _download_json_local(path: str):
    fp = os.path.join(LOCAL_ROOT, path)
    with open(fp, "r", encoding="utf-8") as f:
        return json.load(f)

# Azure imports endast om vi kör mot Blob
if not USE_LOCAL:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient, ContentSettings
else:
    class ContentSettings:
        def __init__(self, *args, **kwargs): ...

TZ = ZoneInfo("Europe/Stockholm")

def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()

def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        print(f"[S.NEWS.TOPN] Missing required env var: {name}", file=sys.stderr)
        sys.exit(2)
    return v

def _blob_container():
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    container = os.environ["AZURE_CONTAINER"]
    url = f"https://{account}.blob.core.windows.net"
    cred = DefaultAzureCredential()
    svc = BlobServiceClient(account_url=url, credential=cred)
    return svc.get_container_client(container)

def _download_json_blob(container_client, path: str):
    stream = container_client.download_blob(path)
    return json.loads(stream.readall().decode("utf-8"))

def _upload_text_blob(container_client, path: str, text: str, content_type: str):
    container_client.upload_blob(
        name=path,
        data=text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )
    return path

def _load_items_for_feed(cc, feed: str, league: str, day: str) -> List[Dict[str, Any]]:
    rel = f"curated/news/{feed}/{league}/{day}/items.json"
    try:
        data = _download_json_local(rel) if USE_LOCAL else _download_json_blob(cc, rel)
    except Exception:
        return []
    # items kan vara list eller {"items":[...]}
    if isinstance(data, dict) and "items" in data:
        data = data["items"]
    if not isinstance(data, list):
        return []
    for it in data:
        it.setdefault("source", feed)
        if "url" not in it and "link" in it:
            it["url"] = it["link"]
    return data

def _parse_dt(it: Dict[str, Any]) -> datetime:
    # published_iso (ISO-8601) → fallback published (best effort)
    ts = it.get("published_iso")
    if ts:
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            pass
    p = it.get("published")
    if p:
        # försök några vanliga RFC-format; om det spricker → MIN
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"):
            try:
                return datetime.strptime(p, fmt)
            except Exception:
                continue
    return datetime.min.replace(tzinfo=timezone.utc)

def _sort_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(items, key=_parse_dt, reverse=True)

def _pick_topn_diverse(items: List[Dict[str, Any]], top_n: int) -> List[Dict[str, Any]]:
    chosen: List[Dict[str, Any]] = []
    seen: set[str] = set()
    # Pass 1: max 1 per källa
    for it in items:
        src = (it.get("source") or it.get("feed") or "unknown").lower()
        if src not in seen:
            chosen.append(it)
            seen.add(src)
            if len(chosen) >= top_n:
                return chosen
    # Pass 2: fyll på med mest färska
    for it in items:
        if it in chosen:
            continue
        chosen.append(it)
        if len(chosen) >= top_n:
            break
    return chosen

def _render_section(league: str, day: str, itemsN: List[Dict[str, Any]]) -> str:
    title = f"Top {len(itemsN)} headlines – {league.replace('_',' ').title()} – {day}"
    lines = [title, ""]
    for i, it in enumerate(itemsN, 1):
        title = (it.get("title") or "").strip()
        url = it.get("url") or it.get("link") or ""
        src = it.get("source") or it.get("feed") or ""
        pub = it.get("published_iso") or it.get("published") or ""
        lines.append(f"{i}. {title} ({src}) — {pub}")
        if url:
            lines.append(f"   {url}")
    lines.append("")
    return "\n".join(lines)

def main():
    league = _env("LEAGUE", "premier_league")
    day = os.environ.get("DAY", today_str())
    feeds_csv = _env("FEEDS", "guardian_football,bbc_football,sky_sports_premier_league,independent_football")
    top_n = int(os.environ.get("TOP_N", "3"))
    lang = os.environ.get("LANG", "en")
    section_id = os.environ.get("SECTION_ID", "S.NEWS.TOPN")

    cc = None if USE_LOCAL else _blob_container()

    # Läs och slå ihop alla feeds
    all_items: List[Dict[str, Any]] = []
    feeds = [f.strip() for f in feeds_csv.split(",") if f.strip()]
    for feed in feeds:
        all_items.extend(_load_items_for_feed(cc, feed, league, day))

    if not all_items:
        print(f"[{section_id}] Inga items för {league} {day} i feeds={feeds}. Exit 3.")
        sys.exit(3)

    items_sorted = _sort_items(all_items)
    picked = _pick_topn_diverse(items_sorted, top_n)
    body = _render_section(league, day, picked)

    out_txt = f"producer/sections/{day}/{league}/_/{section_id}/{lang}/section.txt"
    out_manifest = f"producer/sections/{day}/{league}/_/{section_id}/{lang}/input_manifest.json"

    if USE_LOCAL:
        _upload_text_local(out_txt, body)
        _upload_text_local(out_manifest, json.dumps({
            "league": league, "day": day,
            "feeds": feeds, "count_input": len(all_items),
            "count_output": len(picked),
            "generated_at": datetime.now(timezone.utc).astimezone(TZ).isoformat()
        }, ensure_ascii=False, indent=2))
    else:
        _upload_text_blob(cc, out_txt, body, "text/plain; charset=utf-8")
        _upload_text_blob(cc, out_manifest, json.dumps({
            "league": league, "day": day,
            "feeds": feeds, "count_input": len(all_items),
            "count_output": len(picked),
            "generated_at": datetime.now(timezone.utc).astimezone(TZ).isoformat()
        }, ensure_ascii=False, indent=2), "application/json; charset=utf-8")

    print(f"[{section_id}] OK -> {out_txt} ({len(picked)}/{len(all_items)})")

if __name__ == "__main__":
    main()
