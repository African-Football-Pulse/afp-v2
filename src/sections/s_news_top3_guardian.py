# src/sections/s_news_top3_guardian.py
import os, json, datetime, pathlib, hashlib, sys
from typing import List, Dict, Optional
from src.common.blob_io import get_container_client  # OBS: vi använder inte make_blob_client här

LEAGUE      = os.getenv("LEAGUE", "premier_league")
SECTION_ID  = os.getenv("SECTION_ID", "S.NEWS.TOP3_GUARDIAN")
LANG        = os.getenv("LANG", "en")
FEED_NAME   = os.getenv("FEED_NAME", "guardian_football")
TOP_N       = int(os.getenv("TOP_N", "3"))

USE_LOCAL   = os.getenv("USE_LOCAL", "0") == "1"
LOCAL_ROOT  = pathlib.Path("/app/local_out")

READ_PREFIX  = "" if USE_LOCAL else os.getenv("READ_PREFIX", "collector/")
WRITE_PREFIX = "" if USE_LOCAL else os.getenv("BLOB_PREFIX", os.getenv("WRITE_PREFIX", "producer/"))

# Sätts i main() om online
_CONTAINER = None  # type: ignore

def log(msg: str) -> None:
    print(f"[section] {msg}", flush=True)

def today_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def curated_items_rel(date: str) -> str:
    return f"curated/news/{FEED_NAME}/{LEAGUE}/{date}/items.json"

def _ensure_parent(fp: pathlib.Path) -> None:
    fp.parent.mkdir(parents=True, exist_ok=True)

def _read_text_local(rel_path: str) -> str:
    fp = LOCAL_ROOT / rel_path
    if not fp.exists():
        raise FileNotFoundError(f"Local file not found: {fp}")
    return fp.read_text(encoding="utf-8")

def _write_text_local(rel_path: str, text: str) -> str:
    fp = LOCAL_ROOT / rel_path
    _ensure_parent(fp)
    fp.write_text(text, encoding="utf-8")
    return str(fp)

def read_text(rel_path: str) -> str:
    if USE_LOCAL:
        return _read_text_local(rel_path)
    # ONLINE: läs via container-klienten utan att blanda in BLOB_PREFIX
    blob = _CONTAINER.get_blob_client(blob=READ_PREFIX + rel_path)  # type: ignore[attr-defined]
    return blob.download_blob().readall().decode("utf-8")

def write_text(rel_path: str, text: str, content_type: str = "text/plain; charset=utf-8") -> str:
    if USE_LOCAL:
        return _write_text_local(rel_path, text)
    # ONLINE: skriv explicit med WRITE_PREFIX (ingen auto-prefix)
    blob = _CONTAINER.get_blob_client(blob=WRITE_PREFIX + rel_path)  # type: ignore[attr-defined]
    blob.upload_blob(text.encode("utf-8"), overwrite=True, content_type=content_type)
    return WRITE_PREFIX + rel_path

def find_latest_local_items_rel() -> Optional[str]:
    base = LOCAL_ROOT / "curated" / "news" / FEED_NAME / LEAGUE
    if not base.exists():
        return None
    dates = sorted([p.name for p in base.iterdir() if p.is_dir()], reverse=True)
    for d in dates:
        candidate = base / d / "items.json"
        if candidate.exists():
            return candidate.relative_to(LOCAL_ROOT).as_posix()
    return None

def normalize_items(raw_json) -> List[Dict]:
    if isinstance(raw_json, dict):
        items = raw_json.get("items", [])
    elif isinstance(raw_json, list):
        items = raw_json
    else:
        items = []
    norm: List[Dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or it.get("headline") or it.get("name") or "").strip()
        link = it.get("link") or it.get("url") or ""
        src  = it.get("source") or it.get("site") or "The Guardian"
        published = it.get("published") or it.get("pubDate") or it.get("date") or ""
        norm.append({"title": title, "link": link, "source": src, "published": published})
    return norm

def render_text(items: List[Dict]) -> str:
    lines = ["Here are the top headlines today:"]
    for it in items:
        title = it.get("title", "").strip()
        src   = it.get("source", "")
        link  = it.get("link", "")
        lines.append(f"- {title} ({src}) — {link}")
    if not items:
        lines.append("- (no items available)")
    lines.append("More later.")
    return "\n".join(lines)

def main():
    global _CONTAINER
    mode = "LOCAL" if USE_LOCAL else "SAS"
    log(f"start mode={mode} league={LEAGUE} feed={FEED_NAME} top_n={TOP_N}")

    if not USE_LOCAL:
        _CONTAINER = get_container_client()  # säkerställer att SAS finns

    date = today_utc()
    curated_rel = curated_items_rel(date)
    try:
        log(f"reading curated: {(READ_PREFIX + curated_rel) if not USE_LOCAL else curated_rel}")
        raw_text = read_text(curated_rel)
    except FileNotFoundError:
        if USE_LOCAL:
            latest = find_latest_local_items_rel()
            log(f"today missing, latest local is: {latest}")
            if not latest:
                log("ERROR: no local curated/items.json found")
                sys.exit(2)
            curated_rel = latest
            raw_text = read_text(curated_rel)
        else:
            log("ERROR: curated not found in Blob")
            sys.exit(2)

    inputs_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    raw_json = json.loads(raw_text)
    items = normalize_items(raw_json)
    log(f"normalized items: {len(items)}")
    if TOP_N > 0:
        items = items[:TOP_N]
        log(f"top_n applied -> {len(items)}")

    text = render_text(items)
    base = f"sections/{date}/{LEAGUE}/_/{SECTION_ID}/{LANG}/"

    out_txt = base + "section.txt"
    out_manifest = base + "section_manifest.json"

    write_text(out_txt, text, "text/plain; charset=utf-8")
    write_text(out_manifest,
               json.dumps({
                   "section_id": SECTION_ID,
                   "version": 1,
                   "lang": LANG,
                   "date": date,
                   "league": LEAGUE,
                   "inputs": {"curated_ref": (READ_PREFIX + curated_rel) if not USE_LOCAL else curated_rel},
                   "inputs_hash": inputs_hash,
                   "target_duration_s": 45,
                   "count": len(items),
               }, ensure_ascii=False, indent=2),
               "application/json")

    log(f"wrote: {(WRITE_PREFIX or '[local]/')}{out_txt}")
    log(f"wrote: {(WRITE_PREFIX or '[local]/')}{out_manifest}")
    log("done")

if __name__ == "__main__":
    main()
