# src/sections/s_news_top3_guardian.py
import os, json, datetime, pathlib, hashlib
from typing import List, Dict, Optional

# Gemensamma helpers (samma som collect använder)
from src.common.blob_io import get_container_client, make_blob_client

# --- Parametrar via env (med rimliga defaults)
LEAGUE      = os.getenv("LEAGUE", "premier_league")
SECTION_ID  = os.getenv("SECTION_ID", "S.NEWS.TOP3_GUARDIAN")
LANG        = os.getenv("LANG", "en")
FEED_NAME   = os.getenv("FEED_NAME", "guardian_football")
TOP_N       = int(os.getenv("TOP_N", "3"))

# I/O-läge
USE_LOCAL   = os.getenv("USE_LOCAL", "0") == "1"
LOCAL_ROOT  = pathlib.Path("/app/local_out")

# Läs- och skrivprefix
READ_PREFIX  = "" if USE_LOCAL else os.getenv("READ_PREFIX", "collector/")
WRITE_PREFIX = "" if USE_LOCAL else os.getenv("BLOB_PREFIX", os.getenv("WRITE_PREFIX", "producer/"))

def today_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

def curated_items_rel(date: str) -> str:
    # Matchar collector-layouten: curated/news/{source}/{league}/{date}/items.json
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
    """
    Läser text antingen från lokal disk (offline) eller från Blob (online).
    rel_path ska vara relativ path utan WRITE_PREFIX. READ_PREFIX adderas i online-läget.
    """
    if USE_LOCAL:
        return _read_text_local(rel_path)
    blob = make_blob_client(READ_PREFIX + rel_path)
    return blob.download_blob().readall().decode("utf-8")

def write_text(rel_path: str, text: str, content_type: str = "text/plain; charset=utf-8") -> str:
    """
    Skriver text lokalt eller till Blob. I online-läget skrivs under WRITE_PREFIX.
    """
    if USE_LOCAL:
        return _write_text_local(rel_path, text)
    blob = make_blob_client(WRITE_PREFIX + rel_path)
    blob.upload_blob(text.encode("utf-8"), overwrite=True, content_type=content_type)
    return WRITE_PREFIX + rel_path

def find_latest_local_items_rel() -> Optional[str]:
    """
    Om dagens fil saknas i offline-läget: hitta senaste datum som har items.json.
    """
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
    """Accepterar både list och dict({'items': [...]}); returnerar normaliserad lista."""
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
    return "\n".jo
