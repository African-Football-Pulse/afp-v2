import os, json, uuid, sys, pathlib, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
import feedparser

# Våra gemensamma helpers
from src.common.blob_io import get_container_client, make_blob_client

TZ = ZoneInfo("Europe/Stockholm")

def now_iso():
    return datetime.now(timezone.utc).astimezone(TZ).isoformat()

def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()

# Offline-mode
USE_LOCAL = os.getenv("USE_LOCAL", "0") == "1"
LOCAL_ROOT = "/app/local_out"

def _ensure_parent(fp: pathlib.Path):
    fp.parent.mkdir(parents=True, exist_ok=True)

def _upload_json_local(path: str, obj):
    fp = pathlib.Path(LOCAL_ROOT) / path
    _ensure_parent(fp)
    fp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(fp)

def upload_json(container_client, path: str, obj):
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    if USE_LOCAL:
        return _upload_json_local(path, obj)
    else:
        # Upload direkt via container_client
        container_client.upload_blob(name=path, data=data, overwrite=True)
        return path

def load_feeds_config():
    import yaml
    with open("config/feeds.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not cfg or "sources" not in cfg:
        raise RuntimeError("config/feeds.yaml saknar 'sources'.")
    return cfg

# ---------------------------------------------------------
# Enrichment: spelarnamn (enkel NER/regex + valfri lexikon)
# ---------------------------------------------------------

# (Valfri) enkel lexikon med afrikanska spelare.
# Om filen saknas använder vi bara regex-kandidater.
LEX_PATH = "config/player_lexicon_africa.txt"

def _load_lexicon(path: str) -> set:
    names = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    names.add(s)
    except FileNotFoundError:
        pass
    return names

LEXICON = _load_lexicon(LEX_PATH)

# Vanliga ord som INTE är namn (för att minska falska träffar)
STOPWORDS = {
    "The","A","An","And","Or","But","If","Of","In","On","At","To","For","With","From","By","As",
    "Man","City","United","FC","CF","SC","AC","BC","Cup","League","Premier","La","Liga","Serie","Bundesliga",
    "Goal","Goals","Assist","Assists","Wins","Win","Draw","Loss","Match","Derby","Coach","Boss",
    "Liverpool","Arsenal","Chelsea","Tottenham","Spurs","Manchester","Newcastle","Everton","Aston","Villa",
    "Real","Barcelona","Bayern","Dortmund","PSG","Marseille","Roma","Inter","Milan",
    "Africa","African","Nigeria","Ghana","Senegal","Egypt","Morocco","Algeria","Tunisia","Ivory","Coast",
}

# Regex: matcha sekvenser av 2–3 kapitaliserade ord (ex: "Mohamed Salah", "Thomas Partey")
NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b")

def _extract_candidates(text: str) -> list[str]:
    if not text:
        return []
    cands = []
    for m in NAME_RE.finditer(text):
        name = m.group(1).strip()
        # filtrera uppenbara icke-namn
        parts = name.split()
        if any(p in STOPWORDS for p in parts):
            continue
        # droppa ensamma vanliga ord
        if len(parts) == 1:
            continue
        cands.append(name)
    return cands

def infer_players_from_text(title: str, summary: str) -> list[str]:
    # 1) regex-kandidater från titel + summary
    cands = _extract_candidates(title) + _extract_candidates(summary)
    if not cands:
        return []
    # 2) om lexikon finns: prioritera träffar som finns i lexikon
    lex_hits = [c for c in cands if c in LEXICON]
    if lex_hits:
        # rensa dubbletter, behåll ordning
        seen = set()
        out = []
        for n in lex_hits:
            if n not in seen:
                seen.add(n)
                out.append(n)
        return out
    # 3) annars: unika regex-namn (best effort)
    seen = set()
    out = []
    for n in cands:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out

# ---------------------------------------------------------

def parse_items(feed, source_name):
    items = []
    for e in feed.entries:
        published_iso = None
        try:
            if getattr(e, "published_parsed", None):
                dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc).astimezone(TZ)
                published_iso = dt.isoformat()
            elif getattr(e, "updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc).astimezone(TZ)
                published_iso = dt.isoformat()
        except Exception:
            published_iso = None

        title = (getattr(e, "title", "") or "").strip()
        summary = (getattr(e, "summary", "") or "")[:1000]

        # ENRICH: extrahera spelare
        players = infer_players_from_text(title, summary)

        items.append({
            "id": getattr(e, "id", None) or str(uuid.uuid4()),
            "title": title,
            "link": getattr(e, "link", None),
            "summary": summary,
            "published": getattr(e, "published", None) or getattr(e, "updated", None),
            "published_iso": published_iso,             # ISO för recency
            "published_at": published_iso,              # alias som S1 vill ha
            "source": source_name,
            "entities": {"players": players},           # <— viktiga enrichment-fältet
            # "club": None  # kan enrichas senare
        })
    return items

def collect_one(source, timeout_s, container_client, league, day, prefix=""):
    name = source["name"]
    url = source["url"]
    print(f"[{name}] START {url}")

    raw_prefix = f"{prefix}raw/news/{name}/{day}"
    curated_prefix = f"{prefix}curated/news/{name}/{league}/{day}"

    try:
        r = requests.get(
            url,
            timeout=timeout_s,
            headers={"User-Agent": "afp-collector/1.0 (+https://primearch.se)"},
        )
        r.raise_for_status()
    except requests.exceptions.Timeout:
        path = upload_json(container_client, f"{raw_prefix}/timeout.json", {
            "source": name, "url": url, "kind": "timeout", "timeout_s": timeout_s, "ts": now_iso()
        })
        print(f"[{name}] TIMEOUT -> {path}")
        return
    except Exception as e:
        path = upload_json(container_client, f"{raw_prefix}/error.json", {
            "source": name, "url": url, "kind": "error", "error": str(e), "ts": now_iso()
        })
        print(f"[{name}] ERROR -> {path}")
        return

    feed = feedparser.parse(r.content)
    raw_obj = {
        "source": name,
        "url": url,
        "fetched_at": now_iso(),
        "feed_title": getattr(feed.feed, "title", None),
        "entry_count": len(feed.entries),
        "raw_note": "Body not stored to keep files small.",
    }
    upload_json(container_client, f"{raw_prefix}/rss.json", raw_obj)

    items = parse_items(feed, name)
    curated_items_path = upload_json(container_client, f"{curated_prefix}/items.json", items)

    manifest = {
        "source": name,
        "league": league,
        "curated_items_path": curated_items_path,
        "count": len(items),
        "generated_at": now_iso(),
        "raw_index": f"{raw_prefix}/rss.json",
        "enrichment": {
            "players": "regex+optional_lexicon",
            "lexicon_used": bool(LEXICON),
            "lexicon_path": LEX_PATH if LEXICON else None
        }
    }
    upload_json(container_client, f"{curated_prefix}/input_manifest.json", manifest)

    print(f"[{name}] OK {len(items)} items -> {curated_items_path}")

def main():
    try:
        cfg = load_feeds_config()
    except Exception as e:
        print(f"[collector] KONFIG-FEL: {e}")
        sys.exit(2)

    league = cfg.get("league", "unknown")
    timeout_s = int(cfg.get("timeout_s", 15))
    sources = cfg["sources"]

    container_client = None if USE_LOCAL else get_container_client()
    prefix = os.getenv("BLOB_PREFIX", "")

    day = today_str()

    print(f"[collector] League={league} | Sources={len(sources)} | Day={day} | Timeout={timeout_s}s | Mode={'LOCAL' if USE_LOCAL else 'SAS'}")

    for src in sources:
        try:
            collect_one(src, timeout_s, container_client, league, day, prefix)
        except Exception as e:
            name = src.get("name", "unknown")
            raw_prefix = f"{prefix}raw/news/{name}/{day}"
            upload_json(container_client, f"{raw_prefix}/error.json", {
                "source": name, "kind": "unexpected_error", "error": str(e), "ts": now_iso()
            })
            print(f"[{name}] UNEXPECTED ERROR: {e}")

    print("[collector] DONE")

if __name__ == "__main__":
    main()
