# src/collectors/rss_multi.py
import os, json, uuid, sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from azure.storage.blob import ContainerClient, BlobClient


import requests
import feedparser

# --- Local Mode helpers (no Azure needed) ---
USE_LOCAL = os.environ.get("STORAGE_MODE", "").lower() == "local"
LOCAL_ROOT = os.environ.get("LOCAL_OUT_DIR", "_out")

def _ensure_parent(fp: str):
    os.makedirs(os.path.dirname(fp), exist_ok=True)

def _upload_json_local(path: str, obj):
    fp = os.path.join(LOCAL_ROOT, path)
    _ensure_parent(fp)
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    return path
# --- /Local Mode ---

# Azure imports endast om vi kör mot Blob
if not USE_LOCAL:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient, ContentSettings
else:
    class ContentSettings:  # dummy så koden kompilerar i local mode
        def __init__(self, *args, **kwargs): ...

TZ = ZoneInfo("Europe/Stockholm")

def now_iso():
    return datetime.now(timezone.utc).astimezone(TZ).isoformat()

def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()

def _join_blob_url(container_sas_url: str, blob_name: str) -> str:
    # Säker join: lägger blob-namnet före frågetecknet (SAS-parametrarna)
    base, qs = container_sas_url.split("?", 1)
    if not base.endswith("/"):
        base += "/"
    return f"{base}{blob_name}?{qs}"

def get_blob_clients():
    """
    Prioritet:
    1) BLOB_CONTAINER_SAS_URL (+ BLOB_PREFIX)
    2) AZURE_STORAGE_ACCOUNT + (AZURE_CONTAINER|AZURE_STORAGE_CONTAINER) + (AZURE_SAS|AZURE_STORAGE_SAS)
    """
    sas_url = os.getenv("BLOB_CONTAINER_SAS_URL")
    prefix = os.getenv("BLOB_PREFIX", "")

    if sas_url:
        container = ContainerClient.from_container_url(sas_url)

        def make_blob_client(name: str) -> BlobClient:
            return BlobClient.from_blob_url(_join_blob_url(sas_url, prefix + name))

        return container, make_blob_client

    # Fallback: gamla env-namn
    account   = os.environ["AZURE_STORAGE_ACCOUNT"]
    container = os.getenv("AZURE_CONTAINER") or os.getenv("AZURE_STORAGE_CONTAINER")
    if not container:
        raise RuntimeError("Saknar AZURE_CONTAINER eller AZURE_STORAGE_CONTAINER")
    sas = os.getenv("AZURE_SAS") or os.getenv("AZURE_STORAGE_SAS")
    if not sas:
        raise RuntimeError("Saknar AZURE_SAS eller AZURE_STORAGE_SAS (endast token-delen, utan '?')")

    account_url = f"https://{account}.blob.core.windows.net"
    cont_client = ContainerClient(account_url=account_url, container_name=container, credential=sas)

    def make_blob_client(name: str) -> BlobClient:
        return BlobClient(account_url, container, prefix + name, credential=sas)

    return cont_client, make_blob_client

def upload_json(container_client, path, obj):
    if USE_LOCAL:
        return _upload_json_local(path, obj)
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    container_client.upload_blob(
        name=path,
        data=data,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json; charset=utf-8"),
    )
    return path

def load_feeds_config():
    import yaml
    with open("config/feeds.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not cfg or "sources" not in cfg:
        raise RuntimeError("config/feeds.yaml saknar 'sources'.")
    return cfg

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
        items.append({
            "id": getattr(e, "id", None) or str(uuid.uuid4()),
            "title": (getattr(e, "title", "") or "").strip(),
            "link": getattr(e, "link", None),
            "summary": (getattr(e, "summary", "") or "")[:1000],
            "published": getattr(e, "published", None) or getattr(e, "updated", None),
            "published_iso": published_iso,
            "source": source_name,
        })
    return items

def collect_one(source, timeout_s, container_client, league, day):
    name = source["name"]
    url = source["url"]
    print(f"[{name}] START {url}")

    raw_prefix = f"raw/news/{name}/{day}"
    curated_prefix = f"curated/news/{name}/{league}/{day}"

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

    container_client = None if USE_LOCAL else get_blob_clients()
    day = today_str()

    print(f"[collector] League={league} | Sources={len(sources)} | Day={day} | Timeout={timeout_s}s | Mode={'LOCAL' if USE_LOCAL else 'AZURE'}")

    for src in sources:
        try:
            collect_one(src, timeout_s, container_client, league, day)
        except Exception as e:
            name = src.get("name", "unknown")
            prefix = f"raw/news/{name}/{day}"
            upload_json(container_client, f"{prefix}/error.json", {
                "source": name, "kind": "unexpected_error", "error": str(e), "ts": now_iso()
            })
            print(f"[{name}] UNEXPECTED ERROR: {e}")

    print("[collector] DONE")

if __name__ == "__main__":
    main()
