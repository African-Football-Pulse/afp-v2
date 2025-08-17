# src/sections/s_news_top3_generic.py
"""
Läser första bästa curated items.json för dagens datum och skriver en Top 3-sektion till:
sections/{YYYY-MM-DD}/{league}/_/S.NEWS.TOP3.GENERIC/en/section.txt

Stöd för:
- Azure Blob via Managed Identity (DefaultAzureCredential)
- Local Mode (STORAGE_MODE=local) -> skriver/läser i ./_out
"""
import os, json, sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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

def download_json_local(path):
    fp = os.path.join(LOCAL_ROOT, path)
    with open(fp, "r", encoding="utf-8") as f:
        return json.load(f)

def list_local_item_paths(prefix):
    base = os.path.join(LOCAL_ROOT, prefix)
    if not os.path.exists(base):
        return []
    hits = []
    for root, _, files in os.walk(base):
        for fn in files:
            if fn == "items.json":
                rel = os.path.relpath(os.path.join(root, fn), LOCAL_ROOT)
                hits.append(rel)
    return hits
# --- /Local Mode ---

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

def blob_client():
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    container = os.environ["AZURE_CONTAINER"]
    url = f"https://{account}.blob.core.windows.net"
    cred = DefaultAzureCredential()
    svc = BlobServiceClient(account_url=url, credential=cred)
    return svc.get_container_client(container)

def list_blobs(container_client, prefix):
    if USE_LOCAL:
        class B: pass
        for rel in list_local_item_paths(prefix):
            b = B(); b.name = rel
            yield b
        return
    return container_client.list_blobs(name_starts_with=prefix)

def download_json(container_client, path):
    if USE_LOCAL:
        return download_json_local(path)
    stream = container_client.download_blob(path)
    return json.loads(stream.readall().decode("utf-8"))

def upload_text(container_client, path, text):
    if USE_LOCAL:
        return _upload_text_local(path, text)
    container_client.upload_blob(
        name=path,
        data=text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain; charset=utf-8")
    )
    return path

def pick_items_blob_for_day(container_client, league, day):
    prefix = f"curated/news/"
    for blob in list_blobs(container_client, prefix):
        name = blob.name
        if not name.endswith("/items.json"):
            continue
        parts = name.split("/")
        # expected: curated/news/{source}/{league}/{day}/items.json
        if len(parts) < 6:
            continue
        if parts[0] != "curated" or parts[1] != "news":
            continue
        b_league = parts[3]
        b_day = parts[4]
        if b_league != league or b_day != day:
            continue
        try:
            items = download_json(container_client, name)
            if isinstance(items, list) and len(items) > 0:
                return name, items
        except Exception:
            continue
    return None, None

def top3(items):
    def key(x):
        ts = x.get("published_iso")
        try:
            return datetime.fromisoformat(ts) if ts else datetime.min.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    items_sorted = sorted(items, key=key, reverse=True)
    return items_sorted[:3]

def render_section(league, day, items3):
    lines = [f"Top 3 headlines – {league.replace('_', ' ').title()} – {day}", ""]
    for i, it in enumerate(items3, 1):
        title = (it.get("title") or "").strip()
        link = it.get("link") or ""
        src = it.get("source") or ""
        lines.append(f"{i}. {title} ({src})")
        if link:
            lines.append(f"   {link}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"

def main():
    league = os.environ.get("LEAGUE", "premier_league")
    day = os.environ.get("DAY", today_str())

    cc = None if USE_LOCAL else blob_client()
    blob_name, items = pick_items_blob_for_day(cc, league, day)

    if not items:
        print(f"[top3] Hittar inga curated items för {league} {day}. Exit 3.")
        sys.exit(3)

    top = top3(items)
    section_text = render_section(league, day, top)

    out_path = f"sections/{day}/{league}/_/S.NEWS.TOP3.GENERIC/en/section.txt"
    upload_text(cc, out_path, section_text)

    manifest = {
        "league": league,
        "day": day,
        "source_items_blob": blob_name,
        "count_input": len(items),
        "count_output": len(top),
        "generated_at": datetime.now(timezone.utc).astimezone(TZ).isoformat()
    }
    manifest_path = f"sections/{day}/{league}/_/S.NEWS.TOP3.GENERIC/en/input_manifest.json"
    if USE_LOCAL:
        _upload_text_local(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))
    else:
        from azure.storage.blob import ContentSettings
        cc.upload_blob(
            name=manifest_path,
            data=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json; charset=utf-8")
        )

    print(f"[top3] OK -> {out_path}")

if __name__ == "__main__":
    main()
