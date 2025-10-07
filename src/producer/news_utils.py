import os
import yaml
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def log(msg: str):
    """Standardiserad loggning"""
    print(f"[news_utils] {msg}", flush=True)

def load_feeds_config():
    """Ladda feeds från config/feeds.yaml (stöd för både feeds: och sources:)"""
    with open("config/feeds.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    feeds = []
    if "feeds" in cfg:
        feeds = cfg.get("feeds", [])
    elif "sources" in cfg:
        feeds = [s["name"] for s in cfg.get("sources", []) if s.get("active", True)]

    log(f"Loaded {len(feeds)} feeds from config: {feeds[:5]}{'...' if len(feeds) > 5 else ''}")
    return feeds

def load_curated_news(day: str, league: str = "premier_league"):
    """
    Ladda alla nyhets-items från collector/curated/news/<feed>/<league>/<day>/items.json i Azure.
    Returnerar en sammanslagen lista.
    """
    feeds = load_feeds_config()
    news_items = []
    for feed in feeds:
        blob_path = f"collector/curated/news/{feed}/{league}/{day}/items.json"  # 🔧 fixad sökväg
        if azure_blob.exists(CONTAINER, blob_path):
            try:
                items = azure_blob.get_json(CONTAINER, blob_path)
                if isinstance(items, list):
                    log(f"{feed}: {len(items)} items loaded")
                    news_items.extend(items)
                else:
                    log(f"{feed}: invalid format (expected list in items.json)")
            except Exception as e:
                log(f"{feed}: error loading {blob_path} → {e}")
        else:
            log(f"{feed}: no items found at {blob_path}")

    log(f"TOTAL: {len(news_items)} items loaded from {len(feeds)} feeds")
    return news_items
