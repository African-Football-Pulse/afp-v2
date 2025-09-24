import os
import yaml
from src.sections import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def log(msg: str):
    """Standardiserad loggning"""
    print(f"[news_utils] {msg}", flush=True)


def load_feeds_config():
    """Ladda feeds från config/feeds.yaml"""
    with open("config/feeds.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("feeds", [])


def load_curated_news(day: str, league: str = "premier_league"):
    """
    Ladda alla nyhets-items från curated/news/<feed>/<league>/<day>/items.json
    Returnerar en sammanslagen lista.
    """
    feeds = load_feeds_config()
    news_items = []
    for feed in feeds:
        items = utils.load_news_items(feed, league, day)
        if items:
            log(f"{feed}: {len(items)} items loaded")
            news_items.extend(items)
        else:
            log(f"{feed}: no items found")
    log(f"TOTAL: {len(news_items)} items loaded from {len(feeds)} feeds")
    return news_items
