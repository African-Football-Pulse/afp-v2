import os
from src.sections import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

# Alla RSS-feeds vi samlar in via rss_multi
FEEDS = [
    "guardian_football",
    "bbc_football",
    "sky_sports_premier_league",
    "independent_football",
    "epl",
    "english_championship",
    "liverpool_features",
    "liverpool_transfer",
    "liverpool_schedule",
    "football_london_all",
    "football_london_arsenal",
    "football_london_chelsea",
    "football_london_tottenham",
    "football_london_westham",
    "soccerstats_english",
    "eyefootball_news",
    "eyefootball_transfers",
]


def load_curated_news(day: str, league: str = "premier_league"):
    """
    Ladda alla nyhets-items från curated/news/<feed>/<league>/<day>/items.json
    Returnerar en sammanslagen lista.
    """
    news_items = []
    for feed in FEEDS:
        items = utils.load_news_items(feed, league, day)
        if items:
            news_items.extend(items)
    return news_items

