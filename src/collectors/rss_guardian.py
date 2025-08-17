import os
import requests
import feedparser
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def main():
    print("🚀 RSS Guardian collector started")

    # Hämta miljövariabler
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container_name = os.getenv("AZURE_CONTAINER")
    league = os.getenv("LEAGUE", "unknown_league")
    feed_name = os.getenv("FEED_NAME", "rss_feed")
    feed_url = os.getenv("FEED_URL")
    section_id = os.getenv("SECTION_ID", "S.NEWS.TOP3")
    lang = os.getenv("LANG", "en")
    top_n = int(os.getenv("TOP_N", "3"))

    if not feed_url:
        print("❌ No FEED_URL provided, exiting...")
        return

    try:
        print(f"📡 Fetching RSS feed: {feed_url}")
        # Timeout = 15 sekunder
        response = requests.get(feed_url, timeout=15)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout: RSS fetch from {feed_url} took too long")
        return
    except Exception as e:
        print(f"❌ Failed to fetch RSS feed: {e}")
        return

    # Parsar flödet
    feed = feedparser.parse(response.content)
    items = feed.entries[:top_n]

    if not items:
        print("⚠️ No items found in RSS feed")
        return

    # Förbereda data för blob
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    blob_name = f"raw/{league}/{section_id}/{feed_name}_{timestamp}.json"

    data = []
    for item in items:
        data.append({
            "title": item.get("title"),
            "link": item.get("link"),
            "published": item.get("published", ""),
            "summary": item.get("summary", "")
        })

    # Spara till Azure Blob
    try:
        blob_service = BlobServiceClient(
            f"https://{storage_account}.blob.core.windows.net/",
            credential=None  # Hanterad identitet används
        )
        container = blob_service.get_container_client(container_name)
        container.upload_blob(blob_name, str(data), overwrite=True)
        print(f"✅ RSS data saved to blob: {blob_name}")
    except Exception as e:
        print(f"❌ Failed to upload to blob storage: {e}")
        return

    print("🏁 RSS Guardian collector finished successfully")

if __name__ == "__main__":
    main()
