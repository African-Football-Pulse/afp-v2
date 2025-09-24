import os, sys, json, pathlib, urllib.parse
from src.storage import azure_blob

INBOX_DIR = pathlib.Path("publisher/inbox")

def main():
    if len(sys.argv) < 4:
        print("Usage: python -m src.publisher.build_publish_request <date> <league> <lang>")
        sys.exit(1)

    episode_date, league, lang = sys.argv[1:4]

    container_sas_url = os.getenv("BLOB_CONTAINER_SAS_URL", "")
    if not container_sas_url:
        raise RuntimeError("BLOB_CONTAINER_SAS_URL missing")

    # Dela container-SAS i bas-URL + querystring
    u = urllib.parse.urlparse(container_sas_url)
    base_url = f"{u.scheme}://{u.netloc}{u.path}"
    query = u.query

    # Paths i Azure
    blob_base = f"audio/episodes/{episode_date}/{league}/daily/{lang}"
    render_manifest_path = f"{blob_base}/render_manifest.json"
    mp3_path = f"{blob_base}/final_episode.mp3"

    # Läs render_manifest
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    if not azure_blob.exists(container, render_manifest_path):
        raise RuntimeError(f"Hittar inte render_manifest: {render_manifest_path}")
    manifest = azure_blob.get_json(container, render_manifest_path)

    # Metadata från render_manifest
    title = manifest.get("title") or f"{league.capitalize()} Daily – {episode_date}"
    description = manifest.get("description") or f"Automated recap for {league}."
    language = manifest.get("language") or lang
    explicit = bool(manifest.get("explicit", False))

    # Bygg SAS-URL till mp3
    audio_url = f"{base_url}/{mp3_path}?{query}"

    req = {
        "title": title,
        "description": description,
        "language": language,
        "explicit": explicit,
        "audio_url": audio_url
    }

    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    out_path = INBOX_DIR / "publish_request.json"
    out_path.write_text(json.dumps(req, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"✅ Skapade publish_request.json → {out_path}")
    print(json.dumps(req, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
