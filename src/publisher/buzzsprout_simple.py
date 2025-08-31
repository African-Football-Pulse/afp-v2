import os, json, pathlib, sys, datetime, requests

INBOX_DIR = pathlib.Path("publisher/inbox")
AUDIO_FILE = INBOX_DIR / "episode.mp3"
REQUEST_JSON = INBOX_DIR / "publish_request.json"

def read_json(p: pathlib.Path) -> dict:
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

def main():
    token = os.getenv("BUZZSPROUT_API_TOKEN", "")
    podcast_id = os.getenv("BUZZSPROUT_PODCAST_ID", "")
    if not token or not podcast_id:
        print("ERROR: BUZZSPROUT_API_TOKEN / BUZZSPROUT_PODCAST_ID saknas")
        sys.exit(1)

    if not AUDIO_FILE.exists():
        print(f"ERROR: Hittar inte ljudfil: {AUDIO_FILE}")
        sys.exit(1)

    req = read_json(REQUEST_JSON)

    # Defaults om publish_request.json saknas
    today = datetime.date.today().isoformat()
    title = req.get("title") or f"AFP Episode – {today}"
    description = req.get("description") or "Automated upload from AFP pipeline."
    language = req.get("language") or "en"
    artwork_url = req.get("artwork_url") or ""
    explicit = bool(req.get("explicit", False))
    published_at = req.get("published_at")  # ISO8601 (valfritt)

    url = f"https://www.buzzsprout.com/api/{podcast_id}/episodes.json"
    headers = {"Authorization": f"Token token={token}"}
    files = {
        "audio_file": ("episode.mp3", open(AUDIO_FILE, "rb"), "audio/mpeg")
    }
    data = {
        "title": title,
        "description": description,
        "explicit": str(explicit).lower(),  # "true"/"false"
        "language": language
    }
    if published_at:
        data["published_at"] = published_at
    if artwork_url:
        data["artwork_url"] = artwork_url

    print(f"Uploading to Buzzsprout: title='{title}' language='{language}'")
    r = requests.post(url, headers=headers, files=files, data=data, timeout=300)
    if r.status_code not in (200, 201):
        print(f"ERROR: Buzzsprout {r.status_code}: {r.text[:800]}")
        sys.exit(1)

    resp = r.json()
    out = INBOX_DIR / "publish_report.json"
    out.write_text(json.dumps({"status":"ok","response":resp}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Publicerat ✅  (rapport: publisher/inbox/publish_report.json)")

if __name__ == "__main__":
    main()
