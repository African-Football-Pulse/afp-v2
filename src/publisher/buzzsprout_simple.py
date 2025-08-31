import os, json, pathlib, sys, datetime, time, requests

INBOX_DIR = pathlib.Path("publisher/inbox")
AUDIO_FILE = INBOX_DIR / "episode.mp3"
REQUEST_JSON = INBOX_DIR / "publish_request.json"

API_BASE = "https://www.buzzsprout.com/api"

def read_json(p: pathlib.Path) -> dict:
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

def post_with_retries(url, headers=None, files=None, data=None, json_body=None, max_tries=3):
    """
    Liten robust wrapper pga Cloudflare. Väntar 1s, 2s, 4s vid 403/429/5xx.
    """
    backoff = 1
    for attempt in range(1, max_tries + 1):
        try:
            if json_body is not None:
                r = requests.post(url, headers=headers, json=json_body, timeout=300)
            else:
                r = requests.post(url, headers=headers, files=files, data=data, timeout=300)
        except requests.RequestException as e:
            if attempt == max_tries:
                raise
            time.sleep(backoff); backoff *= 2
            continue

        if r.status_code in (200, 201):
            return r
        if r.status_code in (403, 429) or 500 <= r.status_code < 600:
            if attempt == max_tries:
                return r
            time.sleep(backoff); backoff *= 2
            continue
        return r
    return r  # sista svaret

def main():
    token = os.getenv("BUZZSPROUT_API_TOKEN", "")
    podcast_id = os.getenv("BUZZSPROUT_PODCAST_ID", "")
    if not token or not podcast_id:
        print("ERROR: BUZZSPROUT_API_TOKEN / BUZZSPROUT_PODCAST_ID saknas")
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
    audio_url = req.get("audio_url")  # Om satt → använd import-by-URL

    headers = {
        "Authorization": f"Token token={token}",
        "Accept": "application/json",
        "User-Agent": "AFP-Publisher/1.0 (+github-actions)"
    }

    url = f"{API_BASE}/{podcast_id}/episodes.json"

    if audio_url:
        # Option A: be Buzzsprout hämta filen från URL
        print(f"Publishing by URL import → {audio_url}")
        payload = {
            "title": title,
            "description": description,
            "explicit": str(explicit).lower(),
            "language": language,
            "audio_url": audio_url
        }
        if published_at:
            payload["published_at"] = published_at
        if artwork_url:
            payload["artwork_url"] = artwork_url

        r = post_with_retries(url, headers=headers, json_body=payload)
        if r.status_code not in (200, 201):
            print(f"ERROR: Buzzsprout {r.status_code} (URL import): {r.text[:800]}")
            sys.exit(1)
    else:
        # Option B: multipart upload från repo
        if not AUDIO_FILE.exists():
            print(f"ERROR: Hittar inte ljudfil: {AUDIO_FILE} (eller ange audio_url i publish_request.json)")
            sys.exit(1)

        print(f"Uploading multipart → {AUDIO_FILE}")
        files = {
            "audio_file": ("episode.mp3", open(AUDIO_FILE, "rb"), "audio/mpeg")
        }
        data = {
            "title": title,
            "description": description,
            "explicit": str(explicit).lower(),
            "language": language
        }
        if published_at:
            data["published_at"] = published_at
        if artwork_url:
            data["artwork_url"] = artwork_url

        r = post_with_retries(url, headers=headers, files=files, data=data)
        if r.status_code not in (200, 201):
            print(f"ERROR: Buzzsprout {r.status_code} (multipart): {r.text[:800]}")
            sys.exit(1)

    resp = r.json()
    out = INBOX_DIR / "publish_report.json"
    out.write_text(json.dumps({"status": "ok", "response": resp}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Publicerat ✅  (rapport: publisher/inbox/publish_report.json)")

if __name__ == "__main__":
    main()
