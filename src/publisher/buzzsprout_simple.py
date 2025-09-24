# src/publisher/buzzsprout_simple.py
import os, json, pathlib, sys, datetime, time, requests

INBOX_DIR = pathlib.Path("publisher/inbox")
REQUEST_JSON = INBOX_DIR / "publish_request.json"
REPORT_JSON = INBOX_DIR / "publish_report.json"

API_BASE = "https://www.buzzsprout.com/api"

def read_json(p: pathlib.Path) -> dict:
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

def post_with_retries(url, headers=None, json_body=None, max_tries=3):
    """
    Enkel robust wrapper. Backoff vid 403/429/5xx.
    """
    backoff = 1
    for attempt in range(1, max_tries + 1):
        try:
            r = requests.post(url, headers=headers, json=json_body, timeout=300)
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
        print("❌ ERROR: BUZZSPROUT_API_TOKEN / BUZZSPROUT_PODCAST_ID saknas")
        sys.exit(1)

    req = read_json(REQUEST_JSON)

    # Defaults om publish_request.json är tom
    today = datetime.date.today().isoformat()
    title = req.get("title") or f"AFP Episode – {today}"
    description = req.get("description") or "Automated upload from AFP pipeline."
    language = req.get("language") or "en"
    artwork_url = req.get("artwork_url") or ""
    explicit = bool(req.get("explicit", False))
    published_at = req.get("published_at")  # ISO8601 (valfritt)
    audio_url = req.get("audio_url")

    if not audio_url:
        print("❌ ERROR: publish_request.json måste innehålla 'audio_url'")
        sys.exit(1)

    headers = {
        "Authorization": f"Token token={token}",
        "Accept": "application/json",
        "User-Agent": "AFP-Publisher/1.0 (+github-actions)"
    }

    url = f"{API_BASE}/{podcast_id}/episodes.json"

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

    print(f"📤 Publishing via Buzzsprout URL → {audio_url}")
    r = post_with_retries(url, headers=headers, json_body=payload)

    if r.status_code not in (200, 201):
        print(f"❌ ERROR: Buzzsprout {r.status_code} → {r.text[:800]}")
        sys.exit(1)

    resp = r.json()
    REPORT_JSON.write_text(json.dumps({"status": "ok", "response": resp}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Publicerat! Rapport: {REPORT_JSON}")

if __name__ == "__main__":
    main()
