import os, sys, json, pathlib, datetime, time, requests, yaml
from src.storage import azure_blob

CONFIG_PATH = pathlib.Path("config/pods.yaml")
API_BASE = "https://www.buzzsprout.com/api"

def read_config():
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"Missing config file: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    pods = cfg.get("pods", {})

    print("🔍 Pods i config/pods.yaml:")
    for name, pod in pods.items():
        status_val = str(pod.get("status")).lower()
        pod_id = pod.get("publish", {}).get("buzzsprout_podcast_id")
        print(f"  - {name}: status={status_val} → id={pod_id}")

    active = [
        (k, v) for k, v in pods.items()
        if str(v.get("status")).lower() in ("on", "true")
    ]
    if not active:
        raise RuntimeError("❌ No active pod found in pods.yaml")
    return active[0]

def post_with_retries(url, headers=None, json_body=None, max_tries=3):
    backoff = 1
    for attempt in range(1, max_tries+1):
        try:
            r = requests.post(url, headers=headers, json=json_body, timeout=300)
        except requests.RequestException:
            if attempt == max_tries: raise
            time.sleep(backoff); backoff *= 2
            continue
        if r.status_code in (200,201): return r
        if r.status_code in (403,429) or 500 <= r.status_code < 600:
            if attempt == max_tries: return r
            time.sleep(backoff); backoff *= 2
            continue
        return r
    return r

def main():
    token = os.getenv("BUZZSPROUT_API_TOKEN", "")
    if not token:
        print("❌ ERROR: BUZZSPROUT_API_TOKEN missing")
        sys.exit(1)

    pod_name, pod_cfg = read_config()
    publish_cfg = pod_cfg.get("publish", {})
    podcast_id = publish_cfg["buzzsprout_podcast_id"]

    from datetime import date
    episode_date = os.getenv("EPISODE_DATE", "") or date.today().isoformat()
    league = pod_cfg["leagues"][0]
    lang = pod_cfg["langs"][0]

    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = f"publisher/podcasts/{podcast_id}/episodes/{episode_date}/{league}/{lang}/publish_request.json"
    if not azure_blob.exists(container, blob_path):
        print(f"❌ ERROR: Missing {blob_path} in Azure")
        sys.exit(1)

    req = azure_blob.get_json(container, blob_path)

    headers = {
        "Authorization": f"Token token={token}",
        "Accept": "application/json",
        "User-Agent": "AFP-Publisher/1.0 (+github-actions)"
    }

    url = f"{API_BASE}/{podcast_id}/episodes.json"
    payload = {
        "title": req["title"],
        "description": req["description"],
        "explicit": str(req.get("explicit", False)).lower(),
        "language": req.get("language", "en"),
        "audio_url": req["audio_url"]
    }

    print(f"📤 Publishing {pod_name} → {payload['title']}")
    r = post_with_retries(url, headers=headers, json_body=payload)
    if r.status_code not in (200,201):
        print(f"❌ ERROR: Buzzsprout {r.status_code} → {r.text[:800]}")
        sys.exit(1)

    resp = r.json()
    report_path = f"publisher/podcasts/{podcast_id}/episodes/{episode_date}/{league}/{lang}/publish_report.json"
    azure_blob.upload_json(container, report_path, {"status":"ok","response":resp})

    print(f"✅ Publicerat! Rapport: {report_path}")

if __name__ == "__main__":
    main()
