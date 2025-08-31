import os, json, pathlib, sys, datetime, requests

def read_json(p: pathlib.Path) -> dict:
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

def write_json(p: pathlib.Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    date = os.getenv("DATE") or datetime.date.today().isoformat()
    league = os.getenv("LEAGUE", "epl")
    lang = os.getenv("LANG", "en")
    title_override = os.getenv("TITLE_OVERRIDE", "")

    token = os.getenv("BUZZSPROUT_API_TOKEN", "")
    podcast_id = os.getenv("BUZZSPROUT_PODCAST_ID", "")
    if not token or not podcast_id:
        print("ERROR: BUZZSPROUT_API_TOKEN / BUZZSPROUT_PODCAST_ID saknas")
        sys.exit(1)

    base_audio = pathlib.Path(f"audio/episodes/{date}/{league}/daily/{lang}")
    base_pub = pathlib.Path(f"publisher/episodes/{date}/{league}/daily/{lang}")
    audio_path = base_audio / "episode.mp3"
    render_manifest = base_audio / "render_manifest.json"
    publish_request_path = base_pub / "publish_request.json"
    publish_report_path = base_pub / "publish_report.json"

    if not audio_path.exists():
        print(f"ERROR: Saknar audio: {audio_path}")
        sys.exit(1)

    rm = read_json(render_manifest)
    pr = read_json(publish_request_path)

    title = title_override or pr.get("title") or rm.get("title") or f"{league.upper()} daily – {date} ({lang})"
    description = pr.get("description") or "Automated episode."
    artwork_url = pr.get("artwork_url") or ""
    language = pr.get("language") or lang
    pub_dt = pr.get("published_at")  # ISO8601, t.ex. "2025-08-31T05:15:00Z"
    explicit = pr.get("explicit", False)

    # Buzzsprout upload
    # POST https://www.buzzsprout.com/api/{podcast_id}/episodes.json
    # Headers: Authorization: Token token=<token>
    url = f"https://www.buzzsprout.com/api/{podcast_id}/episodes.json"
    headers = {"Authorization": f"Token token={token}"}

    files = {
        "audio_file": ("episode.mp3", open(audio_path, "rb"), "audio/mpeg")
    }
    data = {
        "title": title,
        "description": description,
        "explicit": str(explicit).lower(),  # "true"/"false"
        "language": language
    }
    if pub_dt:
        data["published_at"] = pub_dt
    if artwork_url:
        data["artwork_url"] = artwork_url

    print("Publish: laddar upp till Buzzsprout…")
    r = requests.post(url, headers=headers, files=files, data=data, timeout=180)
    if r.status_code not in (200, 201):
        print(f"ERROR: Buzzsprout {r.status_code}: {r.text[:600]}")
        write_json(publish_report_path, {
            "status": "error", "http_status": r.status_code, "response": r.text[:2000]
        })
        sys.exit(1)

    resp = r.json()
    write_json(publish_report_path, {"status": "ok", "response": resp})
    print("Klart: publicerat till Buzzsprout (RSS → Spotify m.fl.).")

if __name__ == "__main__":
    main()
