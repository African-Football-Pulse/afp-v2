import os, sys, json, pathlib, urllib.parse, yaml
from src.storage import azure_blob
from datetime import date

CONFIG_PATH = pathlib.Path("config/pods.yaml")

def main():
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

    # Robust filter
    active_pods = [
        (k, v) for k, v in pods.items()
        if str(v.get("status")).lower() in ("on", "true")
    ]
    if not active_pods:
        raise RuntimeError("❌ No active pod found in pods.yaml")

    pod_name, pod_cfg = active_pods[0]
    publish_cfg = pod_cfg.get("publish", {})
    podcast_id = publish_cfg.get("buzzsprout_podcast_id")
    if not podcast_id:
        raise RuntimeError(f"❌ Pod {pod_name} missing buzzsprout_podcast_id")

    league = pod_cfg["leagues"][0]
    lang = pod_cfg["langs"][0]

    episode_date = os.getenv("EPISODE_DATE", "") or date.today().isoformat()

    container_sas_url = os.getenv("BLOB_CONTAINER_SAS_URL", "")
    if not container_sas_url:
        raise RuntimeError("BLOB_CONTAINER_SAS_URL missing")

    u = urllib.parse.urlparse(container_sas_url)
    base_url = f"{u.scheme}://{u.netloc}{u.path}"
    query = u.query

    # Paths i Azure
    blob_base = f"audio/episodes/{episode_date}/{league}/daily/{lang}"
    render_manifest_path = f"{blob_base}/render_manifest.json"
    mp3_path = f"{blob_base}/final_episode.mp3"

    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
    if not azure_blob.exists(container, render_manifest_path):
        raise RuntimeError(f"Hittar inte render_manifest: {render_manifest_path}")
    manifest = azure_blob.get_json(container, render_manifest_path)

    # Metadata
    title = manifest.get("title") or f"{league.capitalize()} Daily – {episode_date}"
    description = manifest.get("description") or f"Automated recap for {league}."
    language = publish_cfg.get("language") or manifest.get("language") or lang
    explicit = publish_cfg.get("explicit", manifest.get("explicit", False))

    # Bygg SAS-URL till mp3
    audio_url = f"{base_url}/{mp3_path}?{query}"

    req = {
        "title": title,
        "description": description,
        "language": language,
        "explicit": explicit,
        "audio_url": audio_url
    }

    # Skriv till Azure
    blob_path = f"publisher/podcasts/{podcast_id}/episodes/{episode_date}/{league}/{lang}/publish_request.json"
    azure_blob.upload_json(container, blob_path, req)

    print(f"✅ Skapade publish_request.json för {pod_name} → {blob_path}")
    print(json.dumps(req, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
