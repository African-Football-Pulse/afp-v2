# src/produce_auto.py
import os, sys, json, yaml, subprocess, tempfile, pathlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.storage.azure_blob import get_text, put_text  # din helper

TZ = ZoneInfo("Europe/Stockholm")

def today_str() -> str:
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[FATAL] Missing env: {name}", flush=True)
        sys.exit(1)
    return v

def blob_read_json(container: str, path: str):
    txt = get_text(container, path)  # kastar på 404/perm
    return json.loads(txt)

def blob_write_json(container: str, path: str, obj):
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    put_text(container, path, payload, content_type="application/json; charset=utf-8")

def items_to_text(items: list) -> str:
    lines = []
    for i in items:
        title = (i.get("title") or "").strip()
        summary = (i.get("summary") or "").strip()
        link = i.get("link") or ""
        parts = [p for p in [title, summary, link] if p]
        if parts:
            lines.append(" — ".join(parts))
    if not lines:
        lines.append("No news items available.")
    return "\n".join(f"- {ln}" for ln in lines)

def run_produce_section(section_code: str, news_path: str, date: str, league: str, personas_path: str) -> dict:
    cmd = [
        "python","-m","src.produce_section",
        "--section-code", section_code,
        "--news", news_path,
        "--date", date,
        "--league", league,
        "--personas-path", personas_path,
        "--dry-run"
    ]
    print(f"[Produce] Exec: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"[ERROR] produce_section failed (rc={res.returncode}): {res.stderr or res.stdout}")
        raise RuntimeError("produce_section failed")

    out = json.loads(res.stdout)
    if not out.get("ok"):
        raise RuntimeError("ok=false in produce_section output")
    return out["manifest"]

def main():
    # Blob-krav (din helper läser KEY/SAS via env)
    require_env("AZURE_STORAGE_ACCOUNT")
    container = require_env("AZURE_CONTAINER")

    # Prefix för läs/skriv (håll dem separata)
    READ_PREFIX  = os.getenv("READ_PREFIX",  "collector/")  # var Collect lägger news
    WRITE_PREFIX = os.getenv("WRITE_PREFIX", "producer/")   # var Produce ska skriva

    # Läs plan
    with open("config/produce_plan.yaml", "r", encoding="utf-8") as f:
        plan = yaml.safe_load(f) or {}

    defaults = plan.get("defaults") or {}
    tasks = plan.get("tasks") or []

    default_league = defaults.get("league", "premier_league")
    default_date = defaults.get("date") or today_str()
    personas_path = defaults.get("personas_path", "config/personas.json")

    def expand_date(d):
        if isinstance(d, str) and "{{today}}" in d:
            return today_str()
        return d or default_date

    for t in tasks:
        section = t["section_code"]
        source = t["source"]
        league = t.get("league") or default_league
        date = expand_date(t.get("date"))

        in_path  = f"{READ_PREFIX}curated/news/{source}/{league}/{date}/items.json"
        out_path = f"{WRITE_PREFIX}sections/{date}/{section}/manifest.json"

        print(f"[Produce] Section={section}  League={league}  Date={date}  Source={source}")
        print(f"[Produce] Reading:  {container}/{in_path}")

        try:
            items = blob_read_json(container, in_path)
        except Exception as e:
            print(f"[WARN] Could not read items ({in_path}): {e}")
            continue

        if not items:
            print(f"[WARN] No items for {section} {league} {date}")
            continue

        # skapa temporär nyhetsfil till CLI:t
        news_txt = items_to_text(items)
        tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix="afp_news_"))
        news_path = str((tmp_dir / "news.txt").resolve())
        pathlib.Path(news_path).write_text(news_txt, encoding="utf-8")

        try:
            manifest = run_produce_section(section, news_path, date, league, personas_path)
        except Exception:
            continue

        print(f"[Produce] Writing: {container}/{out_path}")
        try:
            blob_write_json(container, out_path, manifest)
        except Exception as e:
            print(f"[ERROR] Failed to write manifest: {e}")
            continue

    print("[Produce] DONE")

if __name__ == "__main__":
    main()
