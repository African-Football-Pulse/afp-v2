import os
import sys
import json
import logging
from datetime import datetime

# -------------------------------------------------------
# Logger setup (entrypoint)
# -------------------------------------------------------
logger = logging.getLogger("entrypoint")
handler = logging.StreamHandler()
formatter = logging.Formatter("[ENTRYPOINT] %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]
logger.propagate = False
logger.setLevel(logging.INFO)


def log(msg: str):
    """Standardiserad loggning med timestamp"""
    logger.info(msg)


def export_secrets(secrets_file: str = "/app/secrets/secret.json"):
    """Exportera hemligheter från JSON till env"""
    try:
        with open(secrets_file, "r", encoding="utf-8") as f:
            secrets = json.load(f)
        for k, v in secrets.items():
            os.environ[k] = str(v)
        log(f"Exporterade {len(secrets)} nycklar från {secrets_file}: {', '.join(secrets.keys())}")
    except FileNotFoundError:
        log(f"⚠️ Hittade inte secrets-filen: {secrets_file}")
    except Exception as e:
        log(f"⚠️ Fel vid export av secrets: {e}")


def build_command():
    job_type = os.environ.get("JOB_TYPE", "").strip()
    if not job_type:
        log("❌ JOB_TYPE måste anges")
        sys.exit(1)

    # Standard collect → rss_multi
    if job_type == "collect":
        log("Selected job: COLLECT → rss_multi (default)")
        return ["python", "-m", "src.collectors.rss_multi"]

    # Standard produce → auto
    if job_type == "produce":
        log("Selected job: PRODUCE (auto) → full pipeline via produce_auto")
        return ["python", "-m", "src.producer.produce_auto"]

    # Tillåt explicit modulväg, t.ex. src.collectors.collect_extract_weekly
    if job_type.startswith("src."):
        log(f"Selected job: custom module → {job_type}")
        return ["python", "-m", job_type]

    # Okänd typ
    log(f"❌ Okänd JOB_TYPE: {job_type}")
    sys.exit(1)


def debug_env():
    """Logga relevanta environment-variabler (delvis maskerade)"""
    prefixes = ("AFP", "AZURE", "SOCCERDATA", "BUZZSPROUT", "ELEVENLABS", "BLOB", "JOB_TYPE")
    for k, v in os.environ.items():
        if k.startswith(prefixes):
            val = v if len(v) < 12 else v[:6] + "..."
            log(f"ENV {k}={val}")


def main():
    log("Startar job_entrypoint")
    export_secrets(os.environ.get("SECRETS_FILE", "/app/secrets/secret.json"))

    debug_env()  # 👈 Debug-utskrift av relevanta env-vars

    # Lägg till CLI-argumenten efter modulnamnet
    cmd = build_command() + sys.argv[1:]
    log(f"Running: {' '.join(cmd)}")
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()
