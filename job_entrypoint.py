#!/usr/bin/env python3
import os
import sys
import json
import shlex

def log(msg: str) -> None:
    print(f"[ENTRYPOINT] {msg}", flush=True)

# -------------------------------
# 1) Läs och exportera hemligheter (valfritt)
# -------------------------------
def load_secrets_from_json():
    """
    Om SECRETS_FILE=/app/secrets/secret.json är satt:
    - Läs JSON-objekt { "KEY": "VALUE", ... }
    - Exportera NYCKLAR som env **endast om** de inte redan finns.
    """
    secrets_file = os.getenv("SECRETS_FILE")
    if not secrets_file:
        return

    try:
        with open(secrets_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            log(f"Secrets JSON var inte ett objekt: {secrets_file}")
            return
        exported = []
        for k, v in data.items():
            if k not in os.environ and v is not None:
                os.environ[k] = str(v)
                exported.append(k)
        if exported:
            log(f"Exporterade {len(exported)} nycklar från {secrets_file}: {', '.join(exported)}")
        else:
            log(f"Inga nya env behövde exporteras från {secrets_file}")
    except FileNotFoundError:
        log(f"SECRETS_FILE satt men hittades ej: {secrets_file}")
    except Exception as e:
        log(f"Kunde inte läsa SECRETS_FILE ({secrets_file}): {e}")

# -------------------------------
# 2) Hjälp: exec
# -------------------------------
def exec_cmd(argv):
    """
    Ersätt nuvarande process med kommandot i argv.
    """
    if not argv:
        raise SystemExit("Internal error: tomt argv till exec_cmd.")
    log(f"Running: {' '.join(shlex.quote(a) for a in argv)}")
    os.execvp(argv[0], argv)  # ersätter processen (ingen return)

# -------------------------------
# 3) Välj kommandot baserat på JOB_TYPE/JOB_ARGS
# -------------------------------
def build_command():
    """
    Returnerar argv-listan som ska köras.
    - collect -> python -m src.collectors.rss_multi
    - produce + JOB_ARGS tom -> python -m src.produce_auto
      (automatisk körning enligt planfil)
    - produce + JOB_ARGS satt -> python -m src.produce_section <JOB_ARGS...>
    - annars: om JOB_TYPE pekar på ett modulnamn, kör python -m <värdet>
      (behåller flexibilitet om ni redan använder andra typer)
    """
    job_type = (os.getenv("JOB_TYPE") or "").strip().lower()
    job_args = (os.getenv("JOB_ARGS") or "").strip()

    # Vanliga kvalitetssäkringar för lokalläge om ni nyttjar dem
    # (påverkar inte molnet om de inte används i koden)
    # Normalisera USE_LOCAL
    if "USE_LOCAL" in os.environ:
        v = os.environ["USE_LOCAL"].strip().lower()
        os.environ["USE_LOCAL"] = "1" if v in ("1", "true", "yes", "y") else "0"

    # Standard: om inget job_type satts, anta 'collect'
    if not job_type:
        job_type = "collect"

    # Brancher
    if job_type == "collect":
        return ["python", "-m", "src.collectors.rss_multi"]

    if job_type == "produce":
        if job_args:
            # Pass-through till produce_section med dina flaggor
            return ["python", "-m", "src.produce_section"] + shlex.split(job_args)
        else:
            # Auto-produce (ingen CLI behövs)
            return ["python", "-m", "src.produce_auto"]

    # Fallback: tillåt att JOB_TYPE anger en godtycklig modulväg (t.ex. "src.assemble")
    # så ni kan introducera fler jobb utan att röra entrypointen.
    # Ex: JOB_TYPE=src.assemble  -> python -m src.assemble
    #     JOB_TYPE=my.pkg.task    -> python -m my.pkg.task
    if "." in job_type:
        return ["python", "-m", job_type]

    # Om vi landar här är JOB_TYPE okänt. Visa hjälpsam text.
    log(f"Okänt JOB_TYPE: {job_type}. Stöds: collect, produce eller en full modulväg (ex. 'src.assemble').")
    raise SystemExit(2)

# -------------------------------
# main
# -------------------------------
def main():
    load_secrets_from_json()
    argv = build_command()
    exec_cmd(argv)

if __name__ == "__main__":
    main()
