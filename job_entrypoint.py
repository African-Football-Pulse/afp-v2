#!/usr/bin/env python3
import os
import sys
import json
import shlex

def log(msg: str) -> None:
    print(f"[ENTRYPOINT] {msg}", flush=True)

# -------------------------------
# 1) Läs och exportera hemligheter
# -------------------------------
def load_secrets_from_json():
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
    if not argv:
        raise SystemExit("Internal error: tomt argv till exec_cmd.")
    log(f"Running: {' '.join(shlex.quote(a) for a in argv)}")
    os.execvp(argv[0], argv)  # ersätter processen

# -------------------------------
# 3) Välj kommandot baserat på JOB_TYPE/JOB_ARGS
# -------------------------------
def build_command():
    job_type = (os.getenv("JOB_TYPE") or "").strip().lower()
    job_args = (os.getenv("JOB_ARGS") or "").strip()

    if "USE_LOCAL" in os.environ:
        v = os.environ["USE_LOCAL"].strip().lower()
        os.environ["USE_LOCAL"] = "1" if v in ("1", "true", "yes", "y") else "0"

    if not job_type:
        job_type = "collect"

    if job_type == "collect":
        log("Selected job: COLLECT → rss_multi")
        return ["python", "-m", "src.collectors.rss_multi"]

    if job_type == "produce":
        if job_args:
            log(f"Selected job: PRODUCE (manual) → section with args: {job_args}")
            return ["python", "-m", "src.producer.produce_section"] + shlex.split(job_args)
        else:
            log("Selected job: PRODUCE (auto) → full pipeline via produce_auto")
            return ["python", "-m", "src.producer.produce_auto"]

    if "." in job_type:
        log(f"Selected job: custom module → {job_type}")
        return ["python", "-m", job_type]

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
