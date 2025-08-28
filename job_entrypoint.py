#!/usr/bin/env python3
import os
import shlex
import subprocess
import sys

# Mappning: JOB_TYPE → Python-modul att köra
JOBS = {
    "collect":  "src.collectors.rss_multi",
    "produce":  "src.produce_section",
    "assemble": "src.assembler.main",
}

def main():
    job = os.getenv("JOB_TYPE", "collect").strip().lower()
    mod = JOBS.get(job)
    if not mod:
        print(f"[ENTRYPOINT] Unknown JOB_TYPE={job!r}. Valid: {', '.join(JOBS)}", file=sys.stderr)
        sys.exit(2)

    # Extra args via env (säkert split med shlex)
    extra = shlex.split(os.getenv("JOB_ARGS", ""))

    cmd = [sys.executable, "-m", mod] + extra
    print(f"[ENTRYPOINT] Running: {' '.join(shlex.quote(c) for c in cmd)}")
    try:
        res = subprocess.run(cmd, check=False)
        sys.exit(res.returncode)
    except KeyboardInterrupt:
        sys.exit(130)

if __name__ == "__main__":
    main()
