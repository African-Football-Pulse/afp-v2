# src/common/secrets.py
import os
from pathlib import Path

def get_secret(name: str, default: str | None = None) -> str:
    # 1) Fil-hemlighet
    p = Path("/app/secrets") / name
    if p.exists():
        return p.read_text().strip()
    # 2) Miljövariabel
    v = os.getenv(name)
    if v:
        return v.strip()
    if default is not None:
        return default
    raise RuntimeError(f"Missing secret: {name}. Provide /app/secrets/{name} or env {name}.")
