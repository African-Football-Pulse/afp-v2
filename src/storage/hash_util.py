import hashlib
import json

def hash_dict(d: dict) -> str:
    norm = json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(norm).hexdigest()
