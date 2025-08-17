import os
from datetime import datetime

def _client():
    from azure.storage.blob import BlobServiceClient
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")
    sas = os.getenv("AZURE_BLOB_SAS")
    if not account:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT missing")
    if key:
        url = f"https://{account}.blob.core.windows.net"
        return BlobServiceClient(account_url=url, credential=key)
    if sas:
        url = f"https://{account}.blob.core.windows.net{sas}"
        return BlobServiceClient(account_url=url)
    # Managed Identity path (works in ACA if identity has RBAC)
    return BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net")

def put_bytes(container: str, blob_path: str, data: bytes, content_type: str = "application/octet-stream"):
    svc = _client()
    container_client = svc.get_container_client(container)
    try:
        container_client.create_container()
    except Exception:
        pass
    blob = container_client.get_blob_client(blob_path)
    blob.upload_blob(data, overwrite=True, content_type=content_type)
    return f"{container}/{blob_path}"

def put_text(container: str, blob_path: str, text: str, content_type: str = "text/plain; charset=utf-8"):
    return put_bytes(container, blob_path, text.encode("utf-8"), content_type)

def list_prefix(container: str, prefix: str):
    svc = _client()
    container_client = svc.get_container_client(container)
    return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]

def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
