# src/storage/azure_blob.py
import os
from datetime import datetime

def _client():
    """
    Creates a BlobServiceClient using the best available credential, in order:
    1) Account Key via AZURE_STORAGE_KEY
    2) SAS via AZURE_BLOB_SAS (should start with '?')
    3) Managed Identity / Workload Identity via DefaultAzureCredential
       (requires RBAC: Storage Blob Data Contributor on the storage account)
    """
    from azure.storage.blob import BlobServiceClient
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    if not account:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT missing")

    url = f"https://{account}.blob.core.windows.net"
    key = os.getenv("AZURE_STORAGE_KEY")
    sas = os.getenv("AZURE_BLOB_SAS")

    # 1) Account Key
    if key:
        return BlobServiceClient(account_url=url, credential=key)

    # 2) SAS token
    if sas:
        # Allow both raw token ("?sv=...") and just the query part ("sv=...")
        sas_str = sas if sas.startswith("?") else f"?{sas}"
        return BlobServiceClient(account_url=url + sas_str)

    # 3) Managed Identity / Workload Identity
    # Requires 'azure-identity' installed and RBAC on the storage account
    from azure.identity import DefaultAzureCredential
    return BlobServiceClient(account_url=url, credential=DefaultAzureCredential())

def put_bytes(container: str, blob_path: str, data: bytes, content_type: str = "application/octet-stream"):
    svc = _client()
    container_client = svc.get_container_client(container)
    try:
        container_client.create_container()
    except Exception:
        pass  # container already exists
    blob = container_client.get_blob_client(blob_path)
    blob.upload_blob(data, overwrite=True, content_type=content_type)
    return f"{container}/{blob_path}"

def put_text(container: str, blob_path: str, text: str, content_type: str = "text/plain; charset=utf-8"):
    return put_bytes(container, blob_path, text.encode("utf-8"), content_type)

def get_text(container: str, blob_path: str) -> str:
    """Convenience helper for later stages (e.g., reading curated inputs)."""
    svc = _client()
    blob = svc.get_blob_client(container, blob_path)
    return blob.download_blob().readall().decode("utf-8")

def exists(container: str, blob_path: str) -> bool:
    svc = _client()
    blob = svc.get_blob_client(container, blob_path)
    return blob.exists()

def list_prefix(container: str, prefix: str):
    svc = _client()
    container_client = svc.get_container_client(container)
    return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]

def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
