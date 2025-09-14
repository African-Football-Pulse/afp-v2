# src/storage/azure_blob.py
import os
from datetime import datetime

def _client():
    """
    Creates a BlobServiceClient using the best available credential, in order:
    0) Connection String via AZURE_STORAGE_CONNECTION_STRING (supports SAS)
    1) Full container SAS URL via BLOB_CONTAINER_SAS_URL
    2) Account SAS token via AZURE_BLOB_SAS
    3) Account Key via AZURE_STORAGE_KEY
    4) Managed/Workload Identity via DefaultAzureCredential
    """
    from urllib.parse import urlparse
    from azure.storage.blob import BlobServiceClient

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")
    sas_token = os.getenv("AZURE_BLOB_SAS")
    container_sas_url = os.getenv("BLOB_CONTAINER_SAS_URL")

    if container_sas_url:
        u = urlparse(container_sas_url)
        if not account and u.netloc.endswith(".blob.core.windows.net"):
            account = u.netloc.split(".blob.core.windows.net")[0]
        if not account:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT missing and could not be inferred from BLOB_CONTAINER_SAS_URL")
        account_url = f"https://{account}.blob.core.windows.net"
        if u.query:
            return BlobServiceClient(account_url=account_url + "?" + u.query)

    if sas_token:
        sas_str = sas_token if sas_token.startswith("?") else f"?{sas_token}"
        if not account:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT missing (required with AZURE_BLOB_SAS)")
        account_url = f"https://{account}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url + sas_str)

    if key:
        if not account:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT missing (required with AZURE_STORAGE_KEY)")
        account_url = f"https://{account}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=key)

    from azure.identity import DefaultAzureCredential
    if not account:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT missing (no SAS/connection string/key provided)")
    account_url = f"https://{account}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())

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

def get_text(container: str, blob_path: str) -> str:
    svc = _client()
    blob = svc.get_blob_client(container, blob_path)
    return blob.download_blob().readall().decode("utf-8")

def get_json(container: str, blob_path: str):
    import json
    text = get_text(container, blob_path)
    return json.loads(text)

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

def upload_json(container: str, blob_path: str, obj, content_type: str = "application/json; charset=utf-8"):
    import json
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    return put_bytes(container, blob_path, data, content_type)



