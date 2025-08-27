# src/common/blob_io.py
import os
from azure.storage.blob import ContainerClient, BlobClient
from .secrets import get_secret

def _join_blob_url(container_sas_url: str, blob_name: str) -> str:
    base, qs = container_sas_url.split("?", 1)
    if not base.endswith("/"):
        base += "/"
    return f"{base}{blob_name}?{qs}"

def get_container_client() -> ContainerClient:
    sas_url = get_secret("BLOB_CONTAINER_SAS_URL")
    if "?" not in sas_url or ".blob.core.windows.net/" not in sas_url:
        raise RuntimeError("BLOB_CONTAINER_SAS_URL is invalid; must include /<container>?sp=...")
    return ContainerClient.from_container_url(sas_url)

def make_blob_client(blob_name: str) -> BlobClient:
    sas_url = get_secret("BLOB_CONTAINER_SAS_URL")
    prefix  = os.getenv("BLOB_PREFIX", "")
    return BlobClient.from_blob_url(_join_blob_url(sas_url, f"{prefix}{blob_name}"))
