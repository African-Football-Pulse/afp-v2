import os
from azure.storage.blob import BlobClient

account = "afpstoragepilot"
container = "afp"
blob_name = "stats/2025-2026/228/954472.json"  # byt till något du vet finns
sas_token = os.environ["BLOB_CONTAINER_SAS_URL"]

url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}?{sas_token}"
print("🔗 URL:", url)

blob = BlobClient.from_blob_url(url)
data = blob.download_blob().readall()
print("✅ Blob size:", len(data))
