from __future__ import annotations
from typing import Optional
from google.cloud import storage
import os

_client: Optional[storage.Client] = None

def get_client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client()  # uses ADC
    return _client


def upload_file(local_path: str, bucket_name: str, object_name: str) -> str:
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{object_name}"


def ensure_bucket(bucket_name: str, location: str | None = None) -> None:
    client = get_client()
    try:
        client.get_bucket(bucket_name)
        return
    except Exception:
        pass
    bucket = client.bucket(bucket_name)
    if location:
        client.create_bucket(bucket, location=location)
    else:
        client.create_bucket(bucket)


def gcs_uri_exists(uri: str) -> bool:
    if not uri.startswith("gs://"):
        return False
    _, path = uri.split("gs://", 1)
    bucket_name, object_name = path.split("/", 1)
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.exists()
