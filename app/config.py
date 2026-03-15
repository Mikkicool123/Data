from __future__ import annotations
import os
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

class Settings(BaseModel):
    gcp_project: str = Field(default_factory=lambda: os.getenv("GCP_PROJECT", ""))
    gcp_location: str = Field(default_factory=lambda: os.getenv("GCP_LOCATION", "us-central1"))
    gcs_bucket_raw: str = Field(default_factory=lambda: os.getenv("GCS_BUCKET_RAW", ""))
    gcs_bucket_audio: str = Field(default_factory=lambda: os.getenv("GCS_BUCKET_AUDIO", ""))
    gcs_bucket_features: str = Field(default_factory=lambda: os.getenv("GCS_BUCKET_FEATURES", ""))
    bigquery_dataset: str = Field(default_factory=lambda: os.getenv("BIGQUERY_DATASET", "video_patterns"))
    google_api_key: str = Field(default_factory=lambda: os.getenv("GOOGLE_API_KEY", ""))
    use_gemini: bool = Field(default_factory=lambda: os.getenv("USE_GEMINI", "true").lower() == "true")

    @property
    def has_minimal_gcs(self) -> bool:
        return bool(self.gcs_bucket_raw)

settings = Settings()
