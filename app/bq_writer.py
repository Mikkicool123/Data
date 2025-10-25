from __future__ import annotations
from typing import Dict, Any, List
from google.cloud import bigquery
from datetime import datetime, timezone


VIDEOS_TABLE = "videos"
TRANSCRIPT_EVENTS_TABLE = "transcript_events"
OCR_EVENTS_TABLE = "ocr_events"
VISUAL_LABELS_TABLE = "visual_labels"
VISUAL_OBJECTS_TABLE = "visual_objects"
DETECTIONS_TABLE = "detections"


def _dataset_ref(project: str, dataset: str) -> str:
    return f"{project}.{dataset}"


def _table_ref(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def ensure_dataset_and_tables(project: str, dataset: str, location: str | None = None) -> None:
    client = bigquery.Client(project=project)
    ds_id = _dataset_ref(project, dataset)
    try:
        client.get_dataset(ds_id)
    except Exception:
        ds = bigquery.Dataset(ds_id)
        if location:
            ds.location = location
        client.create_dataset(ds, exists_ok=True)

    # Define schemas
    videos_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_uri", "STRING"),
        bigquery.SchemaField("local_name", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("duration_s", "FLOAT"),
    ]
    transcript_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ts_s", "FLOAT"),
        bigquery.SchemaField("word", "STRING"),
        bigquery.SchemaField("speaker", "INTEGER"),
        bigquery.SchemaField("confidence", "FLOAT"),
    ]
    ocr_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ts_s", "FLOAT"),
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("source", "STRING"),
    ]
    labels_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ts_s", "FLOAT"),
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("confidence", "FLOAT"),
    ]
    objects_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ts_s", "FLOAT"),
        bigquery.SchemaField("entity", "STRING"),
        bigquery.SchemaField("confidence", "FLOAT"),
    ]
    detections_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ts_start_s", "FLOAT"),
        bigquery.SchemaField("ts_end_s", "FLOAT"),
        bigquery.SchemaField("pattern", "STRING"),
        bigquery.SchemaField("score", "FLOAT"),
        bigquery.SchemaField("model_version", "STRING"),
        bigquery.SchemaField("rationale", "STRING"),
    ]

    # Create tables if not exist
    for table_name, schema in [
        (VIDEOS_TABLE, videos_schema),
        (TRANSCRIPT_EVENTS_TABLE, transcript_schema),
        (OCR_EVENTS_TABLE, ocr_schema),
        (VISUAL_LABELS_TABLE, labels_schema),
        (VISUAL_OBJECTS_TABLE, objects_schema),
        (DETECTIONS_TABLE, detections_schema),
    ]:
        table_id = _table_ref(project, dataset, table_name)
        try:
            client.get_table(table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table, exists_ok=True)


def write_run(
    project: str,
    dataset: str,
    location: str,
    video_id: str,
    local_name: str,
    gcs_video_uri: str,
    stt: Dict[str, Any],
    vi: Dict[str, Any],
    events: Dict[str, Any],
    detections: Dict[str, Any] | None = None,
) -> None:
    client = bigquery.Client(project=project)
    ensure_dataset_and_tables(project, dataset, location)

    now = datetime.now(timezone.utc)

    # Videos row
    videos_rows = [
        {
            "video_id": video_id,
            "gcs_uri": gcs_video_uri,
            "local_name": local_name,
            "created_at": now.isoformat(),
            "duration_s": None,
        }
    ]

    # Transcript rows
    transcript_rows = [
        {
            "video_id": video_id,
            "ts_s": w.get("start_s"),
            "word": w.get("word"),
            "speaker": w.get("speaker"),
            "confidence": w.get("confidence"),
        }
        for w in stt.get("words", [])
        if w.get("start_s") is not None
    ]

    # OCR rows
    ocr_rows = [
        {
            "video_id": video_id,
            "ts_s": e.get("ts_s"),
            "text": e.get("text"),
            "source": e.get("source"),
        }
        for e in events.get("ocr_events", [])
    ]

    # Label rows
    label_rows = [
        {
            "video_id": video_id,
            "ts_s": l.get("ts_s"),
            "label": l.get("label"),
            "confidence": l.get("confidence"),
        }
        for l in events.get("visual_labels", [])
    ]

    # Object rows
    object_rows = [
        {
            "video_id": video_id,
            "ts_s": o.get("ts_s"),
            "entity": o.get("entity"),
            "confidence": o.get("confidence"),
        }
        for o in events.get("visual_objects", [])
    ]

    # Detections
    detections_rows: List[Dict[str, Any]] = []
    if detections and isinstance(detections.get("detections", []), list):
        for d in detections["detections"]:
            detections_rows.append(
                {
                    "video_id": video_id,
                    "ts_start_s": d.get("ts_start_s"),
                    "ts_end_s": d.get("ts_end_s"),
                    "pattern": d.get("pattern"),
                    "score": d.get("score"),
                    "model_version": d.get("model_version"),
                    "rationale": d.get("rationale"),
                }
            )

    # Insert rows
    def insert(table: str, rows: List[Dict[str, Any]]):
        if not rows:
            return
        errors = client.insert_rows_json(_table_ref(project, dataset, table), rows)
        if errors:
            raise RuntimeError(f"BigQuery insertion errors for {table}: {errors}")

    insert(VIDEOS_TABLE, videos_rows)
    insert(TRANSCRIPT_EVENTS_TABLE, transcript_rows)
    insert(OCR_EVENTS_TABLE, ocr_rows)
    insert(VISUAL_LABELS_TABLE, label_rows)
    insert(VISUAL_OBJECTS_TABLE, object_rows)
    insert(DETECTIONS_TABLE, detections_rows)
