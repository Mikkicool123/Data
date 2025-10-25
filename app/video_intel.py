from __future__ import annotations
from typing import Dict, Any, List
from google.cloud import videointelligence_v1 as vi


def analyze_video_gcs(gcs_uri: str) -> Dict[str, Any]:
    client = vi.VideoIntelligenceServiceClient()
    features = [
        vi.Feature.LABEL_DETECTION,
        vi.Feature.TEXT_DETECTION,
        vi.Feature.SHOT_CHANGE_DETECTION,
        vi.Feature.OBJECT_TRACKING,
    ]
    request = vi.AnnotateVideoRequest(
        input_uri=gcs_uri,
        features=features,
        video_context=vi.VideoContext(
            label_detection_config=vi.LabelDetectionConfig(model="builtin/latest"),
            text_detection_config=vi.TextDetectionConfig(model="builtin/latest"),
        ),
    )
    operation = client.annotate_video(request=request)
    response = operation.result(timeout=7200)

    result = response.annotation_results[0]

    shots: List[Dict[str, Any]] = []
    for s in result.shot_annotations:
        shots.append(
            {
                "start_s": s.start_time_offset.total_seconds() if s.start_time_offset else 0.0,
                "end_s": s.end_time_offset.total_seconds() if s.end_time_offset else 0.0,
            }
        )

    labels: List[Dict[str, Any]] = []
    for l in result.segment_label_annotations:
        description = l.entity.description
        for seg in l.segments:
            labels.append(
                {
                    "label": description,
                    "confidence": seg.confidence,
                    "start_s": seg.segment.start_time_offset.total_seconds() if seg.segment.start_time_offset else 0.0,
                    "end_s": seg.segment.end_time_offset.total_seconds() if seg.segment.end_time_offset else 0.0,
                }
            )

    texts: List[Dict[str, Any]] = []
    for t in result.text_annotations:
        for seg in t.segments:
            texts.append(
                {
                    "text": t.text,
                    "confidence": seg.confidence,
                    "start_s": seg.segment.start_time_offset.total_seconds() if seg.segment.start_time_offset else 0.0,
                    "end_s": seg.segment.end_time_offset.total_seconds() if seg.segment.end_time_offset else 0.0,
                }
            )

    objects: List[Dict[str, Any]] = []
    for o in result.object_annotations:
        objects.append(
            {
                "entity": o.entity.description,
                "confidence": o.confidence,
                "start_s": o.segment.start_time_offset.total_seconds() if o.segment.start_time_offset else 0.0,
                "end_s": o.segment.end_time_offset.total_seconds() if o.segment.end_time_offset else 0.0,
            }
        )

    return {
        "shots": shots,
        "labels": labels,
        "texts": texts,
        "objects": objects,
    }
