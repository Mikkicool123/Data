from __future__ import annotations
from typing import Dict, Any, List


def align_events(
    words: List[Dict[str, Any]],
    vi_texts: List[Dict[str, Any]],
    vi_labels: List[Dict[str, Any]],
    vi_objects: List[Dict[str, Any]],
    ocr_events: List[Dict[str, Any]],
    fps: float,
) -> Dict[str, Any]:
    # Map OCR frame_idx to timestamps using fps
    def frame_idx_to_time(idx: int) -> float:
        return idx / fps

    ocr_time_events: List[Dict[str, Any]] = []
    for e in ocr_events:
        start_s = frame_idx_to_time(e["frame_idx"])
        ocr_time_events.append(
            {
                "ts_s": start_s,
                "text": e.get("text", ""),
                "source": e.get("source", "vision_api"),
            }
        )

    transcript_events: List[Dict[str, Any]] = []
    for w in words:
        if w.get("start_s") is None:
            continue
        transcript_events.append(
            {
                "ts_s": w["start_s"],
                "word": w["word"],
                "speaker": w.get("speaker"),
                "confidence": w.get("confidence"),
            }
        )

    vi_text_events: List[Dict[str, Any]] = []
    for t in vi_texts:
        vi_text_events.append(
            {
                "ts_s": t.get("start_s", 0.0),
                "text": t.get("text", ""),
                "confidence": t.get("confidence"),
                "source": "video_intel",
            }
        )

    vi_label_events: List[Dict[str, Any]] = []
    for l in vi_labels:
        vi_label_events.append(
            {
                "ts_s": l.get("start_s", 0.0),
                "label": l.get("label", ""),
                "confidence": l.get("confidence"),
            }
        )

    vi_object_events: List[Dict[str, Any]] = []
    for o in vi_objects:
        vi_object_events.append(
            {
                "ts_s": o.get("start_s", 0.0),
                "entity": o.get("entity", ""),
                "confidence": o.get("confidence"),
            }
        )

    return {
        "transcript_events": transcript_events,
        "ocr_events": ocr_time_events + vi_text_events,
        "visual_labels": vi_label_events,
        "visual_objects": vi_object_events,
    }
