from __future__ import annotations
from typing import List, Dict, Any
from google.cloud import vision
import os


def ocr_frames(frame_paths: List[str]) -> List[Dict[str, Any]]:
    client = vision.ImageAnnotatorClient()
    events: List[Dict[str, Any]] = []
    for idx, path in enumerate(frame_paths):
        with open(path, "rb") as f:
            content = f.read()
        image = vision.Image(content=content)
        response = client.text_detection(image=image)
        if response.error.message:
            raise RuntimeError(response.error.message)
        if not response.text_annotations:
            continue
        full_text = response.text_annotations[0].description
        events.append({
            "frame_idx": idx,
            "text": full_text,
            "source": "vision_api",
        })
    return events
