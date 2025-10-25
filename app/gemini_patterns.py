from __future__ import annotations
from typing import List, Dict, Any
import google.generativeai as genai
from .config import settings


PROMPT = """
You are an assistant that identifies trading-education patterns in videos.
Given time-ordered events from transcript words, OCR text from frames, and visual labels, return high-confidence segments with:
- pattern: a concise label (e.g., "Golden Cross", "Bearish Divergence", "Breakout", "EMA Strategy")
- rationale: 1-2 sentence justification referencing words/text/labels
- ts_start_s, ts_end_s
Return JSON with an array field "detections" only.
"""


def run_gemini_pattern_extraction(events: Dict[str, Any]) -> Dict[str, Any]:
    if not settings.google_api_key:
        raise RuntimeError("GOOGLE_API_KEY not configured")

    genai.configure(api_key=settings.google_api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    # Compact the events for prompt
    compact = {
        "transcript_words": [
            {"t": round(e["ts_s"], 2), "w": e["word"]} for e in events.get("transcript_events", [])[:5000]
        ],
        "ocr_text": [
            {"t": round(e["ts_s"], 2), "text": e.get("text", ""), "src": e.get("source", "")}
            for e in events.get("ocr_events", [])[:1000]
        ],
        "labels": [
            {"t": round(e["ts_s"], 2), "label": e.get("label", ""), "conf": e.get("confidence")}
            for e in events.get("visual_labels", [])[:1000]
        ],
    }

    prompt = PROMPT + "\n\nEvents (JSON):\n" + str(compact)
    resp = model.generate_content(prompt)
    text = resp.text or "{}"
    # Best-effort JSON extraction
    import json, re
    match = re.search(r"\{[\s\S]*\}$", text.strip())
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            pass
    # Fallback to empty
    return {"detections": []}
