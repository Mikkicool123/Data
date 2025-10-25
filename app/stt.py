from __future__ import annotations
from typing import List, Dict, Any
from google.cloud import speech_v1p1beta1 as speech
from .gcs_utils import upload_file
from .config import settings
import os


# Uses async Recognize on GCS URI for long media; returns words with timestamps

def transcribe_long_audio_gcs(
    local_audio_path: str,
    gcs_object_name: str,
    language_code: str = "en-US",
    enable_speaker_diarization: bool = True,
) -> Dict[str, Any]:
    if not settings.gcs_bucket_audio:
        raise RuntimeError("GCS_BUCKET_AUDIO not configured")

    gcs_uri = upload_file(local_audio_path, settings.gcs_bucket_audio, gcs_object_name)

    client = speech.SpeechClient()

    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        language_code=language_code,
        enable_word_time_offsets=True,
        enable_automatic_punctuation=True,
        diarization_speaker_count=2 if enable_speaker_diarization else 0,
        enable_speaker_diarization=enable_speaker_diarization,
        model="latest_long",
    )
    audio = speech.RecognitionAudio(uri=gcs_uri)

    operation = client.long_running_recognize(config=config, audio=audio)
    response = operation.result(timeout=3600)

    words: List[Dict[str, Any]] = []
    transcripts: List[str] = []
    for result in response.results:
        if result.alternatives:
            alt = result.alternatives[0]
            transcripts.append(alt.transcript)
            for w in alt.words:
                words.append(
                    {
                        "word": w.word,
                        "start_s": w.start_time.total_seconds() if w.start_time else None,
                        "end_s": w.end_time.total_seconds() if w.end_time else None,
                        "speaker": getattr(w, "speaker_tag", None),
                        "confidence": getattr(alt, "confidence", None),
                    }
                )

    return {
        "gcs_uri": gcs_uri,
        "transcript": " ".join(transcripts).strip(),
        "words": words,
        "language_code": language_code,
    }
