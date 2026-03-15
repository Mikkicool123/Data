from __future__ import annotations
import os
import subprocess
from typing import Optional


def extract_audio_to_wav(
    input_video_path: str,
    output_wav_path: str,
    sample_rate_hz: int = 16000,
    channels: int = 1,
) -> str:
    os.makedirs(os.path.dirname(output_wav_path) or ".", exist_ok=True)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", input_video_path,
        "-ac", str(channels),
        "-ar", str(sample_rate_hz),
        "-vn",
        "-f", "wav",
        output_wav_path,
    ]
    subprocess.run(cmd, check=True)
    return output_wav_path
