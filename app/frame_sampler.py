from __future__ import annotations
import subprocess
import os
from typing import List


def sample_frames(video_path: str, output_dir: str, fps: float = 1.0) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    # Use ffmpeg to sample frames. Filenames include timestamp via frame count.
    # We rely on sequential numbering and derive timestamps externally if needed.
    pattern = os.path.join(output_dir, "frame_%06d.jpg")
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", video_path,
        "-vf", f"fps={fps}",
        "-q:v", "2",
        pattern,
    ]
    subprocess.run(cmd, check=True)
    files = sorted([os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith("frame_")])
    return files
