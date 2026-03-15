from __future__ import annotations
from typing import Optional
from pytube import YouTube
import os


def download_youtube(url: str, output_dir: str, filename_stem: Optional[str] = None) -> str:
    os.makedirs(output_dir, exist_ok=True)
    yt = YouTube(url)
    stream = (
        yt.streams.filter(progressive=True, file_extension="mp4")
        .order_by("resolution")
        .desc()
        .first()
    )
    if not stream:
        # Fallback to audio-only or highest available
        stream = yt.streams.order_by("abr").desc().first() or yt.streams.first()
    filename = (filename_stem or yt.title).replace("/", "-").replace("\\", "-")
    out_path = stream.download(output_path=output_dir, filename=f"{filename}.mp4")
    return out_path
