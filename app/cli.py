from __future__ import annotations
import os
import json
import uuid
import tempfile
from typing import Optional
import click

from .config import settings
from .gcs_utils import ensure_bucket, upload_file
from .audio_utils import extract_audio_to_wav
from .stt import transcribe_long_audio_gcs
from .video_intel import analyze_video_gcs
from .frame_sampler import sample_frames
from .vision_ocr import ocr_frames
from .align import align_events
from .gemini_patterns import run_gemini_pattern_extraction
from .bq_writer import write_run
from .youtube_dl import download_youtube


@click.group()
def cli():
    pass


@cli.command()
@click.option("--video", "video_path", required=False, help="Local path to a video file")
@click.option("--youtube", "youtube_url", required=False, help="YouTube URL to download and analyze")
@click.option("--fps", default=1.0, show_default=True, type=float, help="Frame sampling rate for OCR")
@click.option("--language", default="en-US", show_default=True)
@click.option("--use-gemini/--no-gemini", default=None, help="Override USE_GEMINI")
@click.option("--bq/--no-bq", default=True, show_default=True, help="Write outputs to BigQuery")
def run(video_path: str, youtube_url: Optional[str], fps: float, language: str, use_gemini: Optional[bool], bq: bool):
    if use_gemini is not None:
        os.environ["USE_GEMINI"] = "true" if use_gemini else "false"

    # Ensure buckets exist
    if not settings.gcs_bucket_raw or not settings.gcs_bucket_audio:
        raise click.ClickException("Set GCS_BUCKET_RAW and GCS_BUCKET_AUDIO env vars")
    ensure_bucket(settings.gcs_bucket_raw, settings.gcp_location)
    ensure_bucket(settings.gcs_bucket_audio, settings.gcp_location)

    # Get local video path either from --video or download from YouTube
    if not video_path and not youtube_url:
        raise click.ClickException("Provide --video or --youtube")
    if youtube_url and not video_path:
        tmp_dir = "/workspace/output/downloads"
        os.makedirs(tmp_dir, exist_ok=True)
        video_path = download_youtube(youtube_url, tmp_dir)

    run_id = str(uuid.uuid4())
    base_name = os.path.splitext(os.path.basename(video_path))[0]

    # Upload video to GCS raw
    gcs_video_uri = upload_file(video_path, settings.gcs_bucket_raw, f"videos/{base_name}-{run_id}.mp4")

    # Extract audio
    with tempfile.TemporaryDirectory() as td:
        wav_path = os.path.join(td, "audio.wav")
        extract_audio_to_wav(video_path, wav_path)
        stt = transcribe_long_audio_gcs(wav_path, gcs_object_name=f"audio/{base_name}-{run_id}.wav", language_code=language)

    # Video Intelligence
    vi = analyze_video_gcs(gcs_video_uri)

    # Frame sampling + Vision OCR
    frames_dir = os.path.join("/workspace/output", f"frames-{base_name}-{run_id}")
    frames = sample_frames(video_path, frames_dir, fps=fps)
    ocr = ocr_frames(frames)

    # Align
    events = align_events(stt.get("words", []), vi.get("texts", []), vi.get("labels", []), vi.get("objects", []), ocr, fps=fps)

    detections = {"detections": []}
    if settings.use_gemini:
        try:
            detections = run_gemini_pattern_extraction(events)
        except Exception as e:
            click.echo(f"Gemini step failed: {e}")

    # Save outputs
    out_dir = "/workspace/output"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"{base_name}-{run_id}-stt.json"), "w") as f:
        json.dump(stt, f, indent=2)
    with open(os.path.join(out_dir, f"{base_name}-{run_id}-vi.json"), "w") as f:
        json.dump(vi, f, indent=2)
    with open(os.path.join(out_dir, f"{base_name}-{run_id}-events.json"), "w") as f:
        json.dump(events, f, indent=2)
    with open(os.path.join(out_dir, f"{base_name}-{run_id}-detections.json"), "w") as f:
        json.dump(detections, f, indent=2)

    # Optional BigQuery write
    if bq:
        if not settings.gcp_project or not settings.bigquery_dataset:
            click.echo("Skipping BigQuery: set GCP_PROJECT and BIGQUERY_DATASET")
        else:
            try:
                write_run(
                    project=settings.gcp_project,
                    dataset=settings.bigquery_dataset,
                    location=settings.gcp_location,
                    video_id=run_id,
                    local_name=os.path.basename(video_path),
                    gcs_video_uri=gcs_video_uri,
                    stt=stt,
                    vi=vi,
                    events=events,
                    detections=detections,
                )
                click.echo("Wrote results to BigQuery")
            except Exception as e:
                click.echo(f"BigQuery write failed: {e}")

    click.echo("Pipeline completed. Outputs in /workspace/output")


if __name__ == "__main__":
    cli()
