# VideoMeasurement

Video-file AprilTag measurement project derived from the NDIMeasurement workspace.

## What it does

- Opens a local video file from `Videos/` or a path passed with `-VideoPath`
- Runs AprilTag / board-pose analysis as fast as possible for offline workflows
- Records a clean video by default
- Saves overlay analysis data and renders a second-pass overlay video file
- Writes telemetry events to a Parquet session file by default (MQTT is optional)
- Can publish telemetry over MQTT and board-pose data stream output when enabled
- Supports optional PyAV (FFmpeg) decode backend for higher ingest throughput

## Quick start

1. Run `./setup.ps1`
2. Put a `.mov`, `.mp4`, `.m4v`, `.avi`, or `.mkv` file in `Videos/`
3. Run `./run.ps1`

## Useful commands

- `./run.ps1 -List`
- `./run.ps1 -VideoPath '.\Videos\JEff Move.mov'`
- `./run.ps1 -VideoNoRealtime`
- `./run.ps1 -Display` (show realtime preview; default is headless)
- `./run.ps1 -VideoDecodeBackend pyav`
- `./run.ps1 -MqttEnable -BoardPoseStreamEnable`
- `./run.ps1 -ParquetDisable -MqttEnable` (MQTT-only telemetry)

## Notes

- Realtime preview is optional; default run mode is no-display for maximum ingest throughput.
- Video decode backend `auto` prefers PyAV when installed, then falls back to OpenCV.
- Each run writes all artifacts into one session directory under `recordings/`:
	- `video_raw.mp4` (or `video_raw.avi`), `frames.jsonl`, `manifest.json`
	- `overlay_data.jsonl`, `video_overlay.mp4`, `overlay_manifest.json`
	- `telemetry.parquet`
- Legacy copied NDI-oriented setup files were preserved as `legacy_*` files for reference.
