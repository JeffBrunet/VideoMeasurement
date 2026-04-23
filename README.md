# VideoMeasurement

Video-file AprilTag measurement project derived from the NDIMeasurement workspace.

## What it does

- Opens a local video file from `Videos/` or a path passed with `-VideoPath`
- Runs the same AprilTag / board-pose detection pipeline used in the live project
- Shows the annotated preview with overlays
- Writes telemetry events to a Parquet session file by default (MQTT is optional)
- Can publish telemetry over MQTT and board-pose data stream output when enabled
- Can record the annotated output using the existing recorder backends

## Quick start

1. Run `./setup.ps1`
2. Put a `.mov`, `.mp4`, `.m4v`, `.avi`, or `.mkv` file in `Videos/`
3. Run `./run.ps1`

## Useful commands

- `./run.ps1 -List`
- `./run.ps1 -VideoPath '.\Videos\JEff Move.mov'`
- `./run.ps1 -VideoNoRealtime`
- `./run.ps1 -MqttEnable -BoardPoseStreamEnable`
- `./run.ps1 -ParquetDisable -MqttEnable` (MQTT-only telemetry)

## Notes

- The preview keeps the optional OpenGL display path for GPU-backed presentation when available.
- Video ingest requests hardware-accelerated decode hints through OpenCV/FFmpeg when the local build supports them, and falls back safely to standard decode otherwise.
- Telemetry parquet sessions are written under `recordings/telemetry/` by default.
- Legacy copied NDI-oriented setup files were preserved as `legacy_*` files for reference.
