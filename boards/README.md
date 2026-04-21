# Boards Folder Guide

This folder stores ArUco board definition JSON files used by `run.ps1` and `ndi_hx3_gpu_preview_with_apriltag.py`.

## Naming Convention

Use one JSON file per board:
- Preferred: `board_<Name>.json`
- Examples:
  - `board_WorkDesk.json`
  - `board_Lab55in.json`

When you run `./run.ps1 -ListBoards`, friendly board names are derived from the filename:
- `board_WorkDesk.json` -> `WorkDesk`
- `board_Lab55in.json` -> `Lab55in`

## Required JSON Fields

Minimum fields expected by the board loader:
- `dictionary_id`: OpenCV ArUco dictionary ID (for example 20 for AprilTag 36h11)
- `ids`: list of marker IDs in the board
- `obj_points`: list of 4-corner 3D points per marker, in millimeters

Optional size metadata fields:
- `tag_size_mm`: default physical size for tags in this board
- `tag_size_mm_by_id`: per-ID physical size map, for mixed-size tags, for example `{ "31": 120.0, "32": 95.0 }`

Rules:
- `len(ids)` must equal `len(obj_points)`
- each `obj_points` entry must have 4 points
- each point must be `[x, y, z]`
- units should be millimeters to match pipeline telemetry output (`x_mm`, `y_mm`, `z_mm`)

Notes about tag size:
- If `tag_size_mm_by_id` is present, it is used per tag ID for single-tag pose solve.
- Else if `tag_size_mm` is present, it is used for all tags in that board.
- Else the loader infers each tag size from `obj_points` geometry.

## Add a New Board

Board JSON files can be created with the board GUI project at:
- `D:\programmingScratch\arucotagcreation`

Recommended flow:
1. Use the GUI in `D:\programmingScratch\arucotagcreation` to design/export the board JSON.
2. Copy the exported JSON into this folder.
3. Rename it to `board_<Name>.json`.

1. Copy an existing file in this folder, for example `board_WorkDesk.json`.
2. Rename it to `board_<YourName>.json`.
3. Update `dictionary_id`, `ids`, and `obj_points`.
4. Keep coordinates in mm and use a consistent board coordinate frame.
5. Save the file in this folder.

## Validate and Use

List available boards:

```powershell
./run.ps1 -ListBoards
```

Run a single board by name:

```powershell
./run.ps1 -BoardName YourName
```

Run multiple boards by name:

```powershell
./run.ps1 -BoardName "WorkDesk,Lab55in"
```

Run by explicit JSON path(s):

```powershell
./run.ps1 -BoardJson "boards/board_YourName.json"
./run.ps1 -BoardJson "boards/board_WorkDesk.json,boards/board_Lab55in.json"
```

## Selection Behavior

- `-BoardJson` has priority over `-BoardName`.
- With multiple selected boards, each matched board publishes its own `board_pose` telemetry message.
- If no board file is selected/found, the pipeline falls back to tag-aggregate board pose.

## Related Tooling

- Board/tag GUI workspace: `D:\programmingScratch\arucotagcreation`
