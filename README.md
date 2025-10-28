# 6.5840 MapReduce Labs (2025) — Ethousiast Implementation

This repository contains a Ethousiast's implementation of the MapReduce labs for MIT 6.5840 (Distributed Systems).

- Upstream lab skeleton: `git://g.csail.mit.edu/6.5840-golabs-2025`
- Course site: https://pdos.csail.mit.edu/6.5840/

No affiliation with MIT/CSAIL; this is for educational use only.

## Repo layout
- `main/mr/`: Coordinator/worker/RPC logic
- `main/mrapps/`: MapReduce app plugins (e.g., `wc.go`, `indexer.go`)
- `main/test_files/`: Input texts (public domain Project Gutenberg files)
- `main/test-script/`: End-to-end test harness (`test-mr.sh`)

## Requirements
- Go toolchain with plugin support (Darwin/Linux)
- macOS: `timeout` or `gtimeout` (coreutils) for the test script

## Quick start
```bash
# From repo root
cd main/test-script
bash test-mr.sh        # add "quiet" to suppress output
```
The script computes absolute paths, builds the plugins/binaries, runs the coordinator and workers, and verifies outputs.

## Licensing / attribution
- The lab framework design and original skeleton are attributed to MIT 6.5840; see the upstream repo above.
- Sample texts in `main/test_files/` are from Project Gutenberg and are in the public domain. See headers in each file.
- This repository contains original Ethousiast's code layered on top of the lab skeleton; use at your own risk.
