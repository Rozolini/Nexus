#!/usr/bin/env bash
# Official "subprocess" preset — the same medium workload as bench-official-medium,
# but every individual run executes in a fresh child process to reset user-space
# allocator / FD / read-ahead state between runs. Slower but more trustworthy.
# Expect 10-20 minutes wall on Windows (CreateProcess overhead), ~3-6 minutes on Linux.
# Artifacts land in ./bench-out/official-subprocess/ .
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_DIR="${OUT_DIR:-./bench-out/official-subprocess}"
BASE_SEED="${BASE_SEED:-0xBEEFC0DE}"

echo "Nexus bench — official-subprocess preset"
echo "  runs=11, base_seed=${BASE_SEED}, mode=subprocess"
echo "  key_space=8000, batches=96, cycles=24"
echo "  out_dir=${OUT_DIR}"
echo "  NOTE: slow — each run spawns a fresh child process."
echo

mkdir -p "${OUT_DIR}"
cargo run --release --bin bench -- \
    --runs 11 \
    --base-seed "${BASE_SEED}" \
    --mode subprocess \
    --key-space 8000 --batches 96 --cycles 24 \
    --out-dir "${OUT_DIR}" \
    --csv
