#!/usr/bin/env bash
# Official "medium" preset — canonical numbers. ~2-5 minutes wall on a warm release cache.
# This is the preset used to generate the numbers quoted in docs/results.md.
# Artifacts land in ./bench-out/official-medium/ .
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_DIR="${OUT_DIR:-./bench-out/official-medium}"
BASE_SEED="${BASE_SEED:-0xBEEFC0DE}"

echo "Nexus bench — official-medium preset"
echo "  runs=11, base_seed=${BASE_SEED}, mode=single-process"
echo "  key_space=8000, batches=96, cycles=24"
echo "  out_dir=${OUT_DIR}"
echo

mkdir -p "${OUT_DIR}"
cargo run --release --bin bench -- \
    --runs 11 \
    --base-seed "${BASE_SEED}" \
    --mode single-process \
    --key-space 8000 --batches 96 --cycles 24 \
    --out-dir "${OUT_DIR}" \
    --csv
