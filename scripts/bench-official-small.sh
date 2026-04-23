#!/usr/bin/env bash
# Official "small" preset — quick smoke-level signal, ~20–40s wall on a warm release cache.
# Intended for CI / quick regression checks, NOT for quoted results.
# Artifacts land in ./bench-out/official-small/ .
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_DIR="${OUT_DIR:-./bench-out/official-small}"
BASE_SEED="${BASE_SEED:-0xBEEFC0DE}"

echo "Nexus bench — official-small preset"
echo "  runs=7, base_seed=${BASE_SEED}, mode=single-process"
echo "  key_space=2000, batches=48, cycles=12"
echo "  out_dir=${OUT_DIR}"
echo

mkdir -p "${OUT_DIR}"
cargo run --release --bin bench -- \
    --runs 7 \
    --base-seed "${BASE_SEED}" \
    --mode single-process \
    --key-space 2000 --batches 48 --cycles 12 \
    --out-dir "${OUT_DIR}" \
    --csv
