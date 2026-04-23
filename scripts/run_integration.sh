#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --test phase1_storage --test phase2_index_reads --test phase3_tracking_graph
