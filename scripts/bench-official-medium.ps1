# Official "medium" preset - canonical numbers. ~2-5 minutes wall on a warm release cache.
# This is the preset used to generate the numbers quoted in docs/results.md.
# Artifacts land in ./bench-out/official-medium/ .
# NOTE: PowerShell treats cargo stderr chatter as NativeCommandError under
# ErrorActionPreference=Stop, so we check $LASTEXITCODE explicitly instead.
Set-Location (Join-Path $PSScriptRoot '..')

$OutDir   = if ($env:OUT_DIR)   { $env:OUT_DIR }   else { './bench-out/official-medium' }
$BaseSeed = if ($env:BASE_SEED) { $env:BASE_SEED } else { '0xBEEFC0DE' }

Write-Host "Nexus bench - official-medium preset"
Write-Host "  runs=11, base_seed=$BaseSeed, mode=single-process"
Write-Host "  key_space=8000, batches=96, cycles=24"
Write-Host "  out_dir=$OutDir"
Write-Host ""

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
cargo run --release --bin bench -- `
    --runs 11 `
    --base-seed $BaseSeed `
    --mode single-process `
    --key-space 8000 --batches 96 --cycles 24 `
    --out-dir $OutDir `
    --csv
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
