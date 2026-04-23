# Official "small" preset - quick smoke-level signal, ~20-40s wall on a warm release cache.
# Intended for CI / quick regression checks, NOT for quoted results.
# Artifacts land in ./bench-out/official-small/ .
# PowerShell 5 and 7 treat cargo's normal stderr chatter as NativeCommandError,
# so we intentionally do NOT set $ErrorActionPreference = 'Stop'. Instead we
# check $LASTEXITCODE after cargo finishes.
Set-Location (Join-Path $PSScriptRoot '..')

$OutDir   = if ($env:OUT_DIR)   { $env:OUT_DIR }   else { './bench-out/official-small' }
$BaseSeed = if ($env:BASE_SEED) { $env:BASE_SEED } else { '0xBEEFC0DE' }

Write-Host "Nexus bench - official-small preset"
Write-Host "  runs=7, base_seed=$BaseSeed, mode=single-process"
Write-Host "  key_space=2000, batches=48, cycles=12"
Write-Host "  out_dir=$OutDir"
Write-Host ""

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
cargo run --release --bin bench -- `
    --runs 7 `
    --base-seed $BaseSeed `
    --mode single-process `
    --key-space 2000 --batches 48 --cycles 12 `
    --out-dir $OutDir `
    --csv
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
