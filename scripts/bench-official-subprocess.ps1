# Official "subprocess" preset - same medium workload as bench-official-medium,
# but every individual run executes in a fresh child process to reset user-space
# allocator / FD / read-ahead state between runs. Slower but more trustworthy.
# Expect 10-20 minutes wall on Windows (CreateProcess overhead), ~3-6 minutes on Linux.
# Artifacts land in ./bench-out/official-subprocess/ .
# NOTE: PowerShell treats cargo stderr chatter as NativeCommandError under
# ErrorActionPreference=Stop, so we check $LASTEXITCODE explicitly instead.
Set-Location (Join-Path $PSScriptRoot '..')

$OutDir   = if ($env:OUT_DIR)   { $env:OUT_DIR }   else { './bench-out/official-subprocess' }
$BaseSeed = if ($env:BASE_SEED) { $env:BASE_SEED } else { '0xBEEFC0DE' }

Write-Host "Nexus bench - official-subprocess preset"
Write-Host "  runs=11, base_seed=$BaseSeed, mode=subprocess"
Write-Host "  key_space=8000, batches=96, cycles=24"
Write-Host "  out_dir=$OutDir"
Write-Host "  NOTE: slow - each run spawns a fresh child process."
Write-Host ""

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
cargo run --release --bin bench -- `
    --runs 11 `
    --base-seed $BaseSeed `
    --mode subprocess `
    --key-space 8000 --batches 96 --cycles 24 `
    --out-dir $OutDir `
    --csv
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
