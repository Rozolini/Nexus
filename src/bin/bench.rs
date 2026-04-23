//! Standalone benchmark runner (multi-run statistical harness).
//!
//! Basic usage:
//!   cargo run --release --bin bench                     # default: runs=11, single-process
//!   cargo run --release --bin bench -- --runs 21
//!   cargo run --release --bin bench -- --mode subprocess --runs 11
//!   cargo run --release --bin bench -- --out-dir ./bench-out --csv
//!
//! Each scenario is executed `N` times with a deterministic seed schedule
//! (`derive_seed(base_seed, run_id)`), and per-scenario medians / IQR / min-max
//! are printed. Raw per-run scalars and per-scenario summaries are written to
//! `out_dir/raw_runs.json` and `out_dir/summary.json`. A CSV is optional (`--csv`).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use nexus::{
    aggregate_runs, derive_seed, run_once, run_single_scenario_and_emit_json,
    write_multirun_artifacts, BenchmarkHarnessConfig, IsolationMode, MultiRunConfig,
    MultiRunReport, MultiRunSummary, PlannerConfig, RunResult, SchedulerConfig, WorkloadPattern,
    WorkloadSpec,
};

const DEFAULT_RUNS: usize = 11;
const DEFAULT_BASE_SEED: u64 = 0xBE_EF_C0_DE;

fn fresh_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut p = std::env::temp_dir();
    p.push(format!("nexus_bench_{tag}_{nanos}"));
    std::fs::create_dir_all(&p).expect("create tmp dir");
    p
}

#[derive(Debug, Clone)]
struct Args {
    runs: usize,
    base_seed: u64,
    mode: IsolationMode,
    key_space: u128,
    batches: usize,
    cycles: usize,
    out_dir: Option<PathBuf>,
    emit_csv: bool,

    // Hidden child-run flags (subprocess mode): if set, run ONE scenario + seed,
    // print a single RunResult JSON line on stdout, and exit.
    child_scenario: Option<String>,
    child_seed: Option<u64>,
    child_run_id: Option<u32>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            runs: DEFAULT_RUNS,
            base_seed: DEFAULT_BASE_SEED,
            mode: IsolationMode::SingleProcess,
            key_space: 8_000,
            batches: 96,
            cycles: 24,
            out_dir: None,
            emit_csv: false,
            child_scenario: None,
            child_seed: None,
            child_run_id: None,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(x) = it.next() {
        match x.as_str() {
            "--runs" => a.runs = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.runs),
            "--base-seed" => {
                a.base_seed = it.next().and_then(|s| parse_u64(&s)).unwrap_or(a.base_seed)
            }
            "--mode" => {
                a.mode = match it.next().as_deref() {
                    Some("subprocess") => IsolationMode::Subprocess,
                    _ => IsolationMode::SingleProcess,
                }
            }
            "--key-space" => {
                a.key_space = it
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(a.key_space)
            }
            "--batches" => a.batches = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.batches),
            "--cycles" => a.cycles = it.next().and_then(|s| s.parse().ok()).unwrap_or(a.cycles),
            "--out-dir" => a.out_dir = it.next().map(PathBuf::from),
            "--csv" => a.emit_csv = true,
            "--child-run" => a.child_scenario = it.next(),
            "--child-seed" => a.child_seed = it.next().and_then(|s| parse_u64(&s)),
            "--child-run-id" => a.child_run_id = it.next().and_then(|s| s.parse().ok()),
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
    }
    a
}

fn parse_u64(s: &str) -> Option<u64> {
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(rest, 16).ok()
    } else {
        s.parse::<u64>().ok()
    }
}

fn print_help() {
    println!(
        "nexus bench — multi-run statistical harness.\n\n\
         Flags:\n  \
         --runs N                 number of runs per scenario (default 11)\n  \
         --base-seed N            base seed for the deterministic seed schedule (default 0xBEEFC0DE)\n  \
         --mode MODE              isolation mode: single-process | subprocess (default single-process)\n  \
         --key-space N            keys in workload (default 8000)\n  \
         --batches N              number of query batches per run (default 96)\n  \
         --cycles N               stabilization cycles per run (default 24)\n  \
         --out-dir PATH           directory to write raw_runs.json / summary.json / runs.csv\n  \
         --csv                    also emit out_dir/runs.csv\n"
    );
}

fn harness_template(pattern: WorkloadPattern, args: &Args) -> BenchmarkHarnessConfig {
    BenchmarkHarnessConfig {
        // `seed` is overwritten per-run by the multi-run aggregator.
        seed: 0,
        spec: WorkloadSpec {
            pattern,
            key_space: args.key_space,
            num_query_batches: args.batches,
            query_batch_size: 8,
            cluster_count: 32,
            zipf_s: 1.1,
            read_fraction: 0.7,
            mixed_steps: args.batches * 2,
            write_payload_len: 32,
        },
        load_key_count: args.key_space,
        stabilization_cycles: args.cycles,
        planner: PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 8,
            ..Default::default()
        },
        scheduler: SchedulerConfig {
            max_groups_relocated_per_cycle: 4,
            max_bytes_rewritten_per_cycle: 256 * 1024,
            minimum_improvement_delta: 0.02,
            ..Default::default()
        },
        max_graph_edges: 500_000,
        initial_segment_rotation: (args.key_space / 16).max(64),
        max_relocation_traces: 16,
    }
}

fn pattern_by_name(name: &str) -> Option<WorkloadPattern> {
    Some(match name {
        "Random" => WorkloadPattern::Random,
        "Clustered" => WorkloadPattern::Clustered,
        "SkewedZipfian" => WorkloadPattern::SkewedZipfian,
        "MixedReadWrite" => WorkloadPattern::MixedReadWrite,
        "AdversarialAlternating" => WorkloadPattern::AdversarialAlternating,
        _ => return None,
    })
}

fn all_scenarios() -> Vec<(&'static str, WorkloadPattern)> {
    vec![
        ("Random", WorkloadPattern::Random),
        ("Clustered", WorkloadPattern::Clustered),
        ("SkewedZipfian", WorkloadPattern::SkewedZipfian),
        ("MixedReadWrite", WorkloadPattern::MixedReadWrite),
        (
            "AdversarialAlternating",
            WorkloadPattern::AdversarialAlternating,
        ),
    ]
}

/// Subprocess child branch: run ONE scenario once with the given seed, print
/// a single `RunResult` JSON line to stdout, and exit. Any error goes to stderr
/// and the process exits with code 2.
fn run_child(args: &Args) -> ! {
    let scenario = args
        .child_scenario
        .as_deref()
        .expect("--child-run requires a scenario name");
    let seed = args.child_seed.expect("--child-run requires --child-seed");
    let run_id = args.child_run_id.unwrap_or(0);
    let pattern = pattern_by_name(scenario).unwrap_or_else(|| {
        eprintln!("child: unknown scenario '{scenario}'");
        std::process::exit(2);
    });
    let dir = fresh_dir(scenario);
    let template = harness_template(pattern, args);
    match run_single_scenario_and_emit_json(&dir, scenario, run_id, seed, template) {
        Ok(json) => {
            println!("{json}");
            let _ = std::fs::remove_dir_all(&dir);
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("child: {e}");
            let _ = std::fs::remove_dir_all(&dir);
            std::process::exit(2);
        }
    }
}

fn exec_child_once(
    exe: &Path,
    scenario: &str,
    run_id: u32,
    seed: u64,
    args: &Args,
) -> Result<RunResult, String> {
    let seed_hex = format!("{seed:#x}");
    let output = Command::new(exe)
        .arg("--child-run")
        .arg(scenario)
        .arg("--child-seed")
        .arg(&seed_hex)
        .arg("--child-run-id")
        .arg(run_id.to_string())
        .arg("--key-space")
        .arg(args.key_space.to_string())
        .arg("--batches")
        .arg(args.batches.to_string())
        .arg("--cycles")
        .arg(args.cycles.to_string())
        .output()
        .map_err(|e| format!("spawn child: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "child failed (exit {:?}): {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    // The child prints a single JSON line; take the last non-empty line to be
    // robust against incidental log lines.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let last = stdout
        .lines()
        .rfind(|l| !l.trim().is_empty())
        .ok_or_else(|| "child produced no output".to_string())?;
    serde_json::from_str(last).map_err(|e| format!("parse child json: {e}"))
}

fn run_scenario(args: &Args, scenario: &str, pattern: WorkloadPattern) -> Option<MultiRunReport> {
    let template = harness_template(pattern, args);
    let root = fresh_dir(scenario);

    let mut raw: Vec<RunResult> = Vec::with_capacity(args.runs);
    match args.mode {
        IsolationMode::SingleProcess => {
            for i in 0..args.runs {
                let seed = derive_seed(args.base_seed, i as u32);
                match run_once(&root, scenario, i as u32, seed, template.clone()) {
                    Ok(r) => raw.push(r),
                    Err(e) => {
                        eprintln!("{scenario} run {i}: {e}");
                        let _ = std::fs::remove_dir_all(&root);
                        return None;
                    }
                }
            }
        }
        IsolationMode::Subprocess => {
            let exe = match std::env::current_exe() {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("{scenario}: current_exe failed: {e}");
                    let _ = std::fs::remove_dir_all(&root);
                    return None;
                }
            };
            for i in 0..args.runs {
                let seed = derive_seed(args.base_seed, i as u32);
                match exec_child_once(&exe, scenario, i as u32, seed, args) {
                    Ok(r) => raw.push(r),
                    Err(e) => {
                        eprintln!("{scenario} run {i}: {e}");
                        let _ = std::fs::remove_dir_all(&root);
                        return None;
                    }
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);

    let cfg = MultiRunConfig {
        scenario_name: scenario.to_string(),
        runs: raw.len(),
        base_seed: args.base_seed,
        mode: args.mode,
        data_dir_root: PathBuf::from("."),
        harness_template: template,
    };
    let summary = aggregate_runs(&raw, &cfg);
    Some(MultiRunReport {
        summary,
        raw_runs: raw,
    })
}

fn fmt_ns(x: f64) -> String {
    if x >= 1e9 {
        format!("{:>7.2}s", x / 1e9)
    } else if x >= 1e6 {
        format!("{:>7.2}ms", x / 1e6)
    } else {
        format!("{x:>8.0}ns")
    }
}

fn print_header(args: &Args) {
    println!(
        "Nexus bench — runs={}, base_seed={:#x}, mode={}, key_space={}, batches={}, cycles={}\n",
        args.runs,
        args.base_seed,
        args.mode.as_str(),
        args.key_space,
        args.batches,
        args.cycles
    );
    println!(
        "{:<24}| median p50 b→a             | median p95 b→a           (IQR,min,max) \
         | median range_ops b→a   | median rec/range b→a | median file_opens b→a \
         | med p95_gain [min,max] | +/-/0 | flips | med amp",
        "pattern"
    );
    println!("{}", "-".repeat(260));
}

fn print_summary_row(s: &MultiRunSummary) {
    println!(
        "{:<24}| {} → {} | {} → {} ({:>7.1}%,[{},{}]) \
         | {:>5.2} → {:>5.2} | {:>5.2} → {:>5.2} | {:>5.2} → {:>5.2} \
         | {:>+6.3} [{:>+6.3},{:>+6.3}] | {:>2}/{:>2}/{:>2} | {:>3} | {:>5.3}",
        s.scenario,
        fmt_ns(s.median_p50_baseline_ns),
        fmt_ns(s.median_p50_adapted_ns),
        fmt_ns(s.median_p95_baseline_ns),
        fmt_ns(s.median_p95_adapted_ns),
        if s.median_p95_baseline_ns.abs() < f64::EPSILON {
            0.0
        } else {
            s.p95_adapted_iqr / s.median_p95_baseline_ns * 100.0
        },
        fmt_ns(s.p95_adapted_min),
        fmt_ns(s.p95_adapted_max),
        s.median_range_ops_baseline,
        s.median_range_ops_adapted,
        s.median_rec_per_range_baseline,
        s.median_rec_per_range_adapted,
        s.median_file_opens_baseline,
        s.median_file_opens_adapted,
        s.median_p95_gain,
        s.min_p95_gain,
        s.max_p95_gain,
        s.positive_gain_runs,
        s.negative_gain_runs,
        s.zeroish_gain_runs,
        s.sign_flip_count,
        s.median_rewrite_amplification,
    );
}

fn main() {
    let args = parse_args();
    if args.child_scenario.is_some() {
        run_child(&args);
    }

    print_header(&args);
    let mut reports: Vec<MultiRunReport> = Vec::new();
    for (name, p) in all_scenarios() {
        if let Some(rep) = run_scenario(&args, name, p) {
            print_summary_row(&rep.summary);
            reports.push(rep);
        }
    }

    if let Some(dir) = &args.out_dir {
        match write_multirun_artifacts(dir, &reports, args.emit_csv) {
            Ok(arts) => {
                println!("\nArtifacts written:");
                println!("  raw_runs: {}", arts.raw_runs_json.display());
                println!("  summary : {}", arts.summary_json.display());
                if let Some(csv) = &arts.runs_csv {
                    println!("  csv     : {}", csv.display());
                }
            }
            Err(e) => eprintln!("artifact write failed: {e}"),
        }
    }

    println!("\nLegend:");
    println!(
        "  Each scenario was executed N times with deterministic seed = base_seed + run_id·φ."
    );
    println!("  baseline / adapted still use separate engines and data dirs per run.");
    println!("  p95 IQR / min / max summarise the spread of ADAPTED p95 over the N runs.");
    println!(
        "  +/-/0  = positive / negative / |p95_gain|<{:.2} runs.",
        nexus::ZEROISH_GAIN_THRESHOLD
    );
    println!("  flips = adjacent-run sign flips in the p95_gain sequence (noise indicator).");
    println!("  Use --mode subprocess to isolate runs from shared OS page cache state.");
}
