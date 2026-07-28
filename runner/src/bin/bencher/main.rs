//! YCSB benchmarking utility.

use std::collections::{BTreeSet, HashMap};
use std::thread;
use std::time::Duration;

use color_print::cprintln;

use clap::Parser;

use runner::{ClientProc, RunnerError};

// Hardcoded constants:
const VALID_WORKLOADS: [char; 6] = ['a', 'b', 'c', 'd', 'e', 'f'];
const RESP_TIMEOUT: Duration = Duration::from_secs(60);
const YCSB_TIMEOUT: Duration = Duration::from_secs(600);

mod ycsb;
use ycsb::*;

/// Per-client performance statistics recording.
struct Stats {
    /// Number of client stats merged into this struct.
    merged: usize,
    // Performance statistics:
    total_ms: f64,                          // in millisecs
    lat_samples: HashMap<String, Vec<f64>>, // map from op type -> microsecs
}

impl Stats {
    fn new() -> Self {
        Stats {
            merged: 0,
            total_ms: 0.0,
            lat_samples: HashMap::new(),
        }
    }

    fn record_op(&mut self, op: &str, latency_us: f64) {
        self.lat_samples
            .entry(op.to_string())
            .or_default()
            .push(latency_us);
    }

    fn total_ops(&self) -> usize {
        self.lat_samples.values().map(|s| s.len()).sum()
    }

    fn latency_stats(samples: &[f64]) -> Option<(f64, f64, f64, f64)> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted.len();
        let sum: f64 = sorted.iter().sum();
        let avg = sum / len as f64;
        let min = sorted[0];
        let max = sorted[len - 1];
        let idx = ((len as f64) * 0.99).ceil() as usize;
        let idx = idx.saturating_sub(1).min(len - 1);
        let p99 = sorted[idx];
        Some((avg, min, max, p99))
    }

    fn print(&self, phase: &str) {
        cprintln!(
            "  <cyan>{:6}</>  {:6.0} ms",
            format!("[{}]", phase),
            self.total_ms
        );
        let total_ops = self.total_ops();
        let tput_all = if self.total_ms > 0.0 {
            total_ops as f64 / (self.total_ms / 1000.0)
        } else {
            0.0
        };
        println!("    Throughput:  {:9.2} ops/sec", tput_all);

        let mut ops: Vec<_> = self.lat_samples.keys().cloned().collect();
        ops.sort();
        for (i, op) in ops.iter().enumerate() {
            let samples = &self.lat_samples[op];
            let (avg, min, max, p99) = Self::latency_stats(samples).unwrap_or((0.0, 0.0, 0.0, 0.0));
            if i == 0 {
                print!("    Latency:");
            } else {
                print!("            ");
            }
            println!(
                "    {:6}  ops {:6}  avg {:9.2}  min {:6.0}  max {:6.0}  p99 {:6.0}  us",
                op,
                samples.len(),
                avg,
                min,
                max,
                p99
            );
        }
    }

    /// Merge with another stats struct, taking reasonable arithmetics on the
    /// stats fields.
    fn merge(&mut self, other: Stats) {
        debug_assert_ne!(other.merged, 0);
        if self.merged == 0 {
            *self = other;
        } else {
            // take longer total run time
            self.total_ms = f64::max(self.total_ms, other.total_ms);
            for (op, samples) in other.lat_samples {
                self.lat_samples.entry(op).or_default().extend(samples);
            }
            self.merged += other.merged;
        }
    }
}

/// YCSB benchmarking logic.
fn ycsb_bench(
    args: &Args,
    clients: Vec<ClientProc>,
    load: bool,
    ikeys: BTreeSet<String>,
) -> Result<(Stats, BTreeSet<String>), RunnerError> {
    let mut drivers = vec![];
    for client in clients {
        drivers.push(YcsbDriver::exec(
            args.workload,
            args.num_ops,
            load,
            client,
            ikeys.clone(),
        )?);
    }
    println!("  Launched {} YCSB drivers, now waiting...", drivers.len());

    let mut stats = Stats::new();
    let mut ikeys = BTreeSet::new();
    for driver in drivers {
        if let Some((cli_stats, cli_ikeys)) = driver.wait(YCSB_TIMEOUT)? {
            stats.merge(cli_stats);
            ikeys.extend(cli_ikeys);
        } else {
            return Err(RunnerError::Io("a YCSB driver process failed".into()));
        }
    }

    Ok((stats, ikeys))
}

/// Launcher utility arguments.
#[derive(Parser, Debug)]
struct Args {
    /// Number of concurrent clients.
    #[arg(long, default_value = "1")]
    num_clis: usize,

    /// Number of operations per client to run.
    #[arg(long, default_value = "10000")]
    num_ops: usize,

    /// YCSB workload profile name ('a' to 'f').
    #[arg(long, default_value = "a")]
    workload: char,

    /// Client `just` invocation arguments.
    #[arg(long, num_args(1..))]
    client_just_args: Vec<String>,
}

fn main() -> Result<(), RunnerError> {
    let args = Args::parse();
    cprintln!("<s><yellow>YCSB benchmark configuration:</></> {:#?}", args);
    assert_ne!(args.num_clis, 0);
    assert!(VALID_WORKLOADS.contains(&args.workload));

    // YCSB benchmark load phase
    let (stats_load, ikeys_load) = {
        // run load-phase clients concurrently
        let mut clients_load = vec![];
        for _ in 0..args.num_clis {
            let client =
                ClientProc::new(args.client_just_args.iter().map(|s| s.as_str()).collect())?;
            clients_load.push(client);
        }

        // wait for a few seconds to let cargo finish build check
        thread::sleep(Duration::from_secs(
            (0.3 * args.num_clis as f64).ceil() as u64
        ));
        cprintln!("<s><yellow>Benchmarking [Load] phase...</></>");

        ycsb_bench(&args, clients_load, true, BTreeSet::new())?
    };

    // YCSB benchmark run phase
    let (stats_run, _) = {
        // run run-phase clients concurrently
        let mut clients_run = vec![];
        for _ in 0..args.num_clis {
            let client =
                ClientProc::new(args.client_just_args.iter().map(|s| s.as_str()).collect())?;
            clients_run.push(client);
        }

        // wait for a few seconds to let cargo finish build check
        thread::sleep(Duration::from_secs(
            (0.3 * args.num_clis as f64).ceil() as u64
        ));
        cprintln!("<s><yellow>Benchmarking [Run] phase...</></>");

        ycsb_bench(&args, clients_run, false, ikeys_load)?
    };

    cprintln!(
        "<s><yellow>Benchmarking results:</></>  <cyan>YCSB-{}</>  <magenta>{} clients</>",
        args.workload,
        args.num_clis
    );
    stats_load.print("Load");
    stats_run.print("Run");
    Ok(())
}
