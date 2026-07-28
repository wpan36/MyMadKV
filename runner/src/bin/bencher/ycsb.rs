//! YCSB benchmark basic mode driver.
//!
//! Our translation of YCSB operations to our KV operations do not strictly
//! follow the original YCSB semantics, but are good enough for benchmarking.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::str::SplitWhitespace;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use runner::{ClientProc, KvCall, RunnerError};

use crate::{Stats, RESP_TIMEOUT};

thread_local! {
    /// Thread-local buffer for reading lines of client output.
    static READBUF: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Hardcoded paths of ycsb files:
const YCSB_BIN: &str = "ycsb/bin/ycsb.sh";

const fn ycsb_profile(workload: char) -> &'static str {
    match workload {
        'a' => "ycsb/workloads/workloada",
        'b' => "ycsb/workloads/workloadb",
        'c' => "ycsb/workloads/workloadc",
        'd' => "ycsb/workloads/workloadd",
        'e' => "ycsb/workloads/workloade",
        'f' => "ycsb/workloads/workloadf",
        _ => unreachable!(),
    }
}

/// Wrapper handle to a YCSB basic driver process.
#[derive(Debug)]
pub struct YcsbDriver {
    handle: Child,
    feeder: JoinHandle<Option<(Stats, BTreeSet<String>)>>,
    signal: mpsc::Receiver<()>, // for timeout
}

impl YcsbDriver {
    /// Run a YCSB driver process that runs the specified workload for a number
    /// of operations, returning a handle to it. The driver translates YCSB
    /// output and feeds them directly into a KV client.
    pub(crate) fn exec(
        workload: char,
        num_ops: usize,
        load: bool, // true if 'load', false if 'run'
        client: ClientProc,
        ikeys: BTreeSet<String>,
    ) -> Result<YcsbDriver, RunnerError> {
        let mut handle = Command::new(YCSB_BIN)
            .arg(if load { "load" } else { "run" })
            .arg("basic")
            .arg("-P")
            .arg(ycsb_profile(workload))
            .arg("-p")
            .arg(format!("operationcount={}", num_ops))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = handle.stdout.take().unwrap();

        // spawn a translator & feeder thread that listens on the stdout of
        // the basic driver, translates output lines into our KV operations,
        // and feeds them to the KV client
        let (signal_tx, signal_rx) = mpsc::channel();
        let feeder = thread::spawn(move || Self::feeder_thread(stdout, client, ikeys, signal_tx));

        Ok(YcsbDriver {
            handle,
            feeder,
            signal: signal_rx,
        })
    }

    /// Wait for the workload to finish, returning the statistics reported by
    /// the feeder thread and consuming self. If feeder failed, returns `None`.
    pub(crate) fn wait(
        mut self,
        timeout: Duration,
    ) -> Result<Option<(Stats, BTreeSet<String>)>, RunnerError> {
        self.signal.recv_timeout(timeout)?;
        let feeder_ret = self.feeder.join().map_err(|_| RunnerError::Join)?;

        self.handle.kill()?;
        Ok(feeder_ret)
    }

    /// Parse the key from YCSB call line.
    fn parse_ycsb_key(segs: &mut SplitWhitespace) -> Result<String, RunnerError> {
        let mut key = segs
            .next()
            .ok_or(RunnerError::Parse("missing key segment".into()))?
            .to_string();
        key.push('_');
        key.push_str(
            segs.next()
                .ok_or(RunnerError::Parse("missing key segment".into()))?,
        );
        Ok(key)
    }

    /// Parse the square-bracketed "value" from YCSB call line.
    fn parse_ycsb_value(segs: &mut SplitWhitespace) -> Result<String, RunnerError> {
        if segs.next() != Some("[") {
            return Err(RunnerError::Parse("no value start bracket".into()));
        }
        let mut value = String::new();
        for seg in segs {
            value.push('_'); // substitute space with '_'
            value.push_str(seg);
        }
        Ok(value)
    }

    /// Parse the scan keys count from YCSB call line.
    fn parse_ycsb_scnt(segs: &mut SplitWhitespace) -> Result<usize, RunnerError> {
        let scnt = segs
            .next()
            .ok_or(RunnerError::Parse("missing scan count".into()))?
            .parse::<usize>()?;
        Ok(scnt)
    }

    /// Parse a YCSB driver output line into a KV operation call, or `None` if
    /// not a call line.
    fn interpret_ycsb_call(
        line: &str,
        ikeys: &mut BTreeSet<String>,
    ) -> Result<Option<(KvCall, String)>, RunnerError> {
        let mut segs = line.split_whitespace();
        match segs.next() {
            Some("INSERT") => {
                let key = Self::parse_ycsb_key(&mut segs)?;
                let value = Self::parse_ycsb_value(&mut segs)?;
                ikeys.insert(key.clone());
                Ok(Some((KvCall::Put { key, value }, "INSERT".into())))
            }

            Some("UPDATE") => {
                let key = Self::parse_ycsb_key(&mut segs)?;
                let value = Self::parse_ycsb_value(&mut segs)?;
                Ok(Some((KvCall::Swap { key, value }, "UPDATE".into())))
            }

            Some("READ") => {
                let key = Self::parse_ycsb_key(&mut segs)?;
                Ok(Some((KvCall::Get { key }, "READ".into())))
            }

            Some("SCAN") => {
                let key_start = Self::parse_ycsb_key(&mut segs)?;
                let key_end = if ikeys.is_empty() {
                    "zzzzzzzz".into()
                } else {
                    let scnt = Self::parse_ycsb_scnt(&mut segs)?;
                    ikeys
                        .range(key_start.clone()..)
                        .nth(scnt - 1)
                        .unwrap_or(ikeys.last().unwrap())
                        .clone()
                };
                Ok(Some((KvCall::Scan { key_start, key_end }, "SCAN".into())))
            }

            // no Deletes in default YCSB
            _ => Ok(None),
        }
    }

    /// Feed a line of YCSB basic driver output to the KV client. For the last
    /// few performance reporting lines, this function records the performance
    /// numbers into `stats`.
    fn feed_a_line(
        stdout: &mut BufReader<ChildStdout>,
        client: &mut ClientProc,
        line: &mut String,
        ikeys: &mut BTreeSet<String>,
        stats: &mut Stats,
        ended: &mut bool,
        started: &mut Option<Instant>,
        last_end: &mut Option<Instant>,
    ) -> Result<(), RunnerError> {
        line.clear();

        let size = stdout.read_line(line)?;
        if size == 0 {
            // EOF reached, workload completed
            *ended = true;
            return Ok(());
        }
        if line.trim().is_empty() {
            // skip empty line
            return Ok(());
        }

        if let Some((call, op)) = Self::interpret_ycsb_call(line, ikeys)? {
            // is an operation call, do it synchronously
            // RESP_TIMEOUT should be long enough to prevent false negatives
            let start = Instant::now();
            client.send_call(call)?;
            let _ = client.wait_resp(RESP_TIMEOUT)?;
            let elapsed = start.elapsed();

            if started.is_none() {
                *started = Some(start);
            }
            *last_end = Some(Instant::now());
            stats.record_op(&op, elapsed.as_micros() as f64);
        } else if line.starts_with('[') {
            // performance reporting line from YCSB, ignored
        } else if line.contains("No such file") {
            // probably not finding the workload profile file
            return Err(RunnerError::Io(line.clone()));
        }

        Ok(())
    }

    /// Translator & feeder thread function. Returns a tuple of statistics
    /// collected and sorted list of keys inserted on success.
    fn feeder_thread(
        stdout: ChildStdout,
        mut client: ClientProc,
        mut ikeys: BTreeSet<String>,
        signal: mpsc::Sender<()>,
    ) -> Option<(Stats, BTreeSet<String>)> {
        let mut stats = Stats::new();
        let mut ended = false;
        let mut started: Option<Instant> = None;
        let mut last_end: Option<Instant> = None;

        READBUF.with(|buf| {
            let line = &mut buf.borrow_mut();
            let mut stdout = BufReader::new(stdout);

            loop {
                if let Err(err) = Self::feed_a_line(
                    &mut stdout,
                    &mut client,
                    line,
                    &mut ikeys,
                    &mut stats,
                    &mut ended,
                    &mut started,
                    &mut last_end,
                ) {
                    if !matches!(err, RunnerError::Chan(_)) {
                        eprintln!("Error in feeder: {}", err);
                    }
                    break;
                }
                if ended {
                    break;
                }
            }
        });

        // stop the client process
        if let Err(err) = client.stop() {
            eprintln!("Error stopping client: {}", err);
        }

        let _ = signal.send(()); // for timeout
        if !ended {
            // error in workload feeding
            None
        } else {
            // ended successfully
            if let (Some(start), Some(end)) = (started, last_end) {
                stats.total_ms = (end - start).as_secs_f64() * 1000.0;
            }
            stats.merged = 1;
            Some((stats, ikeys))
        }
    }
}
