//! Workload-matrix benchmark for pinned OmniPaxos 0.2.2.
//!
//! Mirrors benchmarks/benchmark.zig: same value counts, cluster sizes,
//! payload sizes, and commit modes (sync, pipelined windows), same
//! median-of-seven reporting, and one machine-readable JSON line per run.
//! In-memory storage, no serialization, no network, no fsync: a workload
//! fixture, not a service benchmark and not a language comparison.

use omnipaxos::macros::Entry;
use omnipaxos::messages::Message;
use omnipaxos::util::LogEntry;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::collections::VecDeque;
use std::hint::black_box;
use std::time::Instant;

const SAMPLE_COUNT: usize = 7;

#[derive(Clone, Debug, Entry)]
struct U64Value {
    seq: u64,
}

#[derive(Clone, Debug, Entry)]
struct Blob64 {
    seq: u64,
    _pad: [u8; 56],
}

#[derive(Clone, Debug, Entry)]
struct Blob1k {
    seq: u64,
    _pad: [u8; 1016],
}

trait Payload: omnipaxos::storage::Entry + Clone {
    fn from_seq(seq: u64) -> Self;
    fn seq(&self) -> u64;
}

impl Payload for U64Value {
    fn from_seq(seq: u64) -> Self {
        Self { seq }
    }
    fn seq(&self) -> u64 {
        self.seq
    }
}

impl Payload for Blob64 {
    fn from_seq(seq: u64) -> Self {
        Self { seq, _pad: [0; 56] }
    }
    fn seq(&self) -> u64 {
        self.seq
    }
}

impl Payload for Blob1k {
    fn from_seq(seq: u64) -> Self {
        Self {
            seq,
            _pad: [0; 1016],
        }
    }
    fn seq(&self) -> u64 {
        self.seq
    }
}

struct ModeSpec {
    name: &'static str,
    window: u64,
}

const SYNC: ModeSpec = ModeSpec {
    name: "sync",
    window: 1,
};
const PIPELINE8: ModeSpec = ModeSpec {
    name: "pipeline8",
    window: 8,
};
const PIPELINE64: ModeSpec = ModeSpec {
    name: "pipeline64",
    window: 64,
};

struct Sample {
    checksum: u64,
    messages: u64,
    nanoseconds: u128,
}

fn main() {
    let mut failed = false;
    failed |= run_matrix::<U64Value>("u64-3n", 3, 4_096, &[SYNC, PIPELINE8, PIPELINE64]);
    failed |= run_matrix::<Blob64>("blob64-3n", 3, 1_024, &[SYNC, PIPELINE8]);
    failed |= run_matrix::<Blob1k>("blob1k-3n", 3, 1_024, &[SYNC, PIPELINE8]);
    failed |= run_matrix::<U64Value>("u64-5n", 5, 4_096, &[SYNC, PIPELINE8]);
    if failed {
        eprintln!("benchmark self-checks failed");
        std::process::exit(1);
    }
}

fn run_matrix<T: Payload>(
    workload: &str,
    node_count: u64,
    value_count: u64,
    modes: &[ModeSpec],
) -> bool {
    let mut failed = false;
    for mode in modes {
        failed |= run_mode::<T>(workload, node_count, value_count, mode);
    }
    failed
}

fn run_mode<T: Payload>(
    workload: &str,
    node_count: u64,
    value_count: u64,
    mode: &ModeSpec,
) -> bool {
    let mut samples: Vec<Sample> = (0..SAMPLE_COUNT)
        .map(|_| run_sample::<T>(node_count, value_count, mode, None))
        .collect();
    samples.sort_unstable_by_key(|sample| sample.nanoseconds);
    let median = &samples[SAMPLE_COUNT / 2];

    // One extra instrumented pass for the latency distribution, so timer
    // overhead never contaminates the throughput samples above.
    let mut window_ns: Vec<u64> = Vec::new();
    run_sample::<T>(node_count, value_count, mode, Some(&mut window_ns));
    window_ns.sort_unstable();
    let pct = |p: usize| window_ns[(window_ns.len() * p / 100).min(window_ns.len() - 1)];
    let (p50, p90, p99, max) = (pct(50), pct(90), pct(99), window_ns[window_ns.len() - 1]);

    let per_value = median.nanoseconds as f64 / value_count as f64;
    let payload_bytes = std::mem::size_of::<T>();
    println!("workload:       {workload} mode={} (OmniPaxos 0.2.2)", mode.name);
    println!("values:         {value_count} nodes={node_count} payload={payload_bytes}B");
    println!(
        "median_ns:      {} (min {}, max {} across {SAMPLE_COUNT} samples)",
        median.nanoseconds,
        samples[0].nanoseconds,
        samples[SAMPLE_COUNT - 1].nanoseconds
    );
    println!("ns_per_value:   {per_value:.2}");
    println!("per-value ns in window p50/p90/p99/max: {p50}/{p90}/{p99}/{max}");
    println!("messages:       {}", median.messages);
    println!(
        "messages/value: {:.2}",
        median.messages as f64 / value_count as f64
    );
    println!("checksum:       {}", median.checksum);
    println!(
        "{{\"impl\":\"omnipaxos\",\"workload\":\"{workload}\",\"mode\":\"{}\",\
         \"values\":{value_count},\"nodes\":{node_count},\
         \"payload_bytes\":{payload_bytes},\"ns_total_median\":{},\
         \"ns_total_min\":{},\"ns_total_max\":{},\"ns_per_value\":{per_value:.2},\
         \"lat_p50_ns\":{p50},\"lat_p90_ns\":{p90},\"lat_p99_ns\":{p99},\
         \"lat_max_ns\":{max},\"messages\":{},\"checksum\":{}}}",
        mode.name,
        median.nanoseconds,
        samples[0].nanoseconds,
        samples[SAMPLE_COUNT - 1].nanoseconds,
        median.messages,
        median.checksum
    );
    println!();

    let expected_checksum = value_count * (value_count + 1) / 2;
    if median.checksum != expected_checksum {
        eprintln!("SELF-CHECK FAILED for {workload}/{}", mode.name);
        return true;
    }
    false
}

fn run_sample<T: Payload>(
    node_count: u64,
    value_count: u64,
    mode: &ModeSpec,
    mut window_ns: Option<&mut Vec<u64>>,
) -> Sample {
    let mut nodes = build_cluster::<T>(node_count);
    let mut messages = VecDeque::new();
    elect_leader(&mut nodes, &mut messages);
    let leader = current_leader(&nodes);
    let mut message_count = 0;

    let started = Instant::now();
    let mut previous = started;
    let mut seq = 1u64;
    while seq <= value_count {
        let window = mode.window.min(value_count - seq + 1);
        for offset in 0..window {
            nodes[leader]
                .append(T::from_seq(seq + offset))
                .expect("append failed");
        }
        drain(&mut nodes, &mut messages, &mut message_count);
        if let Some(latencies) = window_ns.as_deref_mut() {
            let now = Instant::now();
            latencies.push((now - previous).as_nanos() as u64 / window);
            previous = now;
        }
        seq += window;
    }
    let nanoseconds = started.elapsed().as_nanos();

    let decided = nodes[leader]
        .read_decided_suffix(0)
        .expect("decided suffix missing");
    let checksum = decided
        .iter()
        .filter_map(|entry| match entry {
            LogEntry::Decided(value) => Some(value.seq()),
            _ => None,
        })
        .fold(0_u64, |sum, value| sum.wrapping_add(value));
    assert_eq!(decided.len(), value_count as usize);
    black_box(checksum);

    Sample {
        checksum,
        messages: message_count,
        nanoseconds,
    }
}

fn build_cluster<T: Payload>(node_count: u64) -> Vec<OmniPaxos<T, MemoryStorage<T>>> {
    let members: Vec<u64> = (1..=node_count).collect();
    (1..=node_count)
        .map(|pid| {
            let config = OmniPaxosConfig {
                cluster_config: ClusterConfig {
                    configuration_id: 1,
                    nodes: members.clone(),
                    ..Default::default()
                },
                server_config: ServerConfig {
                    pid,
                    election_tick_timeout: 2,
                    resend_message_tick_timeout: 10_000,
                    ..Default::default()
                },
            };
            config
                .build(MemoryStorage::default())
                .expect("invalid OmniPaxos configuration")
        })
        .collect()
}

fn elect_leader<T: Payload>(
    nodes: &mut [OmniPaxos<T, MemoryStorage<T>>],
    messages: &mut VecDeque<Message<T>>,
) {
    let mut message_count = 0;
    for _ in 0..100 {
        for node in nodes.iter_mut() {
            node.tick();
        }
        drain(nodes, messages, &mut message_count);
        if nodes.iter().all(|node| node.get_current_leader().is_some()) {
            return;
        }
    }
    panic!("leader election did not finish");
}

fn current_leader<T: Payload>(nodes: &[OmniPaxos<T, MemoryStorage<T>>]) -> usize {
    let leader = nodes[0].get_current_leader().expect("leader missing");
    assert!(
        nodes
            .iter()
            .all(|node| node.get_current_leader() == Some(leader))
    );
    (leader - 1) as usize
}

fn drain<T: Payload>(
    nodes: &mut [OmniPaxos<T, MemoryStorage<T>>],
    messages: &mut VecDeque<Message<T>>,
    message_count: &mut u64,
) {
    loop {
        for node in nodes.iter_mut() {
            for message in node.outgoing_messages() {
                messages.push_back(message);
                *message_count += 1;
            }
        }
        let Some(message) = messages.pop_front() else {
            return;
        };
        let receiver = message.get_receiver() as usize - 1;
        nodes[receiver].handle_incoming(message);
    }
}
