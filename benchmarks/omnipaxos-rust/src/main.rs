use omnipaxos::macros::Entry;
use omnipaxos::messages::Message;
use omnipaxos::util::LogEntry;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::collections::VecDeque;
use std::hint::black_box;
use std::time::Instant;

const NODE_COUNT: usize = 3;
const VALUE_COUNT: u64 = 4_096;
const SAMPLE_COUNT: usize = 7;

#[derive(Clone, Debug, Entry)]
struct Value {
    number: u64,
}

type Paxos = OmniPaxos<Value, MemoryStorage<Value>>;

struct Result {
    checksum: u64,
    messages: u64,
    nanoseconds: u128,
}

fn main() {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(run_sample());
    }
    samples.sort_unstable_by_key(|sample| sample.nanoseconds);
    let result = &samples[SAMPLE_COUNT / 2];

    println!("implementation: OmniPaxos 0.2.2 (Rust)");
    println!("values:         {VALUE_COUNT}");
    println!("median_ns:      {}", result.nanoseconds);
    println!(
        "ns_per_value:   {:.2}",
        result.nanoseconds as f64 / VALUE_COUNT as f64
    );
    println!("messages:       {}", result.messages);
    println!(
        "messages/value: {:.2}",
        result.messages as f64 / VALUE_COUNT as f64
    );
    println!("checksum:       {}", result.checksum);
}

fn run_sample() -> Result {
    let mut nodes = build_cluster();
    let mut messages = VecDeque::new();
    elect_leader(&mut nodes, &mut messages);
    let leader = current_leader(&nodes);
    let mut message_count = 0;

    let started = Instant::now();
    for sequence in 1..=VALUE_COUNT {
        let value = Value { number: sequence };
        nodes[leader].append(value).expect("append failed");
        drain(&mut nodes, &mut messages, &mut message_count);
    }
    let nanoseconds = started.elapsed().as_nanos();

    let decided = nodes[leader]
        .read_decided_suffix(0)
        .expect("decided suffix missing");
    let checksum = decided
        .iter()
        .filter_map(|entry| match entry {
            LogEntry::Decided(value) => Some(value),
            _ => None,
        })
        .fold(0_u64, |sum, value| sum.wrapping_add(value.number));
    assert_eq!(decided.len(), VALUE_COUNT as usize);
    assert_eq!(checksum, VALUE_COUNT * (VALUE_COUNT + 1) / 2);
    black_box(checksum);

    Result {
        checksum,
        messages: message_count,
        nanoseconds,
    }
}

fn build_cluster() -> Vec<Paxos> {
    (1..=NODE_COUNT as u64)
        .map(|pid| {
            let config = OmniPaxosConfig {
                cluster_config: ClusterConfig {
                    configuration_id: 1,
                    nodes: vec![1, 2, 3],
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

fn elect_leader(nodes: &mut [Paxos], messages: &mut VecDeque<Message<Value>>) {
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

fn current_leader(nodes: &[Paxos]) -> usize {
    let leader = nodes[0].get_current_leader().expect("leader missing");
    assert!(
        nodes
            .iter()
            .all(|node| node.get_current_leader() == Some(leader))
    );
    (leader - 1) as usize
}

fn drain(nodes: &mut [Paxos], messages: &mut VecDeque<Message<Value>>, message_count: &mut u64) {
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
