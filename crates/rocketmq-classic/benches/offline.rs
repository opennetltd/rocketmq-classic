use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    hint::black_box,
    process::Command,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::BytesMut;
use rocketmq_classic_common::message::{
    DecodeOptions, Message, decode_one, encode_nullable_property_entries,
};
use rocketmq_classic_core::route::RouteSnapshot;
use rocketmq_classic_protocol::{
    codes::SerializeType,
    header::{CommandHeader, HeaderMap, SendMessageRequestHeaderV1},
};
use rocketmq_classic_remoting::{
    client::{RemotingClient, TransportMode},
    codec::frame,
    command::RemotingCommand,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    runtime::Builder,
};

const CODEC_ITERATIONS: usize = 20_000;
const MESSAGE_ITERATIONS: usize = 10_000;
const SEND_ITERATIONS: usize = 20_000;
const ROUTE_ITERATIONS: usize = 100_000;
const PENDING_ITERATIONS: usize = 500;
const ALLOCATION_SAMPLE_LIMIT: usize = 1_000;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

// The harness records aggregate allocation observations only while a bounded
// measurement loop is active; ordinary timing runs do not pay counter costs.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() && TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        result
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, new_size) };
        if !result.is_null() && TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        result
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn main() {
    assert!(
        !cfg!(debug_assertions),
        "run this fixed-input microbenchmark with `cargo bench` in release mode"
    );
    print_environment();

    let command = RemotingCommand {
        code: 11,
        ext_fields: Some(
            BTreeMap::from([
                ("topic".into(), "bench-topic".into()),
                ("producerGroup".into(), "bench-producer".into()),
            ])
            .into(),
        ),
        body: Some(vec![0x5a; 256]),
        serialization: SerializeType::Json,
        ..RemotingCommand::default()
    };
    let command_frame_bytes = frame::encode(&command).unwrap();
    benchmark_sync(
        "remoting-frame-encode-decode",
        CODEC_ITERATIONS,
        command_frame_bytes.len(),
        || {
            let encoded = frame::encode(&command).unwrap();
            let mut input = BytesMut::from(encoded.as_slice());
            let decoded = frame::decode_next(&mut input).unwrap().unwrap();
            black_box((decoded.code, decoded.body.as_ref().map(Vec::len)));
        },
    );

    let message_record = zstd_message_fixture();
    benchmark_sync(
        "message-record-zstd-decode",
        MESSAGE_ITERATIONS,
        message_record.len(),
        || {
            let (message, consumed) =
                decode_one(&message_record, DecodeOptions::default()).unwrap();
            black_box((message.body().len(), consumed));
        },
    );

    let body = vec![0x41; 512];
    benchmark_sync(
        "producer-send-request-construction",
        SEND_ITERATIONS,
        body.len(),
        || {
            let mut message = Message::new("bench-topic", body.clone());
            message.set_tags("bench-tag").unwrap();
            message
                .put_user_property("bench-key", "bench-value")
                .unwrap();
            let entries = message.nullable_property_entries().collect::<Vec<_>>();
            let properties = encode_nullable_property_entries(Some(&entries));
            let header = {
                let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
                __phase18_header_0.set_producer_group(Some("bench-producer".into()));
                __phase18_header_0.set_topic(message.topic().into());
                __phase18_header_0.set_default_topic("TBW102".into());
                __phase18_header_0.set_default_topic_queue_nums(4);
                __phase18_header_0.set_queue_id(-1);
                __phase18_header_0.set_sys_flag(0);
                __phase18_header_0.set_born_timestamp(1_700_000_000_000);
                __phase18_header_0.set_flag(0);
                __phase18_header_0.set_properties(Some(properties));
                __phase18_header_0.set_reconsume_times(Some(0));
                __phase18_header_0.set_unit_mode(Some(false));
                __phase18_header_0.set_max_reconsume_times(None);
                __phase18_header_0.set_batch(Some(false));
                __phase18_header_0
            };
            let mut fields = HeaderMap::new();
            header.encode_fields(&mut fields);
            black_box((message.body().len(), fields.len()));
        },
    );

    let mut routes = RouteSnapshot::default();
    for index in 0..128 {
        routes.broker_addresses.insert(
            format!("broker-{index:03}"),
            BTreeMap::from([(Some(0), Some(format!("127.0.0.1:{}", 10_900 + index)))]),
        );
    }
    benchmark_sync(
        "route-publish-address-lookup-128-entries",
        ROUTE_ITERATIONS,
        "broker-064".len(),
        || {
            black_box(routes.publish_address("broker-064").unwrap().len());
        },
    );

    run_pending_completion_benchmark();
}

fn benchmark_sync(name: &str, iterations: usize, input_bytes: usize, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    let elapsed = started.elapsed();
    print_timing(name, iterations, input_bytes, elapsed);

    let allocation_iterations = iterations.min(ALLOCATION_SAMPLE_LIMIT);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::Release);
    for _ in 0..allocation_iterations {
        operation();
    }
    TRACK_ALLOCATIONS.store(false, Ordering::Release);
    print_allocations(
        name,
        allocation_iterations,
        ALLOCATION_CALLS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
    );
}

fn run_pending_completion_benchmark() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = BytesMut::new();
            let mut read_buffer = [0; 8192];
            loop {
                let length = socket.read(&mut read_buffer).await.unwrap();
                if length == 0 {
                    break;
                }
                input.extend_from_slice(&read_buffer[..length]);
                loop {
                    let command = match frame::decode_next(&mut input) {
                        Ok(Some(command)) => command,
                        Ok(None) => break,
                        Err(error) => {
                            panic!("loopback benchmark received invalid frame: {error}")
                        }
                    };
                    let mut response = command;
                    response.mark_response();
                    let encoded = frame::encode(&response).unwrap();
                    socket.write_all(&encoded).await.unwrap();
                }
            }
        });

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let request = RemotingCommand {
            code: 34,
            ext_fields: Some(BTreeMap::from([("topic".into(), "bench".into())]).into()),
            body: Some(vec![0x42; 128]),
            ..RemotingCommand::default()
        };
        let started = Instant::now();
        for _ in 0..PENDING_ITERATIONS {
            let response = client
                .invoke_sync(&address, request.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            black_box((response.opaque, response.body));
        }
        print_timing(
            "loopback-pending-request-completion",
            PENDING_ITERATIONS,
            128,
            started.elapsed(),
        );

        let allocation_iterations = PENDING_ITERATIONS.min(100);
        ALLOCATION_CALLS.store(0, Ordering::Relaxed);
        ALLOCATED_BYTES.store(0, Ordering::Relaxed);
        TRACK_ALLOCATIONS.store(true, Ordering::Release);
        for _ in 0..allocation_iterations {
            let response = client
                .invoke_sync(&address, request.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            black_box((response.opaque, response.body));
        }
        TRACK_ALLOCATIONS.store(false, Ordering::Release);
        print_allocations(
            "loopback-pending-request-completion",
            allocation_iterations,
            ALLOCATION_CALLS.load(Ordering::Relaxed),
            ALLOCATED_BYTES.load(Ordering::Relaxed),
        );
        client.shutdown().await;
        server.await.unwrap();
    });
}

fn print_timing(name: &str, iterations: usize, input_bytes: usize, elapsed: Duration) {
    let nanos_per_iteration = elapsed.as_nanos() as f64 / iterations as f64;
    println!(
        "bench={name} iterations={iterations} input_bytes={input_bytes} elapsed_ns={} ns_per_iteration={nanos_per_iteration:.1}",
        elapsed.as_nanos()
    );
}

fn print_allocations(name: &str, iterations: usize, calls: u64, bytes: u64) {
    println!(
        "alloc={name} iterations={iterations} allocation_calls_per_iteration={:.2} allocated_bytes_per_iteration={:.1}",
        calls as f64 / iterations as f64,
        bytes as f64 / iterations as f64
    );
}

fn zstd_message_fixture() -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let record = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "compressed-zstd-v1")
        .unwrap()["recordBase64"]
        .as_str()
        .unwrap();
    STANDARD.decode(record).unwrap()
}

fn print_environment() {
    let rustc = Command::new("rustc")
        .arg("-V")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .unwrap_or_else(|| "unavailable; run `rustc -V`".into());
    let hardware = Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .or_else(|| {
            Command::new("uname")
                .arg("-m")
                .output()
                .ok()
                .and_then(|output| String::from_utf8(output.stdout).ok())
        })
        .unwrap_or_else(|| "unavailable".into());
    println!(
        "environment=os:{} arch:{} hardware:{} logical_cpus:{} profile=release toolchain={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        hardware.trim(),
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        rustc.trim()
    );
}
