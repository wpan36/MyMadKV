# CS 739 MadKV Project 1

## Design Walkthrough

### Code Structure

The implementation is written in C++17 and uses synchronous gRPC for communication between clients and the server. The Protocol Buffers file defines the five required operations: Put, Swap, Get, Scan, and Delete. The generated protobuf and gRPC code is shared by both executables.

The server implementation contains the in-memory key-value store and the gRPC service handlers. The client implementation reads commands from standard input, sends the corresponding unary RPC, and prints results using the exact format expected by the testing and benchmarking tools. CMake generates the protobuf sources and builds the client and server, while the project Justfile exposes recipes for building, launching, testing, fuzzing, and benchmarking.

### Server Design

The server stores all entries in a `std::map<std::string, std::string>`. A map was selected because it maintains keys in lexicographic order, allowing Get, Put, Swap, and Delete in logarithmic time and supporting ordered range scans through `lower_bound`.

The gRPC server may execute handlers concurrently, so every operation acquires one global `std::mutex`. The mutex protects both individual map operations and the complete Scan operation. Therefore, every RPC has one atomic critical section and all completed operations can be placed in a single global order. This provides the linearizable behavior required by the project, at the cost of serializing all clients. The project specifies an in-memory service without durability and requires a globally ordered view of completed requests.

### RPC Protocol

The service uses five synchronous unary RPCs:

- `Put(key, value)` writes the value and returns whether the key existed before the operation.
- `Swap(key, value)` atomically replaces the value. It returns the old value when the key existed and returns null otherwise. When the key is absent, the new key-value pair is still inserted.
- `Get(key)` returns the current value or null.
- `Scan(start_key, end_key)` returns an ordered repeated list of key-value pairs whose keys are in the inclusive range.
- `Delete(key)` removes the key and returns whether it existed.

Replies use a separate presence indicator where necessary so that a missing key can be distinguished from an existing key whose value is an empty string. Scan returns repeated key-value messages in dictionary order. These semantics match the required Project 1 API.

The client creates a gRPC stub connected to the address supplied through the Justfile recipe. Each input command causes one blocking RPC. After the reply arrives, the client prints one result before reading the next command. This preserves the input/output ordering required by the automated runner. The required stdin/stdout command format is described in the project specification.

## Self-provided Testcases

<u>Found the following testcase results:</u> 1, 2, 3, 4, 5

You will run some testcases during demo time.

### Explanations

**Testcase 1 — basic single-client operations.**  
This test exercises Put, Get, Swap, Scan, and Delete in one sequential client session. It verifies normal return values, ordered scan output, updates to existing keys, and deletion visibility.

**Testcase 2 — single-client edge cases.**  
This test covers missing keys, repeated deletion, replacement behavior, scan boundary cases, and Swap on an absent key. It specifically verifies that Swap returns null for an absent key while still inserting the new value.

**Testcase 3 — concurrent clients with disjoint keys.**  
Multiple client processes issue operations against separate key sets. The test checks that simultaneous requests do not corrupt shared state and that each client observes the correct results for its own keys.

**Testcase 4 — concurrent clients with overlapping keys.**  
Two clients access the same key. FIFO synchronization ensures that one client first receives acknowledgement for Put or Swap, after which the other client reads the key. This verifies cross-client visibility of acknowledged writes.

**Testcase 5 — concurrent deletion and recreation.**  
Multiple clients operate on the same key through Delete, Get, Put, and Swap. The test verifies that an acknowledged deletion becomes visible to another client and that a later recreation or Swap is also observed correctly.

Together, the tests cover the required two single-client cases, one non-conflicting concurrent case, and two interfering concurrent cases.

## Fuzz Testing

<u>Parsed the following fuzz testing results:</u>

num_clis | conflict | outcome
:-: | :-: | :-:
1 | no | PASSED
3 | no | PASSED
3 | yes | PASSED

You will run a multi-client conflicting-keys fuzz test during demo time.

### Comments

All three fuzz configurations passed: one client with disjoint keys, three clients with disjoint keys, and three clients with conflicting keys. The conflicting-key case is the most important because it tests whether responses from different clients can be explained by a consistent ordering of operations.

During development, fuzz testing exposed an incorrect initial interpretation of Swap. The first implementation returned null when the key was absent but did not insert the supplied new value. After changing Swap to always store the new value while returning null for a missing old value, all fuzz configurations passed.

The use of a single mutex makes each RPC atomic and prevents interleaving inside an operation. This simple design provides strong correctness, although it limits parallelism.

## YCSB Benchmarking

<u>Single-client throughput/latency across workloads:</u>

![single-cli](plots-p1/ycsb-single-cli.png)

<u>Agg. throughput trend vs. number of clients:</u>

![tput-trend](plots-p1/ycsb-tput-trend.png)

<u>Avg. latency trend vs. number of clients:</u>

![lats-trend](plots-p1/ycsb-lats-trend.png)

### Comments

The single-client experiment compares workloads A through F. Workloads dominated by point reads and updates mainly measure one synchronous gRPC round trip plus one logarithmic map operation. Workload E performs range scans and returns multiple key-value pairs, so it has additional map traversal, protobuf construction, serialization, and network-transfer costs. Workload F performs read-modify-write behavior and therefore requires more logical work than a simple point read.

For the scalability experiments, workloads A, C, and E were run with 1, 2, 4, and 8 client processes. Increasing the number of clients creates more outstanding RPCs, which can initially improve aggregate throughput by keeping the server and network busy. However, every handler eventually competes for the same global mutex. Once that serialized critical section becomes the bottleneck, adding clients mainly increases queueing rather than useful parallel execution.

Workload C contains only reads, but reads also acquire the exclusive mutex in this implementation. As a result, multiple readers cannot execute concurrently. Workload A combines reads and updates and experiences the same serialization. Workload E is especially sensitive because a Scan holds the mutex while iterating over a range and constructing the response, which can delay all other requests.

The latency trend follows queueing behavior: as more clients submit requests concurrently, each request may wait longer to acquire the mutex. Therefore, average latency rises once the server approaches its maximum serialized service rate. The principal scalability bottleneck is the single global lock, followed by synchronous unary RPC overhead and response serialization for scans.

These experiments cover the required single-client A–F measurements and multi-client A, C, and E scalability measurements.

## Additional Discussion

The current design intentionally favors correctness and simplicity over scalability. Possible improvements include replacing the global mutex with a reader-writer lock, using finer-grained locking or sharding, shortening the Scan critical section, and adopting asynchronous gRPC handlers. Each optimization would require careful reasoning to preserve atomic operation semantics and linearizability.

The service is entirely in memory, so all state is lost when the server exits. Persistence, replication, and fault tolerance are outside the scope of Project 1 and can be introduced in later stages of MadKV.
