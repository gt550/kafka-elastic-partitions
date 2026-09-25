# Kafka Elastic Partitions

> A design and working standalone Java prototype for reversible Kafka partition resizing without republishing historical records.

Kafka supports increasing a topic's partition count, but reducing the count is not part of the accepted partition-management API. **Kafka Elastic Partitions** explores a lifecycle-based approach that lets a topic shrink when throughput falls and later expand again while preserving existing partition logs and offsets.

The central idea is simple:

> **Retire partitions instead of merging or immediately deleting them.**

```text
Initial topic: 10 writable partitions

P0 P1 P2 P3 P4 P5 P6 P7 P8 P9
A  A  A  A  A  A  A  A  A  A

                 resize 10 -> 5

P0 P1 P2 P3 P4 | P5 P6 P7 P8 P9
A  A  A  A  A  | T  T  T  T  T

               transition grace period

P0 P1 P2 P3 P4 | P5 P6 P7 P8 P9
A  A  A  A  A  | R  R  R  R  R

                 resize 5 -> 10

P0 P1 P2 P3 P4 P5 P6 P7 P8 P9
A  A  A  A  A  A  A  A  A  A
```

Where:

- `A` = `ACTIVE`
- `T` = `TRANSITIONING`
- `R` = `RETIRED`

A retired partition remains readable, keeps its existing log and offsets, and can be reactivated before its deletion deadline.

---

## Current Status

**Status: standalone prototype complete; Apache Kafka integration not yet implemented.**

The repository now contains a runnable Java/Maven prototype covering the core v1 behavior:

```text
Lifecycle state machine          COMPLETE
ResizeEpoch fencing              COMPLETE
Producer routing simulation      COMPLETE
Broker write-gate simulation     COMPLETE
Consumer/readability semantics   COMPLETE
Drain tracking                   COMPLETE
Transaction retirement rules     COMPLETE
Failure recovery simulation      COMPLETE
Kafka integration spike          COMPLETE

Apache Kafka source patch        NOT STARTED
Live Kafka-cluster resize        NOT YET IMPLEMENTED
```

Current test suite:

```text
37 tests
0 failures
0 errors
```

The prototype has demonstrated:

```text
10 ACTIVE
    ->
5 ACTIVE + 5 TRANSITIONING
    ->
5 ACTIVE + 5 RETIRED
    ->
10 ACTIVE
```

as well as stale producer handling, retired-partition reads, drain tracking, transaction-aware retirement, controller/quorum recovery, and stale-epoch deletion fencing.

The next implementation milestone is **K1: lifecycle metadata round-trip in an Apache Kafka fork**:

```text
KRaft controller metadata
        ->
broker metadata cache
        ->
MetadataResponse
        ->
Kafka client lifecycle parsing
```


---

## Why?

Kafka topics are often sized for peak throughput. When traffic later decreases, the extra partitions remain and continue adding broker, filesystem, replication, metadata, and operational overhead.

Shrinking is difficult because partitions are independent append-only logs. Merging them would require Kafka to redefine offsets, consumer state, ordering, and key-to-partition behavior.

This project avoids that problem:

```text
Do not merge old logs.
Change which existing partitions are writable.
```

Historical records stay where they are.

---

## v1 Goals

The first version is designed to:

- reduce the **writable** partition count without republishing historical records
- preserve physical partition logs and offsets
- allow consumers to continue reading retired partitions
- allow retired partitions to be reactivated during a later expansion
- provide a configurable producer metadata-convergence grace period
- enforce retirement at the broker
- keep normal Kafka record-retention behavior unchanged
- make resize operations recoverable after controller or broker failures
- fence stale lifecycle operations using a `ResizeEpoch`
- support sequential shrink and expansion operations
- keep normal Kafka producer/consumer APIs as the long-term application interface

---

## Non-Goals for v1

v1 intentionally does **not** attempt to:

- preserve key ordering across a partition-count change
- merge partition logs
- rewrite offsets
- republish historical records
- run multiple simultaneous resize operations on the same topic
- cancel an in-progress transition
- transparently support old Kafka clients on a topic once elastic resizing is activated

Changing partition count can already change key-to-partition mapping during normal Kafka expansion. Elastic shrink follows the same principle rather than introducing a stronger cross-partition ordering guarantee.

---

# Partition Lifecycle

## `ACTIVE`

Normal Kafka behavior.

```text
Produce: YES
Fetch:   YES
```

The partition participates in automatic producer-side partition selection.

## `TRANSITIONING`

The topic is shrinking and this partition is leaving the writable set.

```text
Fresh producer automatically selects it: NO
Stale producer may still write:          YES, during grace period
Consumer may read:                       YES
```

The grace period is configurable:

```text
partition.resize.transition.grace.ms
```

It allows upgraded producers with cached metadata to converge naturally to the new topology.

## `RETIRED`

The partition is read-only for application traffic.

```text
Produce: NO
Fetch:   YES
```

The broker rejects writes from:

- stale producers
- custom producers
- applications explicitly targeting the retired partition

Consumers may continue reading retained records.

## `DELETED`

The retirement deadline has expired and the physical partition is removed.

```text
Produce: NO
Fetch:   NO
```

---

# Shrink: 10 -> 5

A resize request creates a new lifecycle generation:

```text
ResizeEpoch = 42
oldActiveCount = 10
targetActiveCount = 5
```

The new topology becomes:

```text
P0-P4 = ACTIVE
P5-P9 = TRANSITIONING
```

Lifecycle-aware producers that refresh metadata immediately stop selecting P5-P9.

During the transition grace period, producers using stale cached metadata may still send records to P5-P9.

After the grace period and completion of already accepted work:

```text
P5-P9 = RETIRED
```

The broker rejects further application writes to those partitions, but consumers continue reading them.

No historical records are moved.

---

# Expansion and Reactivation: 5 -> 10

If P5-P9 are still retired rather than deleted, expansion reactivates them:

```text
P5 RETIRED -> ACTIVE
P6 RETIRED -> ACTIVE
P7 RETIRED -> ACTIVE
P8 RETIRED -> ACTIVE
P9 RETIRED -> ACTIVE
```

Their existing offset sequences continue naturally.

```text
P7 before retirement:

799998
799999
800000

----- RETIRED -----

P7 after reactivation:

800001
800002
800003
```

Reactivation cancels the pending deletion lifecycle.

If the requested count exceeds the highest existing partition ID, Kafka reactivates retired partitions first and creates genuinely new partitions only when needed.

Example:

```text
Originally: 10
Shrink:      5
Expand:     12

P0-P4     already ACTIVE
P5-P9     RETIRED -> ACTIVE
P10-P11   newly created
```

---

# Resize Epoch

Every topology-changing operation receives a monotonically increasing `ResizeEpoch`.

```text
ResizeEpoch 42: 10 -> 5
ResizeEpoch 43: 5  -> 8
ResizeEpoch 44: 8  -> 4
ResizeEpoch 45: 4  -> 12
```

The epoch provides:

1. **Crash recovery** — a new controller can resume an incomplete resize.
2. **Fencing** — stale lifecycle commands cannot overwrite newer state.
3. **Idempotency** — retrying an operation for the same epoch is safe.
4. **Delayed-work protection** — an old delete task cannot delete a partition reactivated by a newer epoch.

Conceptually, lifecycle metadata contains:

```text
partitionId
lifecycleState
resizeEpoch
transitionDeadline
deleteDeadline
```

The authoritative state belongs in the Kafka metadata quorum rather than broker-local memory.

---

# Producer Semantics

Kafka producer partition selection happens client-side.

A lifecycle-aware producer derives:

```text
writablePartitions =
    partitions where lifecycleState == ACTIVE
```

Example:

```text
Physical partitions: P0-P9
ACTIVE:              P0-P4
RETIRED:             P5-P9

Producer writable view:
P0-P4
```

The broker remains authoritative:

```text
ProduceRequest(P7)
        |
        v
Broker lifecycle check

ACTIVE        -> accept
TRANSITIONING -> accept during grace
RETIRED       -> reject
```

Explicit partition selection does not bypass lifecycle enforcement.

The transition grace period handles **stale cached metadata in upgraded clients**. It is not a correctness mechanism.

---

# Consumer Semantics

Consumers may read every non-deleted physical partition:

```text
ACTIVE          -> readable
TRANSITIONING   -> readable
RETIRED         -> readable
```

After a `10 -> 5` shrink:

```text
Writable partitions:
P0-P4

Readable partitions:
P0-P9
```

This is a core idea of the proposal:

> **Writable partition count and readable physical partition count can temporarily differ.**

Consumer-group assignors continue assigning retired partitions while those partitions remain readable.

---

# Consumer Drain Tracking

Consumer groups that were already relevant to the topic before the resize can be tracked for operational visibility.

A group may be automatically drain-tracked if it:

- had committed offsets for the topic, or
- had an active subscription/assignment involving the topic

Administrators may add or remove drain-tracked groups.

Example:

```text
P7 Log End Offset = 850001

pricing-service   committed = 850001  -> drained
audit-service     committed = 820000  -> lagging
```

New consumer groups created after the resize may still read retired partitions, but they are not automatically part of the original retirement's drain tracking.

Drain tracking does **not** extend the hard deletion deadline unless an administrator explicitly extends that deadline.

---

# Retirement and Deletion

The full design includes a configurable retirement period:

```text
retired.partition.delete.delay.ms
```

Conceptually:

```text
Day 0   P7 -> RETIRED
Day 30  P7 -> DELETED
```

The standalone prototype includes epoch-fenced physical deletion behavior and verifies that an old resize epoch cannot delete a partition reactivated by a newer epoch.

However, the Kafka integration spike uncovered an important implementation issue: Kafka strongly assumes dense numeric partition IDs, while consumer offsets are keyed by topic/partition identity. Recreating a physically deleted partition ID therefore needs careful offset/incarnation semantics.

For that reason, **hard physical deletion is deferred from the first real Kafka patch**. The first Kafka integration milestones will keep retired partition identity intact and focus on:

```text
ACTIVE -> TRANSITIONING -> RETIRED -> ACTIVE
```

This is sufficient to prove `10 -> 5 -> 10` without republishing historical records.

---

# Normal Kafka Retention Still Applies

Retirement does **not** freeze record retention.

If:

```text
topic retention.ms             = 7 days
retired partition delete delay = 30 days
```

the partition may exist for 30 days, while old log segments continue expiring according to the normal 7-day topic retention policy.

> Retirement preserves the partition lifecycle, not records beyond normal Kafka retention.

---

# Transaction Semantics

Once a partition enters `TRANSITIONING`, no new transaction may add that partition as a new participant.

Transactions that already included the partition may finish normally.

```text
Existing transaction involving P7:
commit -> allowed
abort  -> allowed

New transaction attempting to add P7:
rejected
```

At cutover Kafka:

```text
stop new application writes
        |
        v
finish already accepted work
        |
        v
resolve existing transaction participation
        |
        v
commit RETIRED state
```

An internal quiescing phase may implement this process without exposing `QUIESCING` as a public lifecycle state.

---

# Failure Recovery

Elastic resizing must survive normal Kafka failures.

### Controller failure

Resize state is persisted through the metadata quorum. A replacement controller resumes the existing `ResizeEpoch` rather than starting a new one.

### Broker / leader failure

Lifecycle state belongs to cluster metadata, not a broker process. A new partition leader restores the existing lifecycle state.

### Controller quorum unavailable

Time passing alone does not change lifecycle state. If a transition deadline expires while the metadata quorum is unavailable, the partition remains in its last committed state until the controller can safely resume.

### Stale lifecycle command

If:

```text
Epoch 42: P7 -> RETIRED
Epoch 43: P7 -> ACTIVE
```

a delayed Epoch-42 delete request is stale and must be rejected.

---

# Resize Concurrency

v1 allows **one active resize operation per topic at a time**.

A resize is complete when:

```text
currentActivePartitionCount == targetActivePartitionCount

AND

no partitions are TRANSITIONING
```

Retired partitions awaiting their deletion deadline do **not** keep the resize operation open.

This allows sequential operations such as:

```text
Epoch 42: 10 -> 5
Epoch 43: 5  -> 3
Epoch 44: 3  -> 8
```

Canceling or superseding an in-progress transition is outside the v1 scope.

---

# Metadata and Backward Compatibility

Elastic partitions require lifecycle-aware metadata.

A newer MetadataResponse version exposes:

```text
P0 ACTIVE
P1 ACTIVE
...
P5 RETIRED
```

The same metadata is interpreted differently:

```text
KafkaProducer:
select ACTIVE partitions for new automatic writes

KafkaConsumer:
read ACTIVE + TRANSITIONING + RETIRED
```

A new client talking to an older broker treats missing lifecycle state as `ACTIVE`.

## Old-client boundary

An old Kafka client cannot safely distinguish a retired-but-readable partition from a writable one.

Therefore v1 defines an explicit upgrade boundary:

> **Clients that do not understand lifecycle-aware metadata are not supported for a topic while that topic contains `TRANSITIONING` or `RETIRED` partitions.**

Normal topics on the same upgraded cluster remain compatible with old clients.

Recommended rollout:

```text
1. Upgrade brokers/controllers
2. Upgrade clients using the target topic
3. Enable elastic resizing for that topic
```

---

# Prototype Implementation

The repository now contains a runnable Java/Maven prototype under:

```text
prototype/
```

It intentionally models the lifecycle outside Apache Kafka first so the state machine and recovery invariants can be tested independently.

Implemented components include:

```text
PartitionLifecycleState
PartitionState
TopicState
ResizeController

ProducerMetadata
ElasticPartitioner
BrokerWriteGate
SimulatedProducer

ConsumerMetadata
ConsumerGroupState
SimulatedConsumer
DrainTracker

TransactionState
TransactionRegistry

MetadataQuorum
PartitionDeleteDecision
```

The prototype targets Java 17 and can be tested with:

```bash
cd prototype
mvn test
```

Current result:

```text
Tests run: 37
Failures: 0
Errors: 0
Skipped: 0
```

Runnable demos include:

```text
DemoApp
ProducerRoutingDemo
ConsumerSemanticsDemo
TransactionSemanticsDemo
FailureRecoveryDemo
```

The long-term goal remains unchanged: applications should continue using Kafka's normal APIs.

```java
KafkaProducer<String, Event> producer =
    new KafkaProducer<>(props);
```

The standalone classes such as `SimulatedProducer` are proof-of-concept tools, not proposed permanent application APIs.

---

# Proposed v1 Configuration

Names are provisional:

```text
partition.resize.transition.grace.ms
retired.partition.delete.delay.ms
```

Potential administrative operations:

```text
resizeTopic(...)
describeResize(...)
extendRetirementDeadline(...)
addDrainTrackedGroup(...)
removeDrainTrackedGroup(...)
```

---

# Kafka Integration Spike

A source-level integration spike has been completed against the current Apache Kafka code structure.

The spike maps prototype concepts to likely Kafka integration points:

```text
Prototype concept       Kafka integration area

ResizeController     -> KRaft controller / ReplicationControlManager
PartitionState       -> partition metadata / PartitionRegistration
ResizeEpoch          -> durable controller metadata
ProducerMetadata     -> ProducerMetadata / Cluster
ElasticPartitioner   -> BuiltInPartitioner / RecordAccumulator
BrokerWriteGate      -> KafkaApis / ReplicaManager / Partition
TransactionRegistry  -> AddPartitionsToTxn / transaction coordinator
ConsumerMetadata     -> consumer metadata / group assignors
MetadataQuorum       -> KRaft metadata quorum
```

See:

```text
docs/kafka-integration-spike.md
```

## Integration finding 1: hard deletion needs more design

The standalone simulator originally treated deleted partition IDs as permanently unused.

That does not map cleanly to Kafka's current dense partition-ID assumptions.

The first real Kafka implementation therefore defers physical deletion and focuses on reversible retirement/reactivation.

## Integration finding 2: do not assume transparent cross-partition retry

The producer simulator can demonstrate:

```text
stale metadata selects P7
broker rejects P7
metadata refreshes
future routing uses ACTIVE partitions
```

But a real Kafka producer already batches records and maintains idempotent/transactional sequencing per partition.

Therefore the first Kafka implementation should **not** claim that a broker-rejected batch can safely be moved transparently from P7 to another partition.

Initial real-Kafka behavior should be:

```text
fresh producer:
    chooses ACTIVE only

stale producer during grace:
    write may succeed

write after retirement fence:
    fail with partition-not-writable semantics
    refresh metadata for future sends
```

Transparent rerouting of an already-sent batch is deferred until its idempotence and transaction implications are proven.


---

# Relationship to KIP-694

Apache Kafka already has **KIP-694: Support Reducing Partitions for Topics**. It proposes partition modes including `ReadWrite` and `ReadOnly`, followed by delayed deletion.

As of September 25, 2026, KIP-694 remains **Under Discussion**.

Kafka Elastic Partitions explores a related but distinct design centered on **reversible partition retirement**.

| Area | KIP-694 direction | Kafka Elastic Partitions |
|---|---|---|
| Reduced partition | Read-only before deletion | Long-lived `RETIRED` lifecycle |
| Re-expansion | Not the primary lifecycle model | Reactivate existing retired partitions |
| Producer convergence | Partition-mode-aware client behavior | Explicit `TRANSITIONING` grace + broker fence |
| Resize identity | Deletion-oriented workflow | `ResizeEpoch` fencing and recovery |
| Consumer visibility | Continued reads during deletion delay | Explicit drain tracking |
| Sequential resize | Not the primary focus | Shrink / shrink / expand lifecycle |

This repository is an independent exploration, not a replacement claim for KIP-694.

---

# Standalone Proof of Concept

The standalone proof of concept has been implemented.

Completed behavior:

```text
[x] Create a 10-partition logical topic model
[x] Request 10 -> 5
[x] Move P5-P9 to TRANSITIONING
[x] Exclude TRANSITIONING partitions from refreshed producer routing
[x] Allow stale producer writes during transition grace
[x] Fence writes after grace / retirement
[x] Keep P5-P9 readable after retirement
[x] Track pre-existing consumer groups for drain visibility
[x] Allow new consumers to read retired partitions without auto-tracking them
[x] Block new transaction participation on TRANSITIONING partitions
[x] Allow existing transaction participants to commit/abort
[x] Block retirement while an existing transaction is unresolved
[x] Reactivate P5-P9 for 5 -> 10
[x] Preserve partition object identity through reactivation
[x] Resume resize after controller replacement
[x] Freeze lifecycle changes while metadata quorum is unavailable
[x] Fence stale ResizeEpoch deletion after reactivation
[x] Test hard deletion behavior in the standalone simulator
```

Current automated coverage:

```text
ConsumerSemanticsTest      8 tests
FailureRecoveryTest        9 tests
ProducerRoutingTest        5 tests
ResizeControllerTest       6 tests
TransactionSemanticsTest   9 tests
                           --------
Total                     37 tests
```

---

# Project Status

**Current status: working standalone prototype + completed Kafka integration spike**

Completed:

```text
[x] v1 lifecycle design
[x] architecture documentation
[x] state-machine documentation
[x] ResizeEpoch design
[x] producer semantics design
[x] consumer semantics design
[x] transaction semantics design
[x] failure-recovery design
[x] backward-compatibility design
[x] KIP-694 comparison
[x] test plan
[x] lifecycle Java prototype
[x] producer-routing prototype
[x] consumer/drain-tracking prototype
[x] transaction-aware retirement prototype
[x] failure-recovery prototype
[x] 37 passing Maven/JUnit tests
[x] Kafka source integration spike
```

Next milestones:

```text
[ ] K1 — fork Apache Kafka and add lifecycle metadata round-trip
[ ] K2 — filter producer routing to ACTIVE + enforce broker write fence
[ ] K3 — implement live controller-driven 10 -> 5 transition
[ ] K4 — implement live 5 -> 10 reactivation
[ ] Run end-to-end tests against a real local Kafka cluster
[ ] Revisit hard deletion / partition-ID incarnation semantics
[ ] Persist drain-tracking metadata if needed
[ ] Draft KIP-style proposal after implementation evidence
```

The project should currently be described as:

> **Designed and implemented a standalone Java prototype for reversible Kafka partition retirement/reactivation, with 37 passing tests, and completed a source-level Kafka integration spike.**

It should **not** yet be described as an Apache Kafka implementation or contribution.

---

# Repository Layout

```text
kafka-elastic-partitions/
├── README.md
├── LICENSE
├── docs/
│   ├── architecture.md
│   ├── state-machine.md
│   ├── resize-epoch.md
│   ├── producer-semantics.md
│   ├── consumer-semantics.md
│   ├── transactions.md
│   ├── failure-recovery.md
│   ├── backward-compatibility.md
│   ├── kip-694-comparison.md
│   ├── kafka-integration-spike.md
│   └── test-plan.md
└── prototype/
    ├── pom.xml
    ├── README.md
    └── src/
        ├── main/java/io/kafkaelastic/
        └── test/java/io/kafkaelastic/
```

---

# References

- Apache Kafka — KIP-694: Support Reducing Partitions for Topics  
  https://cwiki.apache.org/confluence/spaces/KAFKA/pages/165227379/KIP-694%2BSupport%2BReducing%2BPartitions%2Bfor%2BTopics

- Apache Kafka — Basic Kafka Operations  
  https://github.com/apache/kafka/blob/trunk/docs/operations/basic-kafka-operations.md

- Apache Kafka — Source  
  https://github.com/apache/kafka

- Apache Kafka — KIP-195: AdminClient.createPartitions  
  https://cwiki.apache.org/confluence/spaces/KAFKA/pages/73637005/KIP-195%2BAdminClient.createPartitions

- Apache Kafka — Protocol documentation  
  https://kafka.apache.org/protocol.html

---

## Disclaimer

This repository is an independent design and prototype exploration. It is not an Apache Kafka feature, an accepted Kafka Improvement Proposal, or an Apache Software Foundation project.
