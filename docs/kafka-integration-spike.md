# Kafka Integration Spike

**Project:** Kafka Elastic Partitions  
**Source baseline inspected:** Apache Kafka `trunk`, September 25, 2026  
**Purpose:** Map the standalone prototype onto real Kafka code paths before creating an Apache Kafka source fork or patch.

> This document is an integration investigation, not an accepted Apache Kafka design.

---

# 1. Current Kafka Baseline

Apache Kafka currently supports increasing a topic's partition count through `Admin.createPartitions`, but the public documentation still states that reducing a topic's partition count is not supported.

The existing Admin API is explicitly increase-oriented:

```text
Admin.createPartitions(...)
NewPartitions.increaseTo(...)
```

The current topic tool also routes `--partitions` increases through `Admin.createPartitions`.

Related Apache proposal:

```text
KIP-694: Support Reducing Partitions for Topics
Status: Under Discussion
```

KIP-694 is especially relevant because it proposes:

- a partition mode in KRaft metadata
- read/write-aware client metadata
- a partition deletion API
- delayed deletion
- producer and consumer filtering

Kafka Elastic Partitions differs by emphasizing:

```text
ACTIVE
    ->
TRANSITIONING
    ->
RETIRED
    ->
ACTIVE or DELETED
```

with reversible retirement and `ResizeEpoch` fencing.

---

# 2. Real Kafka Source Map

The following Kafka areas correspond to the components already modeled by the prototype.

## 2.1 Controller / KRaft Metadata

Primary source:

```text
metadata/src/main/java/org/apache/kafka/controller/
    ReplicationControlManager.java
```

`ReplicationControlManager` owns topic and partition control state and replays `PartitionRecord` / partition changes into controller state.

Relevant metadata concepts include:

```text
TopicRecord
PartitionRecord
PartitionChangeRecord
PartitionRegistration
```

The broker later consumes KRaft metadata through its metadata cache.

### Prototype mapping

```text
TopicState
PartitionState
ResizeController
ResizeEpoch
```

would ultimately become controller-owned durable metadata.

---

## 2.2 Broker Metadata Cache

Broker startup constructs:

```text
KRaftMetadataCache
```

and supplies it to major broker subsystems including:

```text
KafkaApis
ReplicaManager
TransactionCoordinator
AddPartitionsToTxnManager
```

This makes the metadata cache the natural broker-side read path for partition lifecycle state.

Relevant source areas:

```text
core/src/main/scala/kafka/server/BrokerServer.scala
metadata/.../KRaftMetadataCache
```

---

## 2.3 Metadata Protocol

Producer and consumer clients learn partition topology through Kafka metadata.

The integration will require a new Metadata API version with lifecycle information per partition.

Conceptually:

```text
Partition metadata:
    partitionId
    leader
    replicas
    isr
    ...
    lifecycleState
```

Possible values:

```text
ACTIVE = 0
TRANSITIONING = 1
RETIRED = 2
```

Clients do not need retirement deadlines for normal routing.

They primarily need the lifecycle state.

The metadata version itself provides the compatibility boundary for older clients.

---

# 3. Proposed Durable Metadata Model

The prototype currently stores all lifecycle data directly in Java objects.

Real Kafka needs the state persisted through KRaft.

A plausible first design is:

```text
TopicResizeRecord
    topicId
    resizeEpoch
    targetActivePartitionCount
    status
```

and partition lifecycle information such as:

```text
Partition lifecycle:
    lifecycleState
    lifecycleEpoch
    transitionDeadlineMs
    retirementDeleteDeadlineMs
```

There are two implementation alternatives.

## Alternative A — Extend PartitionRecord / PartitionChangeRecord

Add lifecycle fields to Kafka's existing partition metadata.

Advantages:

- lifecycle travels naturally with `PartitionRegistration`
- brokers already receive partition registration updates
- close to the approach proposed by KIP-694

Disadvantages:

- partition replication metadata and resize lifecycle become coupled
- several generated metadata schemas and image classes change

## Alternative B — Add Separate Lifecycle Metadata Records

For example:

```text
TopicResizeRecord
PartitionLifecycleRecord
```

Advantages:

- clearer separation between replica topology and elastic lifecycle
- `ResizeEpoch` has an obvious durable home
- easier conceptual ownership of deadlines and retirement state

Disadvantages:

- additional metadata images/cache structures
- more controller and broker plumbing

### Spike preference

Start the implementation design with:

```text
TopicResizeRecord
+
lifecycle fields associated with PartitionRegistration
```

because brokers ultimately need a cheap per-partition lifecycle lookup in the same path where they already resolve partition state.

The exact schema should be finalized only after the first Kafka fork experiment.

---

# 4. Admin API

Current Kafka:

```java
Admin.createPartitions(...)
```

is explicitly documented as increasing partition count.

It also treats a requested count less than or equal to the current count as invalid.

Therefore overloading `createPartitions` for shrinking would distort an established API contract.

## Proposed API

Prefer a new operation:

```java
Admin.resizePartitions(...)
```

Conceptually:

```java
resizePartitions(
    Map<String, ResizePartitionsSpec> topics,
    ResizePartitionsOptions options
)
```

with:

```text
ResizePartitionsSpec
    targetActivePartitionCount
```

Optional future fields:

```text
transitionGraceMs
retirementDeleteDelayMs
```

but v1 should probably use topic/cluster configuration for those policies instead of putting them into every request.

## Protocol

Likely new protocol pair:

```text
ResizePartitionsRequest
ResizePartitionsResponse
```

Broker request handling would forward the operation to the active KRaft controller in the same broad style as current controller-owned topic mutations.

---

# 5. Controller Integration

The central implementation point is expected to be:

```text
ReplicationControlManager
```

A resize request should roughly become:

```text
validate request
    |
allocate next ResizeEpoch
    |
identify partitions
    |
write durable KRaft metadata records
    |
metadata log replication
    |
brokers observe new lifecycle
```

For:

```text
10 -> 5
```

the controller chooses the highest active partition IDs:

```text
P5-P9
```

and records:

```text
ACTIVE -> TRANSITIONING
```

This tail-selection rule is important for compatibility with Kafka's existing dense partition numbering model.

---

# 6. Producer Metadata Integration

Current producer construction uses:

```text
ProducerMetadata
```

and Kafka producer batching/partition selection uses:

```text
RecordAccumulator
BuiltInPartitioner
Cluster
```

The current `BuiltInPartitioner` obtains available partitions from `Cluster` for sticky routing.

It also has keyed routing logic conceptually equivalent to:

```text
positiveHash(key) % numPartitions
```

That exposes a major integration requirement:

> Producer routing cannot simply replace `numPartitions` with `activePartitionCount` unless active partition IDs are guaranteed to be the dense range `0..activeCount-1`.

During normal shrink:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

this property holds.

But it may stop holding if deleted partition IDs are never reused.

This is discussed in **Integration Finding #1** below.

---

# 7. Proposed Producer View

Metadata should expose every physical non-deleted partition, including lifecycle state.

The producer derives:

```text
writablePartitions =
    lifecycleState == ACTIVE
```

For the normal shrink case:

```text
physical:
P0 P1 P2 P3 P4 P5 P6 P7 P8 P9

writable:
P0 P1 P2 P3 P4
```

## Unkeyed / Sticky Routing

`BuiltInPartitioner` currently asks the cluster for partitions suitable for routing.

Kafka Elastic Partitions needs the equivalent of:

```text
cluster.writablePartitionsForTopic(topic)
```

rather than using all available physical partitions.

## Keyed Routing

Current routing uses the partition count directly.

A lifecycle-aware implementation should conceptually use:

```text
writablePartitionIds
```

instead.

For example:

```text
index = hash(key) % writablePartitionIds.size()
partitionId = writablePartitionIds[index]
```

This matches the standalone `ElasticPartitioner` prototype.

---

# 8. Explicit Producer Partition

If an application explicitly specifies:

```java
new ProducerRecord<>("orders", 7, key, value)
```

the client should not silently move that record.

If P7 is no longer writable:

```text
fail
```

rather than:

```text
redirect to P2
```

The broker must independently enforce the same rule.

---

# 9. Broker Produce Enforcement

Current produce requests enter:

```text
core/src/main/scala/kafka/server/KafkaApis.scala

KafkaApis.handleProduceRequest(...)
```

Kafka currently validates authorization, topic/partition existence, record validity, and then passes valid records toward `ReplicaManager`.

Relevant path:

```text
ProduceRequest
    |
KafkaApis.handleProduceRequest
    |
ReplicaManager
    |
Partition / UnifiedLog append
```

## Recommended enforcement placement

An early check in `KafkaApis` is useful for fast rejection, but it should not be the sole correctness boundary.

Lifecycle may change concurrently between:

```text
request validation
```

and:

```text
log append
```

Therefore the strongest design is:

```text
KafkaApis
    optional early lifecycle check
        |
ReplicaManager / Partition
    authoritative writable-state check
        |
append
```

The local `Partition` representation should know whether client writes are currently permitted.

Internal replication and control records must not be blocked by the application-write gate.

---

# 10. Transition Grace in Real Kafka

Our prototype models:

```text
TRANSITIONING
```

as:

```text
fresh producer:
does not select partition

stale producer:
may still send during grace
```

Broker behavior:

```text
before transitionDeadline:
    client produce accepted

after transitionDeadline:
    new client produce fenced
```

But the visible state remains `TRANSITIONING` until the controller completes all required retirement work and commits `RETIRED`.

This preserves the existing design invariant:

> A wall-clock deadline never commits lifecycle state on its own.

---

# 11. Transaction Integration

Kafka's request path includes:

```text
ADD_PARTITIONS_TO_TXN
```

handled through `KafkaApis` and transaction-related broker components such as:

```text
AddPartitionsToTxnManager
TransactionCoordinator
ReplicaManager
```

Our rule maps cleanly to that operation:

```text
ACTIVE:
    may become a new transaction participant

TRANSITIONING:
    may not become a new participant

RETIRED:
    may not become a new participant
```

Existing transaction participation must still be allowed to resolve.

This means lifecycle enforcement cannot simply reject every transactional operation against a transitioning partition.

Kafka must distinguish:

```text
new participation
```

from:

```text
completion/markers for an existing participant
```

---

# 12. Consumer Integration

Consumer behavior intentionally differs from producer behavior.

```text
Producer:
ACTIVE only

Consumer:
ACTIVE + TRANSITIONING + RETIRED
```

Therefore MetadataResponse should still contain retired partitions.

Existing client-side and broker-side assignment paths must continue seeing them as readable physical partitions.

Relevant areas include:

```text
KafkaConsumer
AsyncKafkaConsumer
ConsumerMetadata
SubscriptionState

group-coordinator/.../assignor/
    UniformAssignor
    RangeAssignor / related assignment paths
```

For the newer broker-driven consumer protocol, the group coordinator's topic describer/assignors must also use the readable physical topology.

---

# 13. Drain Tracking Integration

Drain tracking is not part of basic consumer assignment.

It is controller/operations state associated with a particular retirement.

Conceptually:

```text
RetirementDrainState
    topicId
    resizeEpoch
    groupIds
```

Progress comes from durable committed group offsets.

A later consumer group may read a retired partition without being automatically added to the original drain-tracking set.

This area crosses the topic controller and group coordinator, so it should come after the basic lifecycle patch.

It is **not** the first Kafka-fork milestone.

---

# 14. Integration Finding #1 — Partition IDs After Physical Deletion

This is the most important conflict discovered by the spike.

Our standalone failure-recovery prototype currently has the invariant:

> A physically deleted partition ID is never reused.

For example:

```text
original:
P0-P9

shrink:
P0-P4 ACTIVE
P5-P9 RETIRED

delete P5-P9

later expand:
create P10
```

This is clean in the simulator, but it conflicts with important assumptions in existing Kafka.

Current keyed routing uses a partition count and produces an integer in:

```text
0 .. numPartitions-1
```

Kafka's existing partition-growth tooling also treats the existing partition set as a dense numeric range and adds the next IDs after the current count.

Therefore:

```text
ACTIVE = {0,1,2,3,4,10}
```

is not a safe v1 assumption for an upstream-compatible implementation.

## Consequence

The prototype rule:

```text
deleted partition IDs are never reused
```

should **not** be carried into the Kafka implementation without a much larger protocol redesign.

## Candidate v1 direction

Because shrink always retires the highest-numbered tail:

```text
10 -> 5
retires P5-P9
```

physical deletion can leave:

```text
P0-P4
```

which is still dense.

If the topic later expands after those retired partitions have already been physically deleted, Kafka would naturally want to create:

```text
P5
P6
...
```

again.

However, numeric-ID reuse creates another problem:

```text
old consumer committed offset for P5
vs
new physical P5 log
```

Kafka's consumer offsets are associated with topic/partition identity, and the topic ID remains the same.

A stale old committed offset could therefore be misinterpreted for a newly created P5.

## Blocking design decision

Before implementing hard physical deletion in real Kafka, choose one of:

### Option A — Reuse tail partition IDs and explicitly clean stale group offsets

This preserves Kafka's dense partition model.

It requires a reliable mechanism to remove committed offsets for physically deleted partitions before those IDs can be recreated.

### Option B — Introduce partition incarnation/generation identity

This would distinguish:

```text
P5 incarnation A
P5 incarnation B
```

but would require much broader protocol and offset-coordinator changes.

### Option C — Keep a tombstone / partition shell after data deletion

The numeric partition identity remains reserved even after log data is reclaimed.

Reactivation can recreate storage while retaining logical partition identity.

This may require persisting the next logical offset or equivalent partition-generation information.

### Spike recommendation

Do **not** implement hard partition deletion in the first Kafka patch.

First implement:

```text
ACTIVE
TRANSITIONING
RETIRED
RETIRED -> ACTIVE
```

while keeping retired partition metadata/log identity intact.

Treat permanent physical deletion as a second integration milestone after the partition-ID/offset question is resolved.

---

# 15. Integration Finding #2 — Automatic Cross-Partition Retry

The standalone producer prototype currently demonstrates:

```text
stale metadata selects P7
broker rejects P7
producer refreshes metadata
producer reroutes record to P2
```

This is conceptually useful, but real Kafka makes this substantially harder.

Kafka batches records per destination partition in `RecordAccumulator`, and idempotent/transactional producer state is partition-specific.

Once a batch has been sent to P7, transparently turning that same failed batch into a P2 batch may interact with:

```text
producer sequence numbers
idempotence
transactions
batch ordering
callbacks
retry bookkeeping
```

## Safer initial Kafka behavior

For the first real integration:

```text
fresh lifecycle-aware producer
    -> never chooses RETIRED

stale producer during TRANSITIONING grace
    -> accepted

producer that reaches RETIRED after grace
    -> broker rejects
    -> client refreshes metadata
    -> affected send fails with a specific exception
```

The application can then resend normally.

Transparent cross-partition retry should be considered a later optimization only after proving it preserves idempotent and transactional semantics.

## Prototype follow-up

The standalone simulator should eventually be adjusted so that:

```text
broker rejection
```

causes metadata refresh but does not automatically claim that a sent batch can safely migrate partitions.

---

# 16. Error Model

A new error is preferable to overloading generic errors.

Conceptually:

```text
PARTITION_NOT_WRITABLE
```

mapped to something like:

```text
PartitionNotWritableException
```

Properties:

```text
ACTIVE:
    never returned for lifecycle

TRANSITIONING during grace:
    write accepted

TRANSITIONING after fence:
    PARTITION_NOT_WRITABLE

RETIRED:
    PARTITION_NOT_WRITABLE
```

For an explicitly selected partition, this is a direct application-visible failure.

For automatic routing, the producer should refresh metadata so future records use the active set.

Whether the already-failed record can transparently move partitions is intentionally deferred.

---

# 17. Compatibility Boundary

KIP-694 also recognized that older clients cannot safely understand read/write partition modes.

Kafka Elastic Partitions reaches the same conclusion.

## New client -> old broker

If lifecycle is absent:

```text
default = ACTIVE
```

Safe.

## Old client -> new broker, normal topic

Safe if every partition is `ACTIVE`.

## Old client -> topic containing TRANSITIONING or RETIRED

Not supported in v1.

The server must detect a Metadata API version that cannot represent lifecycle state and fail closed for that topic/operation.

The exact protocol error should be decided during the Kafka fork.

---

# 18. Suggested First Kafka Fork Milestone

Do **not** attempt the entire feature at once.

The first real Kafka patch should prove metadata plumbing only.

## Milestone K1 — Lifecycle Metadata Round Trip

Goal:

```text
controller
    ->
KRaft metadata log
    ->
metadata image/cache
    ->
broker
    ->
MetadataResponse
    ->
Kafka client
```

with one extra field:

```text
lifecycleState
```

No actual shrinking yet.

### Expected test

Create a normal topic and verify all partitions appear as:

```text
ACTIVE
```

Then use a test-only/controller hook to set:

```text
P7 = RETIRED
```

and verify:

```text
broker metadata cache sees RETIRED
MetadataResponse exposes RETIRED
new client parses RETIRED
```

This isolates the hardest cross-layer schema work before introducing resize orchestration.

---

# 19. Suggested Second Kafka Fork Milestone

## Milestone K2 — Producer Filtering + Broker Fence

Add:

```text
producer selects ACTIVE only
```

and:

```text
broker rejects client ProduceRequest to RETIRED
```

Tests:

```text
fresh producer never selects P7
explicit P7 send fails
consumer can still fetch P7
```

Do not implement automatic cross-partition retry yet.

---

# 20. Suggested Third Kafka Fork Milestone

## Milestone K3 — Controller Resize 10 -> 5

Add:

```text
ResizePartitionsRequest
ResizeEpoch
ACTIVE -> TRANSITIONING
TRANSITIONING -> RETIRED
```

At this point demonstrate against an actual local Kafka cluster:

```text
10 -> 5
```

with:

```text
5 writable
10 readable
```

---

# 21. Suggested Fourth Kafka Fork Milestone

## Milestone K4 — Reactivation 5 -> 10

Implement:

```text
RETIRED -> ACTIVE
```

before any hard physical deletion exists.

Demonstrate:

```text
10 -> 5 -> 10
```

without republishing data and without recreating the partition logs.

This is the strongest first real end-to-end proof of the project.

---

# 22. Defer These Until Later

The first Kafka fork should explicitly defer:

```text
hard physical partition deletion
partition-ID reuse after deletion
consumer offset cleanup for deleted partition IDs
automatic cross-partition producer retry
drain-tracked group persistence
retirement deadline automation
full old-client rejection policy
```

They remain part of the full design, but they are not required to prove elastic retirement/reactivation.

---

# 23. Initial Kafka Files to Inspect / Modify

Expected high-value files and modules:

```text
clients/src/main/resources/common/message/
    MetadataResponse.json
    [new ResizePartitions request/response later]

clients/src/main/java/org/apache/kafka/common/
    PartitionInfo.java
    Cluster.java

clients/src/main/java/org/apache/kafka/clients/producer/
    KafkaProducer.java

clients/src/main/java/org/apache/kafka/clients/producer/internals/
    ProducerMetadata.java
    BuiltInPartitioner.java
    RecordAccumulator.java
    Sender.java

clients/src/main/java/org/apache/kafka/clients/consumer/internals/
    ConsumerMetadata.java
    SubscriptionState.java

metadata/src/main/resources/common/metadata/
    PartitionRecord.json
    PartitionChangeRecord.json
    [potential new TopicResizeRecord.json]

metadata/src/main/java/org/apache/kafka/controller/
    ReplicationControlManager.java
    QuorumController.java

metadata/src/main/java/org/apache/kafka/metadata/
    PartitionRegistration.java
    KRaftMetadataCache.java

core/src/main/scala/kafka/server/
    KafkaApis.scala
    ReplicaManager.scala
    BrokerServer.scala

core/src/main/scala/kafka/cluster/
    Partition.scala

group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/
    UniformAssignor.java
    related assignor classes

tools/src/main/java/org/apache/kafka/tools/
    TopicCommand.java
```

Exact files may shift as trunk evolves; this list reflects the source structure inspected for this spike.

---

# 24. Recommended Repository Strategy

Keep this project repository:

```text
gt550/kafka-elastic-partitions
```

as the design/prototype repository.

Do not copy the complete Apache Kafka source tree into it.

For real Kafka work, use a separate fork:

```text
gt550/kafka
```

with a feature branch such as:

```text
elastic-partitions/lifecycle-metadata
```

Then link the fork/branch from this project's README.

This keeps:

```text
kafka-elastic-partitions
    = design + prototype + documentation

apache/kafka fork
    = actual Kafka implementation
```

cleanly separated.

---

# 25. Spike Outcome

The integration spike validates that the project maps onto real Kafka architecture:

```text
prototype concept         Kafka integration area

ResizeController       -> KRaft controller
PartitionState         -> Partition metadata / registration
ResizeEpoch            -> durable controller metadata
ProducerMetadata       -> ProducerMetadata / Cluster
ElasticPartitioner     -> BuiltInPartitioner / RecordAccumulator
BrokerWriteGate        -> Partition / ReplicaManager produce gate
TransactionRegistry    -> AddPartitionsToTxn / transaction coordinator
ConsumerMetadata       -> consumer metadata + group assignors
MetadataQuorum         -> KRaft metadata quorum
```

It also discovered two architectural issues that should change the implementation plan:

1. **Do not assume permanently sparse partition IDs after deletion.**
2. **Do not assume a broker-rejected idempotent batch can be transparently rerouted to another partition.**

Therefore the recommended real-Kafka path is:

```text
K1 lifecycle metadata plumbing
        ->
K2 producer filtering + broker fence
        ->
K3 live 10 -> 5 retirement
        ->
K4 live 5 -> 10 reactivation
        ->
resolve hard deletion separately
```

That gives the project a realistic route from standalone prototype to an Apache Kafka source-level proof of concept.

---

# References

- Apache Kafka source: https://github.com/apache/kafka
- Kafka Admin API: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java
- Kafka producer: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
- BuiltInPartitioner: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java
- RecordAccumulator: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
- KafkaApis: https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/KafkaApis.scala
- ReplicaManager: https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/server/ReplicaManager.scala
- ReplicationControlManager: https://github.com/apache/kafka/blob/trunk/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
- KIP-694: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/165227379/KIP-694+Support+Reducing+Partitions+for+Topics
