# Architecture

## Kafka Elastic Partitions

This document describes the proposed v1 architecture for **Kafka Elastic Partitions**: a lifecycle-based mechanism that allows a Kafka topic to reduce its writable partition count and later increase it again without republishing historical records or rewriting existing offsets.

The design is centered on one principle:

> **Partition shrinking changes partition lifecycle, not historical data placement.**

Existing partition logs remain where they are. A shrink operation changes which partitions are eligible for new writes.

---

## 1. Problem Statement

Kafka supports increasing a topic's partition count, but reducing the partition count is difficult because partitions are independent append-only logs.

A naïve shrink would require Kafka to answer questions such as:

- How should two partition logs be merged?
- How should offsets be rewritten?
- What happens to committed consumer offsets?
- How should ordering be preserved?
- What happens to keys whose partition mapping changes?
- How should in-flight and transactional writes be handled?

Kafka Elastic Partitions avoids these problems by not merging logs.

Instead, the system distinguishes between:

```text
physical partitions
```

and:

```text
writable partitions
```

A topic may temporarily have:

```text
10 physical partitions
5 writable partitions
```

because some physical partitions are `RETIRED` but still readable.

---

## 2. Design Goals

The v1 architecture must:

- shrink the writable partition count without republishing historical records
- preserve physical logs and existing offsets
- allow retired partitions to remain readable
- allow retired partitions to be reactivated before deletion
- let upgraded producers converge naturally through metadata refresh
- provide broker-side write fencing
- preserve normal Kafka retention behavior
- support transactional producers safely
- survive controller and broker failures
- fence stale resize operations using a monotonic `ResizeEpoch`
- support sequential shrink and expansion operations
- limit v1 to one active resize operation per topic

---

## 3. Non-Goals

v1 does not attempt to:

- merge physical partition logs
- rewrite offsets
- preserve key ordering across a partition-count change
- provide cross-partition ordering guarantees
- support multiple simultaneous resize operations on the same topic
- cancel or supersede an in-progress transition
- transparently support lifecycle-unaware clients on an elastically resized topic
- preserve records beyond the topic's normal retention policy

---

# 4. High-Level Architecture

The feature spans five logical areas:

```text
+-------------------------------------------------------------+
|                      KRaft Controller                       |
|                                                             |
|  ResizeEpoch                                                |
|  Target active count                                        |
|  Partition lifecycle                                        |
|  Transition deadlines                                       |
|  Retirement deadlines                                       |
+-------------------------+-----------------------------------+
                          |
                          v
+-------------------------------------------------------------+
|                         Brokers                             |
|                                                             |
|  Produce enforcement                                        |
|  Fetch servicing                                            |
|  Replication                                                |
|  Transaction completion                                     |
|  Lifecycle-aware leader behavior                            |
+-------------------------+-----------------------------------+
                          |
              +-----------+-----------+
              |                       |
              v                       v
+--------------------------+   +--------------------------+
|      KafkaProducer       |   |      KafkaConsumer       |
|                          |   |                          |
| Lifecycle-aware metadata |   | Lifecycle-aware metadata |
| Select ACTIVE only       |   | Read all non-deleted     |
+--------------------------+   +--------------------------+
              |
              v
+-------------------------------------------------------------+
|                     Admin / Operations                      |
|                                                             |
| resize topic                                                |
| describe resize                                             |
| extend retirement deadline                                  |
| manage drain-tracked groups                                 |
+-------------------------------------------------------------+
```

The controller owns lifecycle state.

The broker enforces lifecycle state.

The producer uses lifecycle metadata for partition selection.

The consumer uses the same metadata for read visibility.

---

# 5. Partition Lifecycle

A physical partition has one of three visible lifecycle states.

```text
ACTIVE
  |
  | shrink selects partition for removal from writable set
  v
TRANSITIONING
  |
  | grace expires and accepted work settles
  v
RETIRED
```

A retired partition may either:

```text
RETIRED -> ACTIVE
```

through later expansion, or:

```text
RETIRED -> DELETED
```

after its retirement deadline.

`DELETED` is not a normal metadata lifecycle value because the physical partition no longer exists.

---

## 5.1 ACTIVE

Normal Kafka partition behavior.

```text
Automatic producer selection: YES
Explicit produce:              YES
Fetch:                         YES
Replication:                   YES
```

---

## 5.2 TRANSITIONING

The partition is leaving the writable topology.

```text
Automatic selection by refreshed producer: NO
Produce from stale producer:             YES, during grace
Fetch:                                    YES
Replication:                              YES
New transaction participation:            NO
Existing transaction completion:          YES
```

`TRANSITIONING` exists to make topology convergence graceful.

A producer that has refreshed metadata immediately stops choosing the partition.

A producer with stale cached metadata may continue sending to it until the configured transition deadline.

---

## 5.3 RETIRED

The partition remains physically present but is read-only for application traffic.

```text
Automatic producer selection: NO
Application ProduceRequest:   NO
Fetch:                        YES
Replication:                  YES
Reactivation:                 YES
```

A retired partition remains part of the readable topology until deletion.

---

# 6. Topic Topology Model

The architecture distinguishes three counts.

## Physical Partition Count

Number of physical partition IDs that still exist.

Example:

```text
P0-P9

physicalPartitionCount = 10
```

## Active Partition Count

Number of partitions currently writable for normal producer partition selection.

Example:

```text
P0-P4 ACTIVE
P5-P9 RETIRED

activePartitionCount = 5
```

## Readable Partition Count

Number of non-deleted partitions available to consumers.

Example:

```text
P0-P9 readable

readablePartitionCount = 10
```

This is a central architectural change:

> `activePartitionCount` and `readablePartitionCount` may differ.

---

# 7. Resize Epoch

Each topology-changing operation receives a monotonically increasing `ResizeEpoch`.

Example:

```text
ResizeEpoch 42: 10 -> 5
ResizeEpoch 43: 5  -> 8
ResizeEpoch 44: 8  -> 4
```

The epoch belongs to the topic resize operation.

Conceptually:

```text
TopicResizeState {
    topicId
    resizeEpoch
    previousActiveCount
    targetActiveCount
    state
    startedAt
}
```

Partition lifecycle metadata conceptually contains:

```text
PartitionLifecycleState {
    partitionId
    lifecycleState
    lifecycleEpoch
    transitionDeadline
    retirementDeleteDeadline
}
```

---

## 7.1 Why ResizeEpoch Exists

### Crash recovery

A replacement controller can determine:

```text
topic = orders
ResizeEpoch = 42
targetActiveCount = 5
state = IN_PROGRESS
```

and resume rather than guessing.

### Stale-operation fencing

If:

```text
Epoch 42 retires P7
Epoch 43 reactivates P7
```

then any delayed command from Epoch 42 is stale.

```text
requestEpoch = 42
currentEpoch = 43

42 < 43
=> reject / ignore
```

### Idempotency

Repeating:

```text
Retire P7 for ResizeEpoch 42
```

must be safe if P7 was already retired by the same epoch.

---

# 8. Metadata Architecture

Elastic resizing requires lifecycle-aware metadata.

A newer MetadataResponse version exposes the lifecycle state of each physical partition.

Conceptually:

```text
TopicMetadata
  P0 ACTIVE
  P1 ACTIVE
  P2 ACTIVE
  P3 ACTIVE
  P4 ACTIVE
  P5 RETIRED
  P6 RETIRED
  P7 RETIRED
  P8 RETIRED
  P9 RETIRED
```

The same response is interpreted differently by producers and consumers.

---

## 8.1 Producer View

The producer derives:

```text
writablePartitions =
    partitions where lifecycleState == ACTIVE
```

Example:

```text
physical = P0-P9
writable = P0-P4
```

Automatic partition selection operates only on `writable`.

---

## 8.2 Consumer View

The consumer derives:

```text
readablePartitions =
    ACTIVE
    + TRANSITIONING
    + RETIRED
```

Example:

```text
physical = P0-P9
readable = P0-P9
```

This enables consumers to drain or replay retired partitions.

---

# 9. Shrink Workflow

Consider:

```text
10 -> 5
```

Initial state:

```text
P0-P9 ACTIVE
```

---

## Phase 1: Request

Administrator requests:

```text
targetActiveCount = 5
```

Controller validates:

- no other resize is currently in progress for the topic
- target count is valid
- lifecycle-aware client requirements are satisfied operationally
- target count is less than current active count

Controller creates:

```text
ResizeEpoch = 42
```

---

## Phase 2: Enter TRANSITIONING

Partitions leaving the writable set are:

```text
P5-P9
```

Controller commits:

```text
P0-P4 ACTIVE
P5-P9 TRANSITIONING
```

and assigns:

```text
transitionDeadline =
    now + partition.resize.transition.grace.ms
```

Refreshed producers stop automatically selecting P5-P9.

Stale producers may continue sending to them during the grace period.

Consumers continue reading P0-P9.

---

## Phase 3: Grace Period

Example:

```text
partition.resize.transition.grace.ms = 300000
```

During this period:

```text
refreshed producer -> writes P0-P4
stale producer     -> may still write P5-P9
consumer           -> reads P0-P9
```

The grace period is not relied upon for correctness.

It is a convergence optimization.

---

## Phase 4: Cutover

When the grace deadline is reached, the controller initiates the retirement cutover.

For each transitioning partition:

```text
1. fence new application writes
2. finish already accepted writes
3. resolve allowed existing transactional participation
4. verify recoverable/stable partition state
5. commit RETIRED lifecycle
```

Time alone does not change lifecycle state.

The controller must explicitly commit the transition.

---

## Phase 5: Resize Complete

Once:

```text
activePartitionCount == 5
```

and:

```text
no partition is TRANSITIONING
```

ResizeEpoch 42 is complete.

The topic now has:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

Retired partitions awaiting deletion do not keep the resize operation active.

---

# 10. Expansion Workflow

Consider the topic currently has:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

and an administrator requests:

```text
5 -> 8
```

A new epoch is created:

```text
ResizeEpoch = 43
```

Kafka reactivates:

```text
P5 RETIRED -> ACTIVE
P6 RETIRED -> ACTIVE
P7 RETIRED -> ACTIVE
```

while:

```text
P8-P9 remain RETIRED
```

Reactivation:

- cancels the pending deletion workflow for those partitions
- clears drain-tracking state associated with the prior retirement
- allows new application writes
- preserves the existing offset sequence

---

## 10.1 Expansion Beyond Existing Physical Partitions

Suppose:

```text
current active = 5
physical partitions = P0-P9
target active = 12
```

Kafka:

```text
reactivates P5-P9
creates P10-P11
```

The general rule is:

> **Reactivate before creating.**

---

# 11. Producer Architecture

Partition selection is performed client-side.

The long-term implementation target is the standard Kafka producer.

```text
KafkaProducer
    |
    v
metadata cache
    |
    v
writablePartitions(topic)
    |
    v
partition selection
    |
    v
ProduceRequest
```

The producer does not treat `TRANSITIONING` or `RETIRED` partitions as candidates for new automatic writes.

---

## 11.1 Stale Producer Metadata

Suppose the producer still believes:

```text
P0-P9 writable
```

during transition.

It may send:

```text
ProduceRequest(P7)
```

During transition grace:

```text
broker accepts
```

After P7 becomes retired:

```text
broker rejects
```

The producer refreshes metadata.

For an automatically selected partition, the client may retry after re-partitioning against the current ACTIVE set.

---

## 11.2 Explicit Partition Selection

An application may explicitly request:

```text
partition = 7
```

If P7 is retired:

```text
broker rejects
```

The client must not silently redirect an explicitly targeted record to another partition.

---

# 12. Broker Enforcement

Broker enforcement is the correctness boundary.

Metadata convergence alone is not sufficient.

A leader handling a ProduceRequest evaluates lifecycle state:

```text
ACTIVE
    -> accept

TRANSITIONING
    -> accept only while grace/cutover permits

RETIRED
    -> reject application produce
```

Fetch remains allowed for all non-deleted states.

This protects against:

- stale metadata
- old custom partitioning logic
- explicit partition targeting
- non-standard producer implementations

---

# 13. Consumer Architecture

Consumer-group assignment uses the readable topology.

```text
readablePartitions =
    ACTIVE
    + TRANSITIONING
    + RETIRED
```

Existing consumer groups continue receiving retired partitions.

A new consumer created after the resize may also read retired partitions while they still exist.

---

## 13.1 Drain Tracking

Drain tracking is separate from basic readability.

Groups relevant before the resize may be recorded for operational visibility.

A group is initially considered relevant if it:

- had committed offsets for the topic, or
- had an active subscription or assignment involving the topic

Administrators may add or remove groups.

Example:

```text
P7 LEO = 850001

pricing-service = 850001 -> drained
audit-service   = 820000 -> lagging
```

Because a retired partition no longer accepts writes, its current log-end offset is sufficient as the current drain target.

If the partition is reactivated, the old retirement/drain workflow is canceled.

---

# 14. Offset Retention for Drain-Tracked Groups

Kafka may normally expire committed offsets for sufficiently inactive groups.

For a drain-tracked group and retired partition, the resize subsystem should preserve the relevant committed offset while that retirement lifecycle remains active.

This avoids losing operational drain state.

Offset protection ends when:

- the partition is reactivated
- the partition is deleted
- the administrator removes the group from drain tracking
- the retirement lifecycle otherwise terminates

---

# 15. Record Retention

`RETIRED` does not change normal Kafka record retention.

Example:

```text
retention.ms = 7 days
retired.partition.delete.delay.ms = 30 days
```

P7 may remain physically present for 30 days, while its old log segments continue expiring according to normal 7-day topic retention.

Therefore:

```text
partition lifetime != record lifetime
```

---

# 16. Retirement Deletion

When a partition enters `RETIRED`, the controller records a deletion deadline.

Conceptually:

```text
retirementDeleteDeadline =
    retiredAt + retired.partition.delete.delay.ms
```

Example:

```text
Day 0  -> RETIRED
Day 30 -> DELETE
```

The deadline is hard unless:

- the partition is reactivated
- an administrator explicitly extends it

Drain-tracked consumer lag does not automatically extend the deadline.

---

## 16.1 Deletion

At the deadline, the controller validates that the partition is still:

```text
RETIRED
```

and that the deletion action belongs to the current lifecycle epoch.

If valid:

```text
remove replicas
remove partition metadata
remove lifecycle metadata
```

After deletion the partition no longer appears in metadata.

---

# 17. Transaction Architecture

Kafka transactions can span multiple partitions.

A transaction does not "start on a partition"; partitions become transaction participants as transactional records are sent.

Once P7 becomes `TRANSITIONING`:

```text
new transaction participation for P7 -> rejected
```

But:

```text
transaction already involving P7 -> allowed to commit/abort
```

The rule is based on whether P7 had already become a participant before the transition.

This prevents a long-running transaction from adding P7 after retirement has begun.

---

## 17.1 Cutover and Transactions

At retirement cutover:

```text
fence new application writes
        |
        v
allow already-participating transactions
to resolve
        |
        v
complete internal transaction bookkeeping
        |
        v
commit RETIRED
```

An internal "quiescing" implementation phase may exist, but `QUIESCING` is not required as a public state.

---

# 18. Failure Recovery

Resize correctness must survive failures at every stage.

---

## 18.1 Controller Failure During TRANSITIONING

Suppose:

```text
ResizeEpoch = 42
P5-P9 TRANSITIONING
```

and the active controller fails.

The replacement controller reads committed metadata:

```text
epoch = 42
target = 5
P5-P9 = TRANSITIONING
```

and resumes the operation.

It does not create a new epoch.

---

## 18.2 Broker Failure During TRANSITIONING

If the leader for P7 fails, normal Kafka leader recovery occurs.

The new leader receives:

```text
P7 lifecycle = TRANSITIONING
ResizeEpoch = 42
```

Lifecycle is not reset because broker leadership changes.

---

## 18.3 Failure During Cutover

If a broker fails after the write fence begins but before `RETIRED` is committed:

```text
metadata still says TRANSITIONING
```

The replacement leader recovers the log normally.

The controller retries the retirement process idempotently.

---

## 18.4 Controller Quorum Loss

Brokers do not independently advance lifecycle state based on wall-clock deadlines.

If the metadata quorum is unavailable:

```text
remain in last committed lifecycle state
```

After recovery, the controller resumes and explicitly commits the next state.

---

## 18.5 Stale Delete Versus Reactivation

Example:

```text
Epoch 42:
P7 RETIRED

Epoch 43:
P7 ACTIVE
```

A delayed delete from Epoch 42 must fail lifecycle-epoch validation.

This prevents an old timer/task from removing a currently active partition.

---

# 19. Resize Concurrency Model

v1 supports one active resize operation per topic.

If:

```text
ResizeEpoch 42
10 -> 5
P5-P9 TRANSITIONING
```

another resize request is rejected:

```text
RESIZE_IN_PROGRESS
```

A resize becomes complete once:

```text
target active count reached
AND
no TRANSITIONING partitions remain
```

Retired partitions awaiting deletion are not considered an active resize.

---

## 19.1 Sequential Resizes

After Epoch 42 completes:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

a new shrink may occur:

```text
Epoch 43: 5 -> 3
```

which transitions:

```text
P3-P4
```

Later expansion may reactivate retired partitions.

---

# 20. Backward Compatibility

Lifecycle-aware resizing introduces an explicit client upgrade boundary.

A new client talking to an old broker sees no lifecycle state and treats all partitions as:

```text
ACTIVE
```

That is safe because old brokers do not support elastic retirement.

An old client talking to a new broker works normally for topics that have not entered elastic lifecycle states.

However:

> An old client is not supported for a topic while that topic contains `TRANSITIONING` or `RETIRED` partitions.

The reason is fundamental:

- an old producer cannot distinguish retired-but-readable from writable
- an old consumer must still be able to see retired physical partitions
- the existing metadata model does not provide a safe old-client interpretation for both cases

Recommended rollout:

```text
1. Upgrade brokers/controllers
2. Upgrade clients for the target topic
3. Enable elastic resizing
```

---

# 21. Prototype Architecture

The proof of concept may use a wrapper:

```text
Application
    |
    v
ElasticKafkaProducer
    |
    v
KafkaProducer
    |
    v
Kafka Broker
```

The wrapper is a prototype mechanism only.

It can demonstrate:

- lifecycle-aware metadata
- writable partition filtering
- shrink behavior
- stale metadata handling
- reactivation

The target implementation moves this behavior into the standard Kafka client and broker code paths.

---

# 22. Proposed Configuration

Provisional v1 settings:

```text
partition.resize.transition.grace.ms
retired.partition.delete.delay.ms
```

Both should support a cluster default.

Topic-level override should be supported where appropriate.

---

# 23. Proposed Administrative Operations

Conceptual APIs:

```text
resizeTopic(topic, targetActivePartitionCount)

describeResize(topic)

extendRetirementDeadline(topic, partitions, deadline)

addDrainTrackedGroup(topic, groupId)

removeDrainTrackedGroup(topic, groupId)
```

The exact Java/AdminClient API shape remains an implementation detail for the prototype/KIP phase.

---

# 24. Core Invariants

The following invariants define v1 correctness.

### Invariant 1

Historical records are never moved as part of shrink.

```text
DATA REPUBLISHED = 0
```

### Invariant 2

Existing partition offsets are never rewritten because of resize.

### Invariant 3

A `RETIRED` partition does not accept new application writes.

### Invariant 4

A non-deleted retired partition remains readable.

### Invariant 5

Normal Kafka retention continues on retired partitions.

### Invariant 6

Reactivation preserves the existing partition identity and offset sequence.

### Invariant 7

A stale ResizeEpoch cannot mutate a partition owned by a newer lifecycle epoch.

### Invariant 8

Time passing alone does not mutate lifecycle state; the controller commits every transition.

### Invariant 9

Only one resize may be actively transitioning a topic in v1.

### Invariant 10

Broker lifecycle enforcement is authoritative even if client metadata is stale or bypassed.

---

# 25. End-to-End Example

Start:

```text
Topic orders
P0-P9 ACTIVE
```

Administrator:

```text
resizeTopic("orders", 5)
```

Controller:

```text
ResizeEpoch = 42

P0-P4 ACTIVE
P5-P9 TRANSITIONING
```

Fresh producers:

```text
write P0-P4
```

Stale producers during grace:

```text
may still write P5-P9
```

Consumers:

```text
read P0-P9
```

Grace expires.

Controller fences new writes, resolves accepted work, then commits:

```text
P5-P9 RETIRED
```

Now:

```text
producer writes P0-P4
consumer reads P0-P9
```

Ten days later:

```text
resizeTopic("orders", 8)
```

Controller creates:

```text
ResizeEpoch = 43
```

and reactivates:

```text
P5-P7
```

Final topology:

```text
P0-P7 ACTIVE
P8-P9 RETIRED
```

No messages were republished.

No offsets were rewritten.

---

# 26. Implementation Milestones

## Phase 1 — Design Baseline

- lifecycle model
- ResizeEpoch
- producer semantics
- consumer semantics
- transaction semantics
- failure recovery
- compatibility rules

## Phase 2 — Prototype

- lifecycle-aware metadata model
- `ElasticKafkaProducer` proof of concept
- local admin resize operation
- 10 -> 5 demonstration
- retired partition reads
- 5 -> 10 reactivation

## Phase 3 — Failure Testing

- stale producer
- explicit partition targeting
- controller failover
- broker/leader failure
- transactional producer
- controller quorum interruption
- stale epoch deletion
- sequential resizes

## Phase 4 — Kafka Integration Evaluation

- identify Kafka client changes
- identify Metadata API version changes
- identify controller metadata records
- identify broker ProduceRequest validation points
- identify consumer assignment changes
- evaluate compatibility impact

## Phase 5 — KIP-Style Proposal

Document:

- motivation
- public interfaces
- protocol changes
- configuration
- migration strategy
- compatibility
- rejected alternatives
- failure semantics

---

# 27. Relationship to KIP-694

KIP-694 proposes reducing partitions through partition modes including read-only behavior and delayed deletion.

Kafka Elastic Partitions builds on the same general observation—that a partition can stop accepting writes while remaining readable—but explores a broader reversible lifecycle:

```text
ACTIVE
  ->
TRANSITIONING
  ->
RETIRED
  ->
ACTIVE or DELETED
```

The key architectural emphasis is **reactivation and elastic writable capacity**, rather than treating read-only primarily as a temporary state before deletion.

See:

```text
docs/kip-694-comparison.md
```

for the detailed comparison.

---

# 28. Status

This architecture is currently a **design baseline**.

It is not yet an Apache Kafka feature, accepted KIP, or production implementation.

The next implementation step is to build the lifecycle-aware prototype and demonstrate:

```text
10 -> 5 -> 10
```

while preserving existing partition logs and offsets.
