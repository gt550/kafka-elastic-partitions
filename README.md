Kafka Elastic Partitions
> A design and prototype for reversible Kafka partition resizing without republishing historical records.
Kafka supports increasing a topic's partition count, but reducing the count is not part of the accepted partition-management API. Kafka Elastic Partitions explores a lifecycle-based approach that lets a topic shrink when throughput falls and later expand again while preserving existing partition logs and offsets.
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
`A` = `ACTIVE`
`T` = `TRANSITIONING`
`R` = `RETIRED`
A retired partition remains readable, keeps its existing log and offsets, and can be reactivated before its deletion deadline.
---
Why?
Kafka topics are often sized for peak throughput. When traffic later decreases, the extra partitions remain and continue adding broker, filesystem, replication, metadata, and operational overhead.
Shrinking is difficult because partitions are independent append-only logs. Merging them would require Kafka to redefine offsets, consumer state, ordering, and key-to-partition behavior.
This project avoids that problem:
```text
Do not merge old logs.
Change which existing partitions are writable.
```
Historical records stay where they are.
---
v1 Goals
The first version is designed to:
reduce the writable partition count without republishing historical records
preserve physical partition logs and offsets
allow consumers to continue reading retired partitions
allow retired partitions to be reactivated during a later expansion
provide a configurable producer metadata-convergence grace period
enforce retirement at the broker
keep normal Kafka record-retention behavior unchanged
make resize operations recoverable after controller or broker failures
fence stale lifecycle operations using a `ResizeEpoch`
support sequential shrink and expansion operations
keep normal Kafka producer/consumer APIs as the long-term application interface
---
Non-Goals for v1
v1 intentionally does not attempt to:
preserve key ordering across a partition-count change
merge partition logs
rewrite offsets
republish historical records
run multiple simultaneous resize operations on the same topic
cancel an in-progress transition
transparently support old Kafka clients on a topic once elastic resizing is activated
Changing partition count can already change key-to-partition mapping during normal Kafka expansion. Elastic shrink follows the same principle rather than introducing a stronger cross-partition ordering guarantee.
---
Partition Lifecycle
`ACTIVE`
Normal Kafka behavior.
```text
Produce: YES
Fetch:   YES
```
The partition participates in automatic producer-side partition selection.
`TRANSITIONING`
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
`RETIRED`
The partition is read-only for application traffic.
```text
Produce: NO
Fetch:   YES
```
The broker rejects writes from:
stale producers
custom producers
applications explicitly targeting the retired partition
Consumers may continue reading retained records.
`DELETED`
The retirement deadline has expired and the physical partition is removed.
```text
Produce: NO
Fetch:   NO
```
---
Shrink: 10 -> 5
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
Expansion and Reactivation: 5 -> 10
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
Resize Epoch
Every topology-changing operation receives a monotonically increasing `ResizeEpoch`.
```text
ResizeEpoch 42: 10 -> 5
ResizeEpoch 43: 5  -> 8
ResizeEpoch 44: 8  -> 4
ResizeEpoch 45: 4  -> 12
```
The epoch provides:
Crash recovery — a new controller can resume an incomplete resize.
Fencing — stale lifecycle commands cannot overwrite newer state.
Idempotency — retrying an operation for the same epoch is safe.
Delayed-work protection — an old delete task cannot delete a partition reactivated by a newer epoch.
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
Producer Semantics
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
The transition grace period handles stale cached metadata in upgraded clients. It is not a correctness mechanism.
---
Consumer Semantics
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
Consumer Drain Tracking
Consumer groups that were already relevant to the topic before the resize can be tracked for operational visibility.
A group may be automatically drain-tracked if it:
had committed offsets for the topic, or
had an active subscription/assignment involving the topic
Administrators may add or remove drain-tracked groups.
Example:
```text
P7 Log End Offset = 850001

pricing-service   committed = 850001  -> drained
audit-service     committed = 820000  -> lagging
```
New consumer groups created after the resize may still read retired partitions, but they are not automatically part of the original retirement's drain tracking.
Drain tracking does not extend the hard deletion deadline unless an administrator explicitly extends that deadline.
---
Retirement and Deletion
A retired partition remains available for a configurable period:
```text
retired.partition.delete.delay.ms
```
Example:
```text
Day 0   P7 -> RETIRED
Day 30  P7 -> DELETED
```
The deadline is hard unless:
the partition is reactivated, or
an administrator explicitly extends the deadline
This gives operators a predictable upper bound on retired-partition resource usage.
---
Normal Kafka Retention Still Applies
Retirement does not freeze record retention.
If:
```text
topic retention.ms             = 7 days
retired partition delete delay = 30 days
```
the partition may exist for 30 days, while old log segments continue expiring according to the normal 7-day topic retention policy.
> Retirement preserves the partition lifecycle, not records beyond normal Kafka retention.
---
Transaction Semantics
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
Failure Recovery
Elastic resizing must survive normal Kafka failures.
Controller failure
Resize state is persisted through the metadata quorum. A replacement controller resumes the existing `ResizeEpoch` rather than starting a new one.
Broker / leader failure
Lifecycle state belongs to cluster metadata, not a broker process. A new partition leader restores the existing lifecycle state.
Controller quorum unavailable
Time passing alone does not change lifecycle state. If a transition deadline expires while the metadata quorum is unavailable, the partition remains in its last committed state until the controller can safely resume.
Stale lifecycle command
If:
```text
Epoch 42: P7 -> RETIRED
Epoch 43: P7 -> ACTIVE
```
a delayed Epoch-42 delete request is stale and must be rejected.
---
Resize Concurrency
v1 allows one active resize operation per topic at a time.
A resize is complete when:
```text
currentActivePartitionCount == targetActivePartitionCount

AND

no partitions are TRANSITIONING
```
Retired partitions awaiting their deletion deadline do not keep the resize operation open.
This allows sequential operations such as:
```text
Epoch 42: 10 -> 5
Epoch 43: 5  -> 3
Epoch 44: 3  -> 8
```
Canceling or superseding an in-progress transition is outside the v1 scope.
---
Metadata and Backward Compatibility
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
Old-client boundary
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
Prototype Strategy
The long-term goal is for application code to remain unchanged:
```java
KafkaProducer<String, Event> producer =
    new KafkaProducer<>(props);
```
Elastic behavior should ultimately live inside Kafka's normal metadata and partition-selection implementation.
For the initial proof of concept, an `ElasticKafkaProducer` wrapper may be used to demonstrate lifecycle-aware partition selection before changing Kafka client internals.
```text
Application
    |
    v
ElasticKafkaProducer       <- prototype only
    |
    v
KafkaProducer
    |
    v
Kafka Broker
```
---
Proposed v1 Configuration
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
Relationship to KIP-694
Apache Kafka already has KIP-694: Support Reducing Partitions for Topics. It proposes partition modes including `ReadWrite` and `ReadOnly`, followed by delayed deletion.
As of September 2026, KIP-694 remains Under Discussion.
Kafka Elastic Partitions explores a related but distinct design centered on reversible partition retirement.
Area	KIP-694 direction	Kafka Elastic Partitions
Reduced partition	Read-only before deletion	Long-lived `RETIRED` lifecycle
Re-expansion	Not the primary lifecycle model	Reactivate existing retired partitions
Producer convergence	Partition-mode-aware client behavior	Explicit `TRANSITIONING` grace + broker fence
Resize identity	Deletion-oriented workflow	`ResizeEpoch` fencing and recovery
Consumer visibility	Continued reads during deletion delay	Explicit drain tracking
Sequential resize	Not the primary focus	Shrink / shrink / expand lifecycle
This repository is an independent exploration, not a replacement claim for KIP-694.
---
Planned Proof of Concept
The first prototype will demonstrate:
```text
1. Create a topic with 10 partitions
2. Produce continuously
3. Request 10 -> 5
4. Move P5-P9 to TRANSITIONING
5. Allow stale writes during transition grace
6. Retire P5-P9
7. Reject new writes to retired partitions
8. Continue consuming P5-P9
9. Request 5 -> 10
10. Reactivate P5-P9
11. Continue their existing offset sequences
```
Additional tests will cover:
stale producer metadata
explicit produce to a retired partition
consumer drain tracking
a consumer created after resize
normal retention on retired partitions
transactional producers
controller failover
partition-leader failure
controller-quorum interruption
stale ResizeEpoch commands
retired-partition deletion
repeated sequential resizes
---
Project Status
Current status: Design / pre-prototype
The v1 lifecycle, producer behavior, consumer behavior, resize fencing, retention semantics, failure recovery, concurrency model, and compatibility boundary are defined.
Next milestones:
```text
[ ] Add detailed design documents
[ ] Implement lifecycle metadata prototype
[ ] Implement ElasticKafkaProducer proof of concept
[ ] Demonstrate 10 -> 5
[ ] Demonstrate retired-partition consumption
[ ] Demonstrate 5 -> 10 reactivation
[ ] Add failure-recovery tests
[ ] Evaluate Kafka client/broker integration
[ ] Draft KIP-style proposal
```
---
Planned Repository Layout
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
│   └── test-plan.md
└── prototype/
```
---
References
Apache Kafka — KIP-694: Support Reducing Partitions for Topics  
https://cwiki.apache.org/confluence/display/KAFKA/KIP-694%3A+Support+Reducing+Partitions+for+Topics
Apache Kafka — KIP-195: AdminClient.createPartitions  
https://cwiki.apache.org/confluence/spaces/KAFKA/pages/73637005/KIP-195%2BAdminClient.createPartitions
Apache Kafka — Protocol documentation  
https://kafka.apache.org/protocol.html
---
Disclaimer
This repository is an independent design and prototype exploration. It is not an Apache Kafka feature, an accepted Kafka Improvement Proposal, or an Apache Software Foundation project.