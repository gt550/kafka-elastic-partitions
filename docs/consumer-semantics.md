# Consumer Semantics

This document defines consumer behavior for Kafka Elastic Partitions.

## Principle

Shrinking changes the writable topology, not the readable historical topology.

Consumers may read every non-deleted physical partition.

```text
ACTIVE          -> readable
TRANSITIONING   -> readable
RETIRED         -> readable
```

## Example

After:

```text
10 -> 5
```

the topic may have:

```text
Writable:
P0-P4

Readable:
P0-P9
```

Consumer-group assignment operates over the readable partition set.

## Existing Consumer Groups

Existing consumer groups continue consuming P5-P9 after those partitions become `TRANSITIONING` and later `RETIRED`.

No immediate rebalance is required merely because a partition becomes read-only.

A rebalance may still occur for normal Kafka reasons.

## New Consumers After Resize

A consumer group created after the resize may still read retired partitions while those partitions remain physically present and their records remain retained.

However, it is not automatically included in the original retirement's drain-tracking set.

## Drain Tracking

Drain tracking is operational metadata, not a prerequisite for basic readability.

A group may be initially drain-tracked if, before the resize, it:

- had committed offsets for the topic, or
- had an active subscription/assignment involving the topic

Administrators may add or remove drain-tracked groups.

## Drain Progress

Because a retired partition no longer accepts application writes, its current Log End Offset is sufficient as the current drain target.

Example:

```text
P7 LEO = 850001

pricing-service committed = 850001 -> drained
audit-service   committed = 820000 -> lagging
```

No separate retirement-end offset is required in v1.

If P7 is reactivated, the old retirement/drain workflow is canceled.

## Offset Retention

Kafka may normally expire committed offsets for inactive consumer groups.

For a drain-tracked group and retired partition, the resize subsystem should protect the relevant committed offset while the retirement lifecycle remains active.

Protection ends when:

- the partition is reactivated
- the partition is deleted
- the administrator removes the group from drain tracking

## Hard Deletion Deadline

Drain tracking does not keep a retired partition forever.

Example:

```text
retired.partition.delete.delay.ms = 30 days
```

At the deadline the partition is deleted unless:

- it was reactivated, or
- an administrator explicitly extended the deadline

A lagging drain-tracked group is therefore an operational alert, not an indefinite deletion veto.

## Normal Topic Retention

Retired partitions continue to obey normal topic retention.

A consumer may lose access to old records because of `retention.ms` before the retired partition itself reaches its deletion deadline.

This is consistent with normal Kafka behavior.

## Manual Assignment

Consumers using explicit partition assignment may continue reading a retired partition while it exists.

After deletion, attempts to fetch that partition fail according to normal missing-partition semantics.

## Invariants

- `ACTIVE`, `TRANSITIONING`, and `RETIRED` remain readable
- new consumer groups may read retired partitions
- post-resize groups do not automatically control the old retirement lifecycle
- drain tracking uses committed offsets
- reactivation cancels old drain tracking
- normal Kafka retention remains authoritative for record lifetime
