# Resize Epoch

This document defines the `ResizeEpoch` contract for Kafka Elastic Partitions.

## Purpose

A resize is not atomic. A single request may:

- update topic metadata
- move multiple partitions into `TRANSITIONING`
- wait for a grace period
- fence new writes
- resolve existing transactions
- commit `RETIRED`
- later reactivate or delete retired partitions

`ResizeEpoch` gives every topology-changing operation a durable identity.

```text
ResizeEpoch 42: 10 -> 5
ResizeEpoch 43: 5  -> 8
ResizeEpoch 44: 8  -> 4
```

## Properties

`ResizeEpoch` provides:

1. **Crash recovery** — a replacement controller can resume an unfinished operation.
2. **Fencing** — stale lifecycle commands cannot mutate a newer partition state.
3. **Idempotency** — retrying the same lifecycle command for the same epoch is safe.
4. **Delayed-work protection** — an old delete task cannot delete a partition reactivated by a newer resize.

## Metadata

Conceptually:

```text
TopicResizeState {
    topicId
    resizeEpoch
    previousActiveCount
    targetActiveCount
    status
    startedAt
}
```

Partition lifecycle metadata carries the epoch that currently owns its lifecycle state:

```text
PartitionLifecycleState {
    partitionId
    lifecycleState
    lifecycleEpoch
    transitionDeadline
    retirementDeleteDeadline
}
```

## Monotonicity

Epochs are monotonically increasing per topic.

```text
41 < 42 < 43
```

A command with an epoch lower than the partition's current lifecycle epoch is stale.

Example:

```text
Epoch 42: P7 -> RETIRED
Epoch 43: P7 -> ACTIVE

Delayed command:
DELETE P7, epoch=42

Result:
reject / ignore as stale
```

## Crash Recovery

Suppose:

```text
ResizeEpoch = 42
10 -> 5
P5-P9 = TRANSITIONING
```

and the active controller fails.

The replacement controller reads committed metadata:

```text
resizeEpoch = 42
targetActiveCount = 5
P5-P9 = TRANSITIONING
```

and resumes Epoch 42.

It does **not** create Epoch 43 simply because the controller changed.

## Idempotent Retry

A lifecycle action must be safe to retry:

```text
RetirePartition(P7, epoch=42)
```

If P7 is already `RETIRED` under Epoch 42, the retry returns success/already-complete semantics rather than applying the transition twice.

## Reactivation

Reactivation is a new topology change and therefore receives a new epoch.

```text
Epoch 42: 10 -> 5
P5-P9 -> RETIRED

Epoch 43: 5 -> 8
P5-P7 -> ACTIVE
```

Reactivation cancels the old retirement lifecycle for those partitions.

## Epoch vs Leader Epoch

`ResizeEpoch` is separate from Kafka's partition leader epoch.

```text
Leader epoch:
tracks partition leadership changes

ResizeEpoch:
tracks topic writable-topology changes
```

A broker leadership change does not create a new `ResizeEpoch`.

## v1 Invariants

- only the current lifecycle epoch may mutate partition lifecycle state
- duplicate commands for the same epoch are idempotent
- stale commands from older epochs are rejected
- controller failover does not increment the epoch
- a new accepted resize request creates a new epoch
- only one resize is actively transitioning a topic in v1
