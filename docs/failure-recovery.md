# Failure Recovery

This document defines how Kafka Elastic Partitions recovers from controller, broker, leadership, quorum, and delayed-work failures.

## Principle

Lifecycle state must be durable and controller-owned.

> **Time passing alone never changes partition lifecycle state.**

The controller must commit every visible transition.

## Controller Failure During Transition

Example:

```text
ResizeEpoch = 42
10 -> 5
P5-P9 = TRANSITIONING
```

If the active controller fails, the replacement controller reads committed metadata and resumes Epoch 42.

It does not create a new epoch.

## Broker / Leader Failure During Transition

Suppose P7 is `TRANSITIONING` and its leader broker fails.

Normal Kafka leader recovery occurs.

The replacement leader learns:

```text
P7 lifecycle = TRANSITIONING
lifecycleEpoch = 42
```

and continues enforcing that state.

Leadership change does not reset lifecycle.

## Failure During Retirement Cutover

Suppose the write fence has begun but `RETIRED` has not yet been committed.

If the broker fails:

```text
metadata still says TRANSITIONING
```

The replacement leader restores the partition and the controller retries the cutover idempotently.

## Controller Failure After Retirement

If committed metadata says:

```text
P7 = RETIRED
deleteDeadline = D
epoch = 42
```

a replacement controller reconstructs that state and continues the retirement lifecycle.

## Broker Failure While Retired

A retired partition remains replicated and readable.

If its leader fails, another replica may become leader.

The new leader continues:

```text
Fetch   -> allowed
Produce -> rejected
```

## Controller Quorum Loss

If the metadata quorum is unavailable when a transition or deletion deadline passes:

```text
partition remains in last committed state
```

Brokers do not independently advance lifecycle state based on local wall-clock time.

Once the quorum recovers, the controller resumes.

## Duplicate Command

Repeated lifecycle commands for the same epoch must be idempotent.

```text
Retire P7, epoch=42
Retire P7, epoch=42
```

The second invocation is already-complete, not a second retirement.

## Stale Command

Example:

```text
Epoch 42: P7 -> RETIRED
Epoch 43: P7 -> ACTIVE
```

A delayed Epoch-42 delete is stale:

```text
42 < 43
```

and must not mutate P7.

## Reactivation vs Deletion Race

Deletion must validate immediately before acting:

```text
partition is still RETIRED
AND
delete command epoch == lifecycle epoch
AND
deadline reached
```

If P7 has been reactivated, deletion fails validation.

## Partial Multi-Partition Progress

A shrink may have some partitions retired while others are still transitioning.

Example:

```text
P5 RETIRED
P6 RETIRED
P7 TRANSITIONING
P8 TRANSITIONING
P9 TRANSITIONING
```

This is valid intermediate state for the same `ResizeEpoch`.

The resize is not complete until:

```text
target active count reached
AND
no TRANSITIONING partitions remain
```

## Invariants

- lifecycle state survives controller failover
- broker leadership changes do not alter lifecycle
- stale epochs cannot mutate newer state
- duplicate same-epoch commands are safe
- deadlines require explicit controller action
- reactivation always fences old pending deletion work
