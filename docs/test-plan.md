# Test Plan

This document defines the initial validation plan for Kafka Elastic Partitions.

The first proof of concept should demonstrate:

```text
10 -> 5 -> 10
```

without republishing historical records or rewriting partition offsets.

# 1. Baseline

## Test 1.1 — Normal topic behavior

Create a topic with 10 partitions.

Verify:

- P0-P9 writable
- P0-P9 readable
- normal keyed and non-keyed production works
- normal consumer groups work

# 2. Shrink Happy Path

## Test 2.1 — 10 -> 5

Request:

```text
10 -> 5
```

Verify:

```text
P0-P4 ACTIVE
P5-P9 TRANSITIONING
```

Then after grace/cutover:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

Verify no historical records moved.

## Test 2.2 — Producer metadata convergence

Use a lifecycle-aware producer.

Verify that after metadata refresh it automatically selects only P0-P4.

## Test 2.3 — Stale producer during grace

Keep producer metadata stale.

Verify P5-P9 writes are accepted during `TRANSITIONING` grace.

## Test 2.4 — Stale producer after retirement

Attempt to write to P7 after it becomes `RETIRED`.

Expected:

```text
broker rejects
producer refreshes metadata
```

## Test 2.5 — Explicit retired partition

Send explicitly to:

```text
partition=7
```

after retirement.

Expected:

```text
failure
no silent redirect
```

# 3. Consumer Tests

## Test 3.1 — Existing consumer continues

Existing group consumes P0-P9 before shrink.

After P5-P9 retire, verify it can continue consuming retained data from P5-P9.

## Test 3.2 — New consumer after shrink

Create a new consumer group after retirement.

Verify it may read P5-P9 while those partitions remain present.

Verify it is not automatically added to the original drain-tracking set.

## Test 3.3 — Drain status

Set:

```text
P7 LEO = N
```

Verify a group is marked drained only when committed offset reaches the current required position.

## Test 3.4 — Offset retention protection

Make a drain-tracked group inactive.

Verify relevant retirement offset state is not silently lost through ordinary offset expiration.

# 4. Reactivation

## Test 4.1 — 5 -> 10 before deletion

Reactivate P5-P9.

Verify:

- deletion workflow canceled
- drain tracking cleared
- P5-P9 writable again
- offsets continue rather than reset

## Test 4.2 — Partial expansion

From 5 active with P5-P9 retired:

```text
5 -> 8
```

Expected:

```text
P0-P7 ACTIVE
P8-P9 RETIRED
```

## Test 4.3 — Expand beyond existing physical count

From 5 active and 10 physical:

```text
5 -> 12
```

Expected:

```text
P5-P9 reactivated
P10-P11 created
```

# 5. Sequential Resize

## Test 5.1 — 10 -> 5 -> 3

Verify second shrink begins only after first resize is complete.

## Test 5.2 — Resize while transition active

Issue a second resize while P5-P9 are still `TRANSITIONING`.

Expected:

```text
RESIZE_IN_PROGRESS
```

# 6. Transactions

## Test 6.1 — Existing transaction

Start transaction involving P7 before P7 transitions.

Verify commit/abort can finish.

## Test 6.2 — New transaction participation

After P7 becomes `TRANSITIONING`, attempt to newly add P7 to a transaction.

Expected:

```text
rejected
```

## Test 6.3 — Long-running transaction

Verify normal Kafka transaction timeout/abort semantics eventually allow retirement to proceed.

# 7. Failure Recovery

## Test 7.1 — Controller failover during transition

Fail active controller while resize is in progress.

Expected:

- replacement resumes same ResizeEpoch
- no duplicate new epoch
- no lifecycle reset

## Test 7.2 — Leader failure during transition

Fail leader broker for P7.

Expected:

- new leader restores `TRANSITIONING`
- resize continues

## Test 7.3 — Failure during retirement cutover

Crash leader after fencing writes but before `RETIRED` is committed.

Expected:

- metadata remains `TRANSITIONING`
- retry is idempotent
- no lost accepted writes

## Test 7.4 — Controller quorum unavailable

Let transition deadline pass without controller quorum.

Expected:

```text
no autonomous broker transition
```

Lifecycle advances only after quorum recovery.

# 8. Epoch Fencing

## Test 8.1 — Stale delete after reactivation

```text
Epoch 42: P7 RETIRED
Epoch 43: P7 ACTIVE
```

Execute delayed Epoch-42 delete.

Expected:

```text
rejected as stale
```

## Test 8.2 — Duplicate same-epoch retire

Replay:

```text
Retire P7, epoch=42
```

Expected idempotent result.

# 9. Retention and Deletion

## Test 9.1 — Normal topic retention

Retire P7 with topic `retention.ms` shorter than retirement delete delay.

Verify old P7 segments age out normally.

## Test 9.2 — Hard retirement deadline

Let delete deadline expire.

Verify retired partition is physically removed even if a drain-tracked consumer remains behind.

## Test 9.3 — Extend deadline

Administrator extends retirement deadline before expiration.

Verify deletion uses the new deadline.

# 10. Compatibility

## Test 10.1 — New client, old broker

Lifecycle-aware client connects to broker without elastic support.

Expected:

```text
all partitions treated as ACTIVE
```

## Test 10.2 — Old client, normal new-broker topic

Verify ordinary topic remains usable.

## Test 10.3 — Old client, elastic topic

Verify broker/client fails closed rather than silently misinterpreting lifecycle metadata.

# 11. Success Criteria

The v1 prototype is successful when it demonstrates:

```text
10 ACTIVE
   ->
5 ACTIVE + 5 RETIRED
   ->
10 ACTIVE
```

with:

- zero historical record republishing
- zero resize-driven offset rewriting
- retired partitions readable
- retired partitions not writable
- offsets preserved on reactivation
- stale resize operations fenced
- controller/broker failures recoverable
