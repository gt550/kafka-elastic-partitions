# Partition Lifecycle State Machine

This document defines the v1 lifecycle state machine for **Kafka Elastic Partitions**.

The state machine governs how a physical Kafka partition moves between writable, transitioning, retired, reactivated, and deleted states during topic resize operations.

The design principle is:

> **A partition changes lifecycle state; its historical log is not merged or republished.**

---

# 1. States

A physical partition has one of three visible lifecycle states:

```text
ACTIVE
TRANSITIONING
RETIRED
```

`DELETED` is a terminal physical outcome rather than a normal metadata state because the partition no longer exists.

## 1.1 ACTIVE

The partition behaves like a normal Kafka partition.

```text
Automatic producer selection: YES
Explicit application produce: YES
Consumer fetch:               YES
Replication:                  YES
New transaction participation:YES
```

## 1.2 TRANSITIONING

The partition is leaving the writable topology as part of a shrink operation.

```text
Automatic producer selection: NO
Stale producer writes:        YES, during transition grace
Consumer fetch:               YES
Replication:                  YES
New transaction participation:NO
Existing transaction finish:  YES
```

The partition remains physically present and readable.

## 1.3 RETIRED

The partition remains physically present but is no longer writable for application traffic.

```text
Automatic producer selection: NO
Application produce:          NO
Consumer fetch:               YES
Replication:                  YES
Reactivation:                 YES
```

Normal topic retention continues while the partition is retired.

## 1.4 DELETED

The partition has been physically removed.

```text
Produce: NO
Fetch:   NO
Metadata presence: NO
```

Deletion is terminal for that physical partition instance.

---

# 2. State Diagram

```text
                 shrink selects partition
                         |
                         v
+---------+        +----------------+
| ACTIVE  | -----> | TRANSITIONING  |
+---------+        +----------------+
    ^                      |
    |                      | grace expires
    |                      | accepted work settles
    |                      v
    |               +-------------+
    +---------------|   RETIRED   |
      reactivation  +-------------+
                           |
                           | retirement deadline
                           | reached
                           v
                       [DELETED]
```

v1 deliberately does **not** allow:

```text
TRANSITIONING -> ACTIVE
```

as a cancellation mechanism.

An in-progress shrink must finish before a new resize operation begins.

---

# 3. Allowed Transitions

The following transitions are valid in v1:

```text
ACTIVE        -> TRANSITIONING
TRANSITIONING -> RETIRED
RETIRED       -> ACTIVE
RETIRED       -> DELETED
```

No other visible lifecycle transitions are allowed.

---

# 4. ACTIVE -> TRANSITIONING

## Trigger

A shrink operation reduces the topic's active partition count.

Example:

```text
currentActiveCount = 10
targetActiveCount  = 5
```

Partitions `P5-P9` are selected to leave the writable set.

## Controller actions

The controller:

```text
1. creates a new ResizeEpoch
2. records target active partition count
3. marks selected partitions TRANSITIONING
4. records transition deadline
5. publishes updated lifecycle metadata
```

Conceptually:

```text
P7 {
    state = TRANSITIONING
    lifecycleEpoch = 42
    transitionDeadline = T
}
```

## Producer behavior

A lifecycle-aware producer that refreshes metadata stops selecting the transitioning partition for automatic writes.

A producer with stale cached metadata may still attempt to write to it during the grace period.

## Consumer behavior

Consumers continue reading the partition normally.

## Transaction behavior

Once the partition becomes `TRANSITIONING`:

```text
new transaction may add partition: NO
already-participating transaction: YES, may finish
```

---

# 5. TRANSITIONING -> RETIRED

## Trigger

The transition grace period has expired.

Expiration of the timer alone is **not sufficient** to change lifecycle state.

The controller must successfully complete the retirement cutover.

## Retirement cutover

```text
1. fence new application writes
2. finish already accepted writes
3. resolve existing transactional participation
4. ensure partition state is recoverable/stable
5. commit RETIRED lifecycle metadata
```

The broker may use an internal quiescing phase while this work completes.

`QUIESCING` is not exposed as a public lifecycle state in v1.

## Result

```text
P7 {
    state = RETIRED
    lifecycleEpoch = 42
    retirementDeleteDeadline = D
}
```

The partition is now:

```text
readable  = YES
writable  = NO
```

---

# 6. RETIRED -> ACTIVE

## Trigger

A later expansion requests an active partition count that includes the retired partition.

Example:

```text
current active = 5
target active  = 8
```

If `P5-P9` are retired, then `P5-P7` return to `ACTIVE`.

## Controller actions

A new `ResizeEpoch` is created.

For each partition being reactivated:

```text
1. validate partition still exists
2. validate lifecycle is RETIRED
3. move lifecycle ownership to new ResizeEpoch
4. cancel pending deletion
5. clear old retirement/drain-tracking state
6. mark ACTIVE
```

## Offset behavior

The partition keeps its identity and continues its existing offset sequence.

No offset reset occurs.

---

# 7. RETIRED -> DELETED

## Trigger

The configured retirement deletion deadline is reached.

Example:

```text
retired.partition.delete.delay.ms = 30 days
```

## Preconditions

Before deleting, the controller validates:

```text
partition state == RETIRED
deletion action epoch == current lifecycle epoch
deadline is reached
partition has not been reactivated
```

Drain-tracked consumer lag does not automatically extend the deadline.

## Controller actions

Conceptually:

```text
1. validate deletion for current epoch
2. remove partition replicas
3. remove partition metadata
4. remove lifecycle metadata
5. clear retirement/drain metadata
```

After completion, the partition no longer exists.

---

# 8. Invalid / Rejected Transitions

## 8.1 TRANSITIONING -> ACTIVE

Not supported in v1.

Allowing cancellation of an in-progress resize introduces races involving:

- producer metadata already propagated
- transition deadlines
- transaction fencing
- controller failover
- lifecycle epoch ownership

A topic must finish its current resize before another resize starts.

## 8.2 ACTIVE -> RETIRED

Not allowed directly.

The `TRANSITIONING` phase is required to provide metadata convergence and graceful cutover.

A configuration may set:

```text
partition.resize.transition.grace.ms = 0
```

but the logical transition still remains:

```text
ACTIVE -> TRANSITIONING -> RETIRED
```

## 8.3 TRANSITIONING -> DELETED

Not allowed.

A partition must first become `RETIRED`.

## 8.4 DELETED -> ACTIVE

Not allowed for the same physical partition instance.

If expansion later needs additional partitions beyond surviving physical IDs, Kafka creates new partitions.

---

# 9. Resize Operation State

Partition lifecycle and resize-operation lifecycle are related but distinct.

A topic resize can conceptually have:

```text
IN_PROGRESS
COMPLETE
FAILED
```

v1 considers the resize complete when:

```text
currentActivePartitionCount == targetActivePartitionCount
AND
no partitions are TRANSITIONING
```

Retired partitions awaiting deletion do **not** keep the resize operation active.

---

# 10. One Active Resize per Topic

v1 allows only one in-progress resize operation for a topic.

If:

```text
ResizeEpoch 42
10 -> 5
P5-P9 TRANSITIONING
```

and another resize request arrives, the request fails with a resize-in-progress condition.

Conceptually:

```text
RESIZE_IN_PROGRESS
currentResizeEpoch = 42
targetActiveCount = 5
```

Once no partitions are `TRANSITIONING`, another resize may begin.

---

# 11. Sequential Resize Examples

## Example A: 10 -> 5 -> 8

Initial:

```text
P0-P9 ACTIVE
```

Epoch 42:

```text
10 -> 5
P0-P4 ACTIVE
P5-P9 TRANSITIONING
```

After cutover:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

Epoch 43:

```text
5 -> 8
```

Result:

```text
P0-P7 ACTIVE
P8-P9 RETIRED
```

## Example B: 10 -> 5 -> 3

After Epoch 42:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

Epoch 43:

```text
5 -> 3
```

Transitions:

```text
P3-P4 ACTIVE -> TRANSITIONING
```

Final:

```text
P0-P2 ACTIVE
P3-P9 RETIRED
```

## Example C: 10 -> 5 -> 12

After shrink:

```text
P0-P4 ACTIVE
P5-P9 RETIRED
```

Expansion:

```text
targetActiveCount = 12
```

Kafka:

```text
P5-P9 RETIRED -> ACTIVE
P10-P11 newly created
```

Final:

```text
P0-P11 ACTIVE
```

---

# 12. Producer Enforcement by State

```text
+----------------+--------------------------+----------------------+
| State          | Auto partition selection | Broker Produce       |
+----------------+--------------------------+----------------------+
| ACTIVE         | YES                      | ACCEPT               |
| TRANSITIONING  | NO                       | ACCEPT during grace  |
| RETIRED        | NO                       | REJECT               |
| DELETED        | N/A                      | UNKNOWN PARTITION    |
+----------------+--------------------------+----------------------+
```

The producer view is advisory for graceful convergence.

The broker view is authoritative for correctness.

---

# 13. Consumer Visibility by State

```text
+----------------+------------------+
| State          | Consumer Fetch   |
+----------------+------------------+
| ACTIVE         | YES              |
| TRANSITIONING  | YES              |
| RETIRED        | YES              |
| DELETED        | NO               |
+----------------+------------------+
```

Consumer group assignment operates over all readable physical partitions.

---

# 14. Transaction Rules by State

```text
+----------------+--------------------------+--------------------------+
| State          | New Transaction Join     | Existing Transaction     |
+----------------+--------------------------+--------------------------+
| ACTIVE         | YES                      | YES                      |
| TRANSITIONING  | NO                       | COMMIT / ABORT allowed   |
| RETIRED        | NO                       | must already be resolved |
| DELETED        | NO                       | N/A                      |
+----------------+--------------------------+--------------------------+
```

---

# 15. Retention Rules by State

Normal Kafka retention applies in every non-deleted state.

```text
ACTIVE         -> normal retention
TRANSITIONING  -> normal retention
RETIRED        -> normal retention
```

Retirement does not freeze log segments.

---

# 16. Failure Rules

Lifecycle transitions are committed by the controller through durable metadata.

A broker does not independently move:

```text
TRANSITIONING -> RETIRED
```

because a wall-clock deadline passed.

If the controller quorum is unavailable, lifecycle remains at the last committed state.

## 16.1 Controller failure

The replacement controller resumes the current `ResizeEpoch`.

## 16.2 Broker failure

The new partition leader restores lifecycle state from cluster metadata.

## 16.3 Duplicate lifecycle command

If:

```text
Retire P7, ResizeEpoch 42
```

is replayed after successful completion for the same epoch, it is treated idempotently.

## 16.4 Stale lifecycle command

If:

```text
P7 lifecycleEpoch = 43
```

then a command carrying:

```text
epoch = 42
```

cannot mutate P7.

---

# 17. Lifecycle Metadata Invariants

Every existing partition must have exactly one visible lifecycle state.

```text
ACTIVE XOR TRANSITIONING XOR RETIRED
```

A partition may not be simultaneously active and retired.

---

# 18. Core State-Machine Invariants

1. A `RETIRED` partition never accepts new application writes.
2. A non-deleted `RETIRED` partition remains readable.
3. A partition cannot skip `TRANSITIONING` during shrink.
4. Only the current lifecycle epoch may mutate partition state.
5. Time passing alone does not commit a lifecycle transition.
6. Reactivation cancels the old retirement/deletion lifecycle.
7. Normal Kafka retention continues in `RETIRED`.
8. A resize is complete once its target active count is reached and no partitions remain `TRANSITIONING`.
9. Only one resize may be in progress per topic in v1.
10. Deletion is permitted only from `RETIRED`.

---

# 19. Summary

The v1 lifecycle is intentionally small:

```text
ACTIVE
  |
  v
TRANSITIONING
  |
  v
RETIRED
  |      \
  |       \
  v        v
ACTIVE   DELETED
```

This state machine enables elastic writable capacity while preserving Kafka's existing physical partition logs, offsets, replication model, and normal retention behavior.

Its main simplifying constraint is:

> **v1 serializes topology changes: one active resize per topic, with no cancellation while partitions are transitioning.**
