# Comparison with KIP-694

This document compares Kafka Elastic Partitions with Apache Kafka **KIP-694: Support Reducing Partitions for Topics**.

Reference:

https://cwiki.apache.org/confluence/display/KAFKA/KIP-694%3A+Support+Reducing+Partitions+for+Topics

As of September 2026, KIP-694 is still marked **Under Discussion**.

## Shared Motivation

Both designs address the same operational problem:

- Kafka partitions can be increased but are difficult to reduce
- clusters can accumulate unnecessary partitions
- excess partitions increase operational and resource overhead
- physically merging existing logs is undesirable

Both designs use the idea that a partition may stop accepting new writes while remaining readable.

## KIP-694 Direction

KIP-694 proposes partition modes including:

```text
ReadWrite
ReadOnly
None
```

Its workflow is primarily:

```text
ReadWrite
   ->
ReadOnly
   ->
None / delete
```

It also proposes:

- partition mode in metadata
- producer and consumer filtering based on mode
- delayed deletion
- client compatibility constraints for older metadata versions
- topic-command support for reducing partition count

## Kafka Elastic Partitions Direction

This project defines:

```text
ACTIVE
   ->
TRANSITIONING
   ->
RETIRED
   ->
ACTIVE or DELETED
```

The main difference is that `RETIRED` is a first-class reversible lifecycle state rather than primarily a temporary stop before deletion.

## Comparison

| Area | KIP-694 | Kafka Elastic Partitions |
|---|---|---|
| Core reduced state | `ReadOnly` | `RETIRED` |
| Grace before read-only | Not the central model | Explicit `TRANSITIONING` grace |
| Re-expansion | Not a primary lifecycle concept | Reactivate retired partitions |
| Resize identity | Deletion workflow | Monotonic `ResizeEpoch` |
| Stale operation fencing | Not the central abstraction | Explicit epoch fencing |
| Consumer visibility | Read-only remains readable | Retired remains readable |
| Consumer tracking | Delayed deletion | Drain-tracked groups for operations |
| Hard delete deadline | Delayed deletion | Configurable retirement deadline |
| Sequential shrink/expand | Not primary focus | Explicit v1 behavior |
| Retention while reduced | Existing log behavior | Explicitly unchanged |
| In-progress concurrency | Warning that topic is under changes | One active resize per topic |
| Prototype goal | Partition deletion support | Elastic writable topology |

## Why Add `TRANSITIONING`?

A refreshed producer should stop selecting retiring partitions immediately, while a stale upgraded producer may continue using cached metadata for a short period.

`TRANSITIONING` makes that convergence explicit:

```text
ACTIVE
   ->
TRANSITIONING
   ->
RETIRED
```

The broker remains authoritative at cutover.

## Why Add `ResizeEpoch`?

Long-lived retirement plus reactivation introduces a race that a deletion-only model does not emphasize.

Example:

```text
Epoch 42: P7 retired
Epoch 43: P7 reactivated
```

A delayed delete from Epoch 42 must never remove the now-active P7.

`ResizeEpoch` provides the fencing identity for that rule.

## Why Reactivate Instead of Recreate?

If P7 still physically exists:

```text
RETIRED -> ACTIVE
```

preserves:

- partition identity
- existing replicas/logs
- offset sequence
- historical data still retained

No republish or rebuild is necessary.

## Compatibility

Both designs recognize that older clients cannot safely interpret new partition lifecycle metadata once the feature is active.

Kafka Elastic Partitions therefore adopts an explicit client upgrade boundary for elastically resized topics.

## Positioning

This project should be described as:

> an independent design and prototype exploration inspired by the same partition-reduction problem as KIP-694, with additional emphasis on reversible retirement, reactivation, epoch fencing, and elastic writable capacity.

It should **not** claim that KIP-694 is accepted, replaced, or obsolete.
