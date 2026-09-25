# Producer Semantics

This document defines producer behavior for Kafka Elastic Partitions.

## Principle

Kafka producer partition selection happens client-side.

Elastic resizing changes the set of partitions that a lifecycle-aware producer considers writable.

```text
writablePartitions =
    partitions where lifecycleState == ACTIVE
```

Broker enforcement remains authoritative.

## State Behavior

| Partition state | Automatic selection | Broker accepts application produce |
|---|---:|---:|
| `ACTIVE` | Yes | Yes |
| `TRANSITIONING` | No after metadata refresh | Yes during grace |
| `RETIRED` | No | No |
| Deleted | N/A | No |

## Shrink Example

Before:

```text
P0-P9 ACTIVE
```

Shrink:

```text
10 -> 5
```

Controller publishes:

```text
P0-P4 ACTIVE
P5-P9 TRANSITIONING
```

A producer that refreshes metadata immediately begins partitioning only across P0-P4.

A producer with stale cached metadata may still choose P5-P9 during the configured transition grace period.

## Transition Grace

Provisional setting:

```text
partition.resize.transition.grace.ms
```

The grace period:

- is configurable independently of producer `metadata.max.age.ms`
- allows stale upgraded producers to converge naturally
- is not relied upon for correctness
- ends when the controller begins retirement cutover

## Broker Fence

After a partition becomes `RETIRED`:

```text
ProduceRequest(P7)
    ->
broker lifecycle check
    ->
reject
```

This protects against:

- stale metadata
- custom partitioners
- explicitly targeted partition writes
- non-standard producer implementations

## Metadata Refresh After Rejection

For an automatically selected partition:

```text
produce rejected because partition no longer writable
        |
        v
refresh metadata
        |
        v
recompute destination using ACTIVE set
        |
        v
retry according to normal producer retry semantics
```

The implementation must preserve Kafka's existing idempotence and retry guarantees.

## Explicit Partition Selection

If application code specifies:

```java
new ProducerRecord<>("orders", 7, key, value)
```

and P7 is retired, Kafka must **not** silently redirect the record.

The send fails with a lifecycle/not-writable error.

## Custom Partitioners

Custom partitioners remain allowed, but:

- lifecycle-aware metadata should expose writable partitions
- the broker still rejects writes to retired partitions
- an application that explicitly insists on a retired partition receives an error

## Idempotent Producers

Elastic resize must not weaken normal idempotent-producer guarantees.

A record accepted before the retirement fence remains governed by ordinary Kafka producer sequencing and replication semantics.

A record submitted after the fence is rejected before it becomes a valid application append to the retired partition.

## Long-Term Client Design

The production goal is:

```java
KafkaProducer<K,V>
```

with lifecycle-aware metadata integrated into the normal Kafka client.

A prototype may temporarily use:

```text
ElasticKafkaProducer
```

to demonstrate writable-partition filtering, but applications should not need a proprietary producer type in the final design.

## Invariants

- automatic producer selection uses only `ACTIVE`
- broker lifecycle enforcement is authoritative
- `TRANSITIONING` accepts stale application writes only during grace
- `RETIRED` accepts no new application writes
- explicit partition requests are never silently redirected
