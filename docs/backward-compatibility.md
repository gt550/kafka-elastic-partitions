# Backward Compatibility

This document defines the v1 compatibility boundary for Kafka Elastic Partitions.

## Core Problem

After shrink, producer and consumer semantics differ:

```text
Producer writable set:
ACTIVE only

Consumer readable set:
ACTIVE + TRANSITIONING + RETIRED
```

An old Kafka client does not understand lifecycle state.

If an old producer sees all physical partitions, it may continue treating retired partitions as writable.

If an old consumer sees only writable partitions, it cannot read retired partitions.

Therefore there is no safe generic old-client interpretation once elastic lifecycle states are active.

## Metadata Versioning

Elastic partitions require a newer metadata representation containing partition lifecycle state.

Conceptually:

```text
P0 ACTIVE
P1 ACTIVE
P5 RETIRED
```

The same metadata is interpreted differently by producer and consumer code.

## New Client -> Old Broker

Safe.

If lifecycle state is absent, the new client treats all partitions as:

```text
ACTIVE
```

This is correct because the old broker cannot retire partitions.

## Old Client -> New Broker, Normal Topic

Safe while the topic contains only normal active partitions.

Existing Kafka behavior remains unchanged.

## Old Client -> Elastically Resized Topic

Not supported in v1 while the topic contains:

```text
TRANSITIONING
or
RETIRED
```

The broker should fail closed for lifecycle-unaware metadata access rather than allow the client to silently misinterpret partition state.

## Rollout Model

Recommended operational sequence:

```text
1. upgrade brokers/controllers
2. upgrade producers and consumers using target topic
3. enable elastic resizing for that topic
4. perform shrink/expand operations
```

## Transition Grace Is Not Legacy Compatibility

The transition grace period exists for:

```text
upgraded producer
+
stale cached metadata
```

It does **not** make a lifecycle-unaware old producer compatible.

## Old Custom Producer

Even if a non-standard client bypasses lifecycle-aware partition selection, broker enforcement still prevents writes once a partition becomes retired.

## API Compatibility Goal

Elastic behavior should eventually be integrated into ordinary:

```text
KafkaProducer
KafkaConsumer
AdminClient
```

rather than requiring permanently separate client classes.

## Invariants

- normal topics remain compatible with older clients
- lifecycle-aware topics require lifecycle-aware clients in v1
- new clients default missing lifecycle state to `ACTIVE`
- broker write fencing protects correctness even when client behavior is wrong
