# Transaction Semantics

This document defines how Kafka transactions interact with partition retirement.

## Background

A Kafka transaction may involve multiple partitions.

A transaction does not literally "start on P7." P7 becomes a participant when transactional records are sent to it.

## v1 Rule

Once a partition enters `TRANSITIONING`:

> **No new transaction may add that partition as a new participant.**

Transactions that already included the partition before transition began may complete.

## Example

Before transition:

```text
T100 participants:
P2
P7
```

Then:

```text
P7 -> TRANSITIONING
```

T100 may:

```text
COMMIT
or
ABORT
```

But a new transaction:

```text
T101
```

cannot newly add P7.

## Important Edge Case

Suppose:

```text
T100 begins
T100 writes P2

P7 -> TRANSITIONING

T100 attempts first write to P7
```

That write is rejected.

The rule is based on whether P7 was already a transaction participant before transition, not merely whether the transaction started earlier.

## Retirement Cutover

At the end of transition grace:

```text
fence new application writes
        |
        v
finish already accepted non-transactional writes
        |
        v
allow already-participating transactions to resolve
        |
        v
complete transaction bookkeeping/markers
        |
        v
commit RETIRED
```

## Internal Quiescing

Implementation may internally use a quiescing phase:

> stop admitting new work while allowing accepted work to finish

`QUIESCING` does not need to be a public partition lifecycle state in v1.

## Timeout / Failure

If an existing transaction does not resolve promptly, normal Kafka transaction timeout and abort behavior should remain authoritative.

Elastic resizing should not invent a second transaction timeout model.

## Reactivation

Once a partition is reactivated:

```text
RETIRED -> ACTIVE
```

new transactions may again add it as a participant.

## Invariants

- `ACTIVE`: normal transaction participation
- `TRANSITIONING`: no new participation; existing participation may finish
- `RETIRED`: no application transaction participation
- retirement is committed only after required existing transactional state is resolvable/stable
- normal Kafka transaction timeout semantics remain intact
