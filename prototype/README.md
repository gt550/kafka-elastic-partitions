# Prototype

This module is the first executable proof of concept for Kafka Elastic Partitions.

It models the lifecycle independently of Apache Kafka internals so that the state machine and `ResizeEpoch` rules can be validated before modifying Kafka source.

## Requirements

- Java 17+
- Maven 3.9+

## Run tests

```bash
mvn test
```

## Run the demo

First compile:

```bash
mvn test
```

Then run:

```bash
java -cp target/classes io.kafkaelastic.DemoApp
```

The demo performs:

```text
10 ACTIVE
    ->
5 ACTIVE + 5 TRANSITIONING
    ->
5 ACTIVE + 5 RETIRED
    ->
10 ACTIVE
```

## Current scope

The prototype currently models:

- `ACTIVE`
- `TRANSITIONING`
- `RETIRED`
- monotonic `ResizeEpoch`
- one active resize per topic
- transition grace deadline
- retirement deadline
- reactivation before new partition creation
- stale epoch fencing

It does not yet connect to a real Kafka broker.
