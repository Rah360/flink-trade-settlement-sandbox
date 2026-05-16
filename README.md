# Flink Trade Settlement Sandbox

A simple Apache Flink learning project for simulating trade settlement.

## What this project does

This project can run in two modes:
- Kafka mode
- Mock mode

It reads trade orders and bank payments, matches them by `orderId`, and produces `SettledTrade` records.
Successful settlements are sent to a demo sink that simulates storing data in a demat account system.

The project also logs timeout cases when:
- an order arrives but payment does not arrive in time
- a payment arrives but the order does not arrive in time

The streams use event timestamps, watermarks, and event-time timers for timeout handling.

## Main concepts used

- Flink DataStream API
- Kafka source
- custom mock source functions
- JSON to POJO mapping with Jackson
- `keyBy`
- connected streams
- `KeyedCoProcessFunction`
- `ValueState`
- watermarks
- event-time timers
- custom sink with `RichSinkFunction`

## Project structure

- `TradeSettlementApp.java` — application entry point
- `com.settlement.models` — event and output models
- `com.settlement.functions` — mock generators and matching logic
- `com.settlement.sinks` — demo sink for settled trades
- `docker-compose.yml` — local Kafka setup

## Kafka topics

The app expects these Kafka topics:
- `orders-topic`
- `payments-topic`

Kafka is configured on:
- `localhost:9092`

## Message format examples

Trade order JSON:
```json
{"orderId":"101","asset":"GOLD_ETF","eventTimestamp":1710000000000}
```

Bank payment JSON:
```json
{"orderId":"101","status":"SUCCESS","eventTimestamp":1710000005000}
```

## Running modes

Default behavior:
- Kafka mode is enabled by default

Start local Kafka:
```bash
docker compose up -d
```

Run with mock mode from an IDE by setting this VM option:
```bash
-DuseKafka=false
```

Or launch with an environment variable:
```bash
USE_KAFKA=false
```

## Notes

- This is a sandbox/learning project, not a production system.
- The app is designed to run locally with Flink's local environment and Web UI.
- The sink currently prints messages to simulate downstream storage.
- Event-time behavior is driven by timestamps on incoming events and watermark progress.

## Future improvements

- add tests
- separate success and timeout/failure handling more cleanly
- package the app more cleanly under a Java package
- replace demo sink with a real external system later
