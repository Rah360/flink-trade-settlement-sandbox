# Flink Trade Settlement Sandbox

A simple Apache Flink learning project for simulating trade settlement.

## What this project does

This project creates two mock streams:
- Trade orders
- Bank payments

It matches them by `orderId` and produces `SettledTrade` records.
Successful settlements are then sent to a demo sink that simulates storing data in a demat account system.

The project also logs timeout cases when:
- an order arrives but payment does not arrive in time
- a payment arrives but the order does not arrive in time

The streams now use event timestamps, watermarks, and event-time timers for timeout handling.

## Main concepts used

- Flink DataStream API
- custom source functions
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

## Notes

- This is a sandbox/learning project, not a production system.
- The app is designed to run locally with Flink's local environment and Web UI.
- The sink currently prints messages to simulate downstream storage.
- Event-time behavior is driven by timestamps on mock events and watermark progress.

## Future improvements

- add tests
- separate success and timeout/failure handling more cleanly
- package the app more cleanly under a Java package
- replace demo sink with a real external system later
