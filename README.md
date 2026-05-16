# Flink Trade Settlement Sandbox

A simple Apache Flink learning project for simulating trade settlement.

## What this project does

This project creates two mock streams:
- Trade orders
- Bank payments

It matches them by `orderId` and prints results such as:
- successful settlement
- missing order warning
- timeout alert when payment does not arrive in time

## Main concepts used

- Flink DataStream API
- custom source functions
- `keyBy`
- connected streams
- `KeyedCoProcessFunction`
- `ValueState`
- processing-time timers

## Project structure

- `TradeSettlementApp.java` — application entry point
- `com.settlement.models` — simple event models
- `com.settlement.functions` — mock generators and matching logic

## Notes

- This is a sandbox/learning project, not a production system.
- The app is designed to run locally with Flink's local environment and Web UI.

## Future improvements

- handle payment failure states more explicitly
- store unmatched payments as state
- add tests
- package the app more cleanly under a Java package
