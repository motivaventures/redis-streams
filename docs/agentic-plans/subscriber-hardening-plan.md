# Redis Streams Hardening Plan

## Context

Initial review surfaced reliability gaps in subscriber lifecycle, error handling, and test strategy.

## Defects Found

1. Subscriber loop exits on unhandled runtime failures.
   - `xreadgroup` failures, `JSON.parse` failures, or handler failures can terminate polling.
2. No shutdown mechanism for subscribers.
   - `subscribe` recursively schedules itself and never provides a cancellation API.
3. Async handlers are not awaited.
   - Handler execution can overlap and produce unbounded concurrency with unclear ordering guarantees.
4. Consumer identity is unstable by default.
   - `nanoid()` creates a new consumer name each restart, causing consumer buildup in Redis.
5. Pending message recovery is incomplete.
   - Current flow reads only `>`, so orphaned pending entries from dead consumers are not reclaimed.
6. Group creation error handling is brittle.
   - String matching on "already exists" can miss Redis-specific error variants (`BUSYGROUP`).
7. Payload parsing and shape assumptions are fragile.
   - Parsing assumes fixed field ordering (`inner[1][1]`) without validation.
8. Test strategy is tightly coupled to local Redis integration.
   - Unit-level behavior is not covered with deterministic mocks.

## Proposed Changes

1. Add explicit subscription handle API.
   - Return an object from `subscribe` with `unsubscribe()`.
   - Track cancellation state and prevent further timer scheduling after unsubscribe.
2. Harden polling loop.
   - Wrap read + parse + handler + ack path in `try/catch`.
   - Keep polling alive on transient failures with bounded retry/backoff.
   - Surface errors through optional `onError` callback for observability.
3. Support async-safe handler contract.
   - Keep type as `() => void | Promise<void>` for compatibility.
   - Always `await` handler completion before polling next batch.
   - Preserve per-subscription message ordering.
4. Stabilize consumer identity defaults.
   - Prefer explicit `consumerName` via opts/env.
   - If not set, derive deterministic fallback (for example `HOSTNAME`) instead of random IDs.
5. Implement pending recovery and orphan reclaim.
   - On startup, drain own pending entries.
   - Reclaim orphaned pending entries using `XPENDING` + `XAUTOCLAIM` (or `XCLAIM`) with idle threshold.
6. Improve payload robustness.
   - Decode by field name (`json`) instead of positional index.
   - Handle malformed JSON without terminating the loop.
7. Improve tests.
   - Keep one optional integration test for real Redis behavior.
   - Add deterministic unit tests with mocked Redis client for lifecycle and error semantics.

## Execution Plan

1. Introduce `Subscription` handle type and cancellation flow.
2. Refactor recursive `setTimeout` into loop function with guarded rescheduling.
3. Add try/catch and configurable retry strategy (`retryDelayMs`, `maxRetryDelayMs`).
4. Update handler typing and await behavior.
5. Introduce stable default consumer naming strategy and docs.
6. Add pending recovery mode:
   - phase A: own pending drain,
   - phase B: orphan reclaim with idle timeout,
   - phase C: normal `>` tailing.
7. Add decode/validation guardrails for payload extraction.
8. Add/adjust tests to cover:
   - unsubscribe stops polling,
   - transient read failure does not permanently kill polling,
   - async handler does not overlap,
   - default consumer identity is deterministic,
   - startup flow recovers pending before `>` reads,
   - malformed JSON or handler throw is reported and loop continues.

## Risks and Mitigations

- Risk: Behavior change for users relying on fire-and-forget handler execution.
  - Mitigation: Document new sequential default and add opt-in concurrency later if needed.
- Risk: Retry loop can mask persistent failures.
  - Mitigation: expose error callback and include metadata (stream/group/consumer).
- Risk: Aggressive pending reclaim can steal in-flight work.
  - Mitigation: reclaim only entries above idle threshold and make threshold configurable.
