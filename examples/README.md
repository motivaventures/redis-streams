# Redis Streams Examples Harness

This harness demonstrates realistic subscriber behavior with multiple consumer instances and
pending-message recovery semantics.

## Run

```bash
pnpm run examples:harness
```

Optional environment variable:

- `REDIS_URL` (default: `redis://127.0.0.1:6379/0`)

## What It Validates

1. Two consumer instances in one group load-balance messages while another group receives full fanout.
2. A message left pending by one consumer identity is **not** picked up by a different identity.
3. The same consumer identity can restart with `recoverPending: true` and recover its own pending message.

This gives an operational sanity check beyond unit tests for the subscriber lifecycle changes.
