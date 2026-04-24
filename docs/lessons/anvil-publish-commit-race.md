# Anvil push/nack–claim race — root cause of CI data-loss flakes

*Investigation: 2026-04-24*

## Problem

`test_chaos_stress::test_many_small_batches_stress`,
`test_long_running_stability`, `test_sustained_chaos`,
`test_stability_worker_recovery::test_worker_restart_continues_from_offset`,
and `test_distributed_elasticity::test_scale_down_worker_failures` all
flake with the same signature under CI: kill a worker mid-flight and
~1–5 % of records go missing at the sink. One example (PR #82, run
24907821368, job 72941673472):

```
DIAGNOSTIC: test_scale_down_worker_failures
Expected: 10000, Got: 9900, Delta: 100
Missing composite keys: [(550, 0), …, (599, 1)]
Affected batches (split indices): [11]
Batch ranges: [(550, 599)]
Collector: 9900 records stored, 0 duplicates filtered
--- Broker Queue Stats ---
  source    output: pending=0, claimed=0
  transform output: pending=0, claimed=0
  sink      output: pending=0, claimed=0
```

Exactly one contiguous batch missing, zero duplicates, every queue
empty at end of run. A whole batch simply vanished — it wasn't
reprocessed by a second worker (no dup), and it wasn't stuck in flight
(all queues empty).

## Root cause

The broker's push/nack code advances the in-memory `push_seq` atomic
counter **before** committing the `pending_key` to the underlying DB:

`lib/anvil-rs/src/storage.rs`

```rust
// push_messages
let base_seq = c.push_seq.fetch_add(count, Ordering::Relaxed);   // 1. counter up
// … build WriteBatch with pending_key(base_seq..) …
self.db.write(batch).await?;                                      // 2. DB commit
```

```rust
// nack_messages_internal (used by recover_expired_claims)
let base_seq = c.push_seq.fetch_add(nack_count, Ordering::Relaxed);
for (i, msg_id) in msg_ids.iter().enumerate() {
    batch.delete(claimed_key(queue, msg_id));
    batch.put(pending_key(queue, base_seq + i as u64), msg_id.as_bytes());
}
self.db.write(batch).await?;
```

And `claim_messages` uses the same atomic as its upper bound:

```rust
let cur = c.claim_seq.load(Ordering::Acquire);
let lim = c.push_seq.load(Ordering::Acquire);   // ← may be ahead of what's committed
if cur >= lim { return Ok(Vec::new()); }
let target = std::cmp::min(cur + batch_size as u64, lim);
// CAS claim_seq cur → target
// Read pending_key(seq) for seq in [cur, target)
// If pending_key missing: skip silently  ← data loss
```

There is a window — between `push_seq.fetch_add` and `db.write` — where
a claimer can:

1. Observe the new `push_seq`.
2. CAS its `claim_seq` past the reserved range.
3. Read `pending_key(seq)`, find **nothing** (not yet committed).
4. Skip silently (comment: *"gap from crashed push, skip silently"*).

`claim_seq` has now advanced past seq N. When the nacking writer finally
commits `pending_key(N) = msg_id`, no future claim will ever read it —
it is orphaned.

Under chaos tests the window is hit repeatedly because recovery reclaims
N messages in a tight loop while many workers are actively claiming.

## Why it matches the symptoms

| Observation | Explained |
| --- | --- |
| Exactly one contiguous batch missing | One reclaim op lost its pending_key to one concurrent claim |
| 0 duplicates filtered | No second worker ever processed the msg — it was never claimable |
| All queues empty at end | The msg is orphaned in `pending_key(N)` but `claim_seq ≥ N`, so `pending_count = push_seq − claim_seq` reports 0 |
| `acked_count = 0` on transform output | Stat reporter returns counter state after retention window; orthogonal to the bug |
| Only 1–5 % loss, not 100 % | The race window is narrow (in-memory counter update → batch.write); most reclaims complete before a concurrent claim reads the gap |

The same shape applies to `push_messages` during source production, but
the test suite doesn't usually run concurrent claimers on a source's
first batch, so the source-side race rarely bites in practice. Reclaim
is the dominant trigger.

## The "silent skip" comment is wrong

`storage.rs:532`:

```rust
// If pending key is missing (gap from crashed push), skip silently
```

There is no crashed-push scenario under the current architecture — the
broker runs in the driver (see lesson L5), and `push_messages` either
returns success with both counter and batch committed, or returns an
error and neither change lands persistently (the atomic counter *can*
drift on error, but that's a separate counter-corruption bug — see the
comment at `storage.rs:601-604`). The "gap" the skip exists for is
actually the publish–commit race described above, not a crash.

## Candidate fixes

Ranked by scope.

1. **Commit-then-advance (medium scope, preferred).** Reorder push/nack
   so the persistent `seq_push_key` counter in the WriteBatch is the
   source of truth, and the in-memory atomic is only advanced **after**
   `db.write` returns. Readers (claim) use a separate "committed
   watermark" — e.g. reserve the range in a second atomic
   (`push_seq_reserved`) but have claimers bound their work by a new
   `push_seq_committed` atomic that is bumped post-commit. Handles
   out-of-order commits by advancing committed only when reserved ≥
   committed contiguously; for the common single-reclaim case it's a
   simple `fetch_add` after `db.write`.

2. **Per-queue lock (smallest scope).** Wrap
   `push_messages`/`nack_messages_internal` with a per-queue async
   mutex; `claim_messages` takes the lock only for the
   `push_seq`-read + CAS path (not for DB reads). Adds contention but
   removes the race entirely. Most surgical, least design change.

3. **Retry-on-gap in claim (tactical).** If `pending_key(seq)` is
   missing, roll back `claim_seq` and retry after a brief sleep. The
   "roll back a CAS'd value" bit is brittle — you have to use
   `fetch_min` semantics or a tombstone list. Not recommended: papers
   over the bug without fixing the invariant.

4. **Move counter into WriteBatch only (largest scope).** Delete the
   in-memory atomic entirely; use only the persisted `seq_push_key`
   and serialize advance through a single-writer task. Simplest mental
   model, biggest rewrite.

Recommendation: start with (2) as a targeted fix, then evaluate (1) if
the lock contention shows up in bench numbers. Either one makes the
"skip silently" comment go away, because the invariant becomes: *a
claimer that observes `push_seq ≥ X` is guaranteed to see `pending_key`
for every seq in `[0, X)`*.

## Interim: make the race visible in CI

Replace the silent skip with a tracing warning that logs
`queue=… seq=… claim_seq=… push_seq=…`. This turns the next CI flake
into direct evidence (and locally into an early-warning signal during
chaos tests).

## Related code / commits

- Broker push: `lib/anvil-rs/src/storage.rs` `push_messages` (~line 454)
- Reclaim: `lib/anvil-rs/src/storage.rs` `nack_messages_internal`
  (~line 857), called from `recover_expired_claims` (~line 1087)
- Claim: `lib/anvil-rs/src/storage.rs` `claim_messages` (~line 493),
  silent-skip at `storage.rs:532`
- Diagnostics helper: `engine/tests/utils/diagnostics.py`
- Recent area-touching commits: #79, #80 added diagnostics but didn't
  fix the race; #74 (b10d50e) "WorkQueue atomic counters" introduced
  the current counter scheme.
