# Payment Orchestrator — Minimal POC (Bun + TypeScript)

A minimal proof-of-concept for the architecture in [../PLAN.md](../PLAN.md). Goal: prove the plan's money-safety core in runnable code — KISS, nothing more. Zero npm dependencies: Bun + bun:sqlite + bun:test.

## The six core claims it proves

1. **The database is the only enforcer.** Every state change is a conditional UPDATE (SET status = next WHERE id = ? AND status = expected); affected rows checked; 0 rows = someone else moved it = no-op. No read-then-write.
2. **Transactional outbox.** State change + event row commit in ONE transaction; an in-process relay (claim → dispatch → mark) delivers at-least-once; idempotent handlers make crash/replay safe.
3. **Double-entry ledger.** Every group sums to zero, asserted on write; tenant and account aggregates update in the same transaction; a verifier recomputes both from the immutable ledger.
4. **Evidence-only money.** Pay-ins credit only against a real ingested bank credit corroborated by UTR + amount + account (one matcher, used two-sided). Holds release only on positive bank evidence. Ops resolutions cannot mint money.
5. **One authoritative gate.** A compare-and-set row with expiry arbitrates statement-vs-payout; expired = free; payouts defer; fencing re-checked before every bank step.
6. **Ambiguity-safe payouts.** Hold first; submit exactly once with an idempotency key; timeout → unknown with the hold KEPT; failed only on positive non-execution evidence; every non-terminal row has a deadline and a sweeper.

## Stack substitutions (plan → POC)

| Plan v2 | POC | Why it is safe |
|---|---|---|
| MySQL 8 | bun:sqlite, BEGIN IMMEDIATE | Same ACID single source of truth; BEGIN IMMEDIATE is the FOR UPDATE serialization analog; identical conditional-UPDATE guards, UNIQUE backstops, CHECK constraints |
| Redis Streams + claim relay | in-process outbox relay | Redis is disposable coordination in the plan; at-least-once + idempotent handlers is the claim, not the transport |
| Fastify HTTP | direct function calls | Every claim here is DB/transaction semantics |
| Real bank APIs | scripted fake connector | Same connector contract + failure scripts: reject, timeout, delayed credit, reversal, status-error |
| Loop daemons | sweeper.tick() called explicitly | Deadlines (next_check_at) are the claim, not timers |

## Money rules folded in from the plan review

Full 4-agent review record: [../REVIEW.md](../REVIEW.md). Rules this POC implements:

- Cancel payouts only from **created** — a held payout exits exclusively via evidence-based reconciliation (closes the cancel-releases-hold double-spend window).
- Inconclusive bank status → **unknown with the hold kept**; failed only on positive non-execution evidence; the fake bank's not_found is contractually definitive.
- Every ops resolution to succeeded must attach a **distinct unmatched bank credit** (UNIQUE matched_bank_txn_id backstop): no credit row → no money.
- All ingested credits **park in escrow at ingest** (debit bank_mirror · credit escrow) and consume escrow on match — bank_mirror always mirrors physical reality, matched or not.
- Payout selection requires tenant-available AND **account-available minus active holds** on that account.
- One open review per (kind, ref) — partial unique index; resolutions idempotent through entity-conditional guards.
- Resolution credits equal the **bank transaction's** amount, never the order's.

## Run

    bun run demo    # narrative walk of the headline flows
    bun test       # full scenario suite

## Scenarios (tests)

1. Immediate match at claim; event redelivery + repeat claim = no-ops
2. Delayed bank sync: credit arrives after claim → inline match inside the sync commit
3. Claim edges → reviews, never auto-fail: amount mismatch, duplicate live UTR (unique index → utr_reuse), claim after expiry (late_claim)
4. Ingest dedupe (aggregator re-report) + one-credit-one-order (matched_bank_txn_id UNIQUE)
5. Payout happy path; duplicate create with same idempotency key = same payout, one hold
6. Submit timeout → unknown → status check → settled; bank saw exactly one submission; hold consumed once
7. Unknown without evidence past max reconcile age → payout_ambiguous review, hold kept; positive evidence → failed → released
8. Crashes: after hold (never submitted) → sweeper bank-lookup by key → not_found → failed + released; after submit before mark → sweeper finds it executed → settled; never resubmitted
9. Gate: statement holds gate → payout defers; expired gate is free; fencing loss before submit → abort, zero bank calls
10. Reversal after success → storno + reversed + overdraw review; review resolutions replay = one money move; resolution without credit row rejected
11. Every test ends with: all groups zero-sum; aggregates equal ledger recompute; held equals the sum of non-terminal holds

## Project layout

    poc/
      package.json   tsconfig.json   .env.example   .gitignore
      apps/           api.ts  worker.ts  simbank.ts  demo.ts    # process entrypoints
      src/
        core/         clock  db  gate  ledger  matcher  outbox
                      payin  payout  reviews  sweeper  sync     # the money domain
        platform/     config  connector  fakebank  httpkit
                      runtime  system                             # infra seam + process wiring
      tests/          ledger  payin  payout unit scenarios
      e2e/            scale.ts (the multi-process scaling proof)
      run/            gitignored runtime artifacts (the SQLite file)

core is the plan's §modules + §db: guarded transitions, ledger, saga, matching — everything that decides money. platform is the plan's §platform + §config: the seams (bank connector, HTTP kit, config) and the two composition roots (runtime for real processes, system for in-memory tests/demo). apps is the plan's two process roles plus the simulated bank.

## Files

| File | Role |
|---|---|
| src/core/clock.ts | injectable clock — deterministic time travel for deadline tests |
| src/core/db.ts | DDL + seed + BEGIN IMMEDIATE transaction helper |
| src/core/ledger.ts | zero-sum groups, guarded aggregates, verifier, opening deposit |
| src/core/outbox.ts | emit-in-txn; claim → dispatch → mark relay; handler registry |
| src/core/gate.ts | CAS gate with expiry: acquire / release / fencing assert |
| src/core/matcher.ts | ONE corroborator used two-sided + rematch + reversal matching |
| src/platform/fakebank.ts | scripted connector |
| src/core/payin.ts | orders, claims, ingest, pay-in review resolutions |
| src/core/payout.ts | create, select → gate → hold → submit, cancel (created only), reconcile |
| src/core/sweeper.ts | tick(): expiries, deadlines, saga escalation, rematch backstop |
| src/platform/system.ts | wiring: fresh in-memory system per test / demo |
| tests/payin.test.ts, tests/payout.test.ts, tests/ledger.test.ts | the scenarios |
| src/platform/httpkit.ts + src/platform/config.ts | the entire HTTP "framework" (error type, validators, router) + env config |
| src/platform/connector.ts | BankConnector seam + HTTP client to the simbank (timeout < STALE_HELD_MS; never retries writes) |
| src/platform/runtime.ts | real-process wiring: file DB, wall clock, instance-scoped claims/gate holders, executor + crash hooks |
| apps/api.ts, apps/worker.ts, apps/simbank.ts | the three process entrypoints (api xN, worker xN, bank x1) |
| e2e/scale.ts | multi-process scaling proof: spawns everything and asserts exactly-once effects |
| apps/demo.ts | narrative run |

## Multi-process run (api xN + worker xN + simbank x1)

Three process kinds share ONE SQLite file (WAL + busy_timeout=5000, BEGIN IMMEDIATE). No Redis, no broker: the outbox claim row IS the consumer group, the bank-gate row IS the lock, conditional guards ARE the dedupe — replicas are safe by construction, which is exactly what the e2e proves.

    bun run bank        # simbank on :4000 (fake bank over HTTP)
    bun run api         # api on :3000 - run several: PORT=3001 bun run api
    bun run worker      # worker xN - each gets a unique WORKER_ID automatically
    bun run e2e         # the scaling proof: api x2 + workers xN + simbank, exactly-once asserts

Config (env, defaults in src/platform/config.ts): PORT, BANK_PORT, BANK_URL, DB_PATH (shared file), API_KEY, RELAY_INTERVAL_MS, SYNC_INTERVAL_MS, SWEEP_INTERVAL_MS, WORKER_HEALTH_PORT, WORKER_ID, DRAIN_DEADLINE_MS, FAULT_CRASH_AFTER_HOLD/FAULT_CRASH_AFTER_SUBMIT (deterministic crash injection for the e2e). Module timings are env-overridable too: EXEC_DELAY_MS, STALE_HELD_MS, RECONCILE_DELAY_MS, MAX_RECONCILE_AGE_MS, DEFER_BACKOFF_MS, CLAIM_TTL_MS, GATE_TTL_MS.

Multi-process safety (what changed in the core, and why): process-unique outbox claim tokens; instance-scoped gate holders (a same-string second worker would otherwise satisfy the gate's same-holder branch and defeat statement-vs-payout exclusion); idempotent seed (N processes booting on one file); wall clock in the runtime (createClock freezes time — test fixture semantics); SQLITE_BUSY skip policy in loops and one-retry-then-503 in the api; connector timeout (10s) strictly below STALE_HELD_MS; busy_timeout set before any lock-taking pragma.

Scaling honestly: N apis and N workers are replica-safe, and replicas buy availability + overlapped bank I/O — but SQLite has exactly one writer, so write throughput is capped (~3k-18k short txns/s measured; a fat txn blocks that process's whole event loop since bun:sqlite is synchronous). That ceiling, plus single-host-only WAL, is what forces the production swap — MySQL per the plan with the same guard SQL: BEGIN IMMEDIATE becomes START TRANSACTION + FOR UPDATE on the aggregates, the outbox claim and gate CAS are syntax-level changes, and every conditional guard, unique backstop and recovery path ports unchanged.
