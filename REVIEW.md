# Plan Check & POC Record — 4 subagent reviews (2026-11-18)

How the check was run: four independent subagent reviewers with distinct lenses, each reading the full PLAN.md (v2, 519 lines) before judging. Findings below were folded into the POC in poc/ (rules + tests) and into a patch list for the plan.

## Verdicts

| Lens | Verdict | Summary |
|---|---|---|
| Money-safety | fixable | The MySQL-truth / conditional-guard / hold-first / outbox core is sound; every money-loss path found is a specific, patchable transition or resolution rule |
| Simplicity / POC scope | scoped | 6 core claims distilled; 14 cuts safe for a POC; 9 do-not-cut rules |
| Consistency audit | minor gaps | Status sets, ledger signs, UNIQUEs and indexes are internally consistent; 12 omissions / dangling refs, zero money-path contradictions |
| Bun feasibility | feasible | Every needed primitive verified empirically on Bun 1.4.4 / SQLite 3.53.4; zero-dependency POC viable |

## Money-safety findings

CRITICAL — all folded into the POC as rules with tests:

1. Cancel-from-held double-spend: admin cancel on a held payout could release a hold that is mid-submit at the bank. POC rule: cancel only from **created**; held exits exclusively via evidence-based reconciliation. (test: cancel rules)
2. Hold released on inconclusive evidence: stale-held recovery with an unavailable bank status must never fail + release. POC rule: inconclusive → **unknown with the hold kept**; the fake bank's not_found is contractually definitive; failed only on positive non-execution evidence. (tests 7, 8a, 8b)
3. Resolutions could mint money: ops confirm could reach succeeded with matched_bank_txn_id NULL. POC rule: every succeeded resolution **requires a distinct unmatched credit** (UNIQUE matched_bank_txn_id backstop); the credit equals the **bank txn amount**. (test 10)

MAJOR:

4. bank_mirror reflected only matched activity → POC parks **every** ingested credit in escrow at ingest and consumes escrow on match, so bank_mirror always mirrors physical reality. (tests 1, 3a)
5. Payout window usage omitted unknown → plan patch; POC counts held + in_flight + unknown as active holds.
6. Duplicate reviews could double-move money → POC: one open review per (kind, ref) via partial unique index + entity-guarded, idempotent resolutions. (test 10 replay)
7. Reversal evidence was undefined → POC: debit-side corroborated matcher (UTR + amount + account) driving the guarded succeeded → reversed storno, with a reversal_overdraw review when the tenant goes negative. (test 10b)
8. Redis flush between XADD and consumption permanently loses webhook deliveries → plan patch; the POC's in-process relay keeps dispatch durable from the outbox row itself.

MINOR (noted): lock-order inversion between the matcher halves (deadlock retry saves it — moot in SQLite); holds did not reserve physical funds → POC subtracts per-account active holds at selection (test 14); return-to-sender should spend the specific credit's escrow, not operating funds; select/acquire order wording → POC unified as select → acquire gate → re-verify → hold (gate-before-hold, one transaction).

## Plan patch list (candidate PLAN.md v3)

1. Outbox claim predicate must include **published_at IS NULL** (+ supporting index) — as written it re-claims published rows forever.
2. The webhook dispatcher is missing from the §10 worker loop list — it is the sole consumer of every payin.* / payout.* event.
3. Add reviews.created_at and an audit_log timestamp column (both referenced, neither declared).
4. Add the retention/janitor task to the master sweeper's enumeration.
5. Specify the late-claim path: expired → late_claim review; resolution → succeeded (with attached credit) | cancelled.
6. Widen the 1-minute re-match sweep to review-parked orders, or mark that resolution ops-only.
7. Include unknown in every payout usage status set (§6) — its holds are retained and may have executed.
8. Give payin_reversed / reversal_overdraw reviews producing flows.
9. Add payout.rejected / payout.cancelled / payin.cancelled to the §9 event catalog (promised webhooks otherwise dangle).
10. Align providers.config wording (§8) with the actual per-connection config in §4.
11. Document or fix the Redis-flush window between XADD and consumption (webhook deliveries).
12. Add escrow / fee_income entry examples to §5, or mark both post-v1.

## POC scope (from the simplicity review — implemented in poc/)

IN: conditional-transition guards on every state change; one transaction per effect (guard + ledger + aggregates + outbox); zero-sum double-entry ledger with a recompute verifier; CAS gate with expiry; ONE two-sided matcher (claim-time + ingest-time) + rematch backstop; payout saga with sweeper escalation; scripted fake bank; injectable clock; 28 scenario tests.
OUT: Redis, HTTP API, webhooks, RBAC/audit, tenant authn, rate limiter, window limits, loop daemons, DLQ/metrics, retention/snapshots/drift, graceful drain, migrations/docker, deadlock retry, all npm dependencies.

## Assumptions hardcoded in the POC (from §2 + §17)

- Check-in = bank reads (statement/balance); that is the payout-gating op.
- Shared tenant accounts matched by UTR + corroboration; no virtual accounts.
- Payout requires BOTH tenant-available AND per-account-available (minus active holds).
- Reversal = auto-storno + alert; overdraw raises a review.
- Stray credits park in escrow; default ops resolution = credit tenant.
- Statement/status confirmation only; unknown → failed needs positive evidence; the fake bank's not_found is definitive (every submit is recorded durably).
- Defaults: 30 m order expiry, 48 h claim window, 24 h max reconcile age, 60 s outbox claims and gate TTL, 30 s reconcile cadence.
- Direct-vs-aggregator is moot at POC scale: the mock connector sits behind the same BankConnector contract.

## Bun feasibility notes (all verified empirically)

- BEGIN IMMEDIATE is the FOR UPDATE serialization analog; transactions are synchronous only — never span an await.
- Guards: conditional UPDATE ... WHERE status = expected, affected-rows checked; encode source != target so redelivery is a 0-row no-op in both engines.
- Unique violations: e.code SQLITE_CONSTRAINT_UNIQUE + message naming table.columns; classify by both; never INSERT OR IGNORE on guard paths.
- active_utr as a generated column + UNIQUE(account_id, active_utr) works; NULLs are distinct, exactly like MySQL.
- Gate CAS: INSERT ... ON CONFLICT (scope, entity) DO UPDATE ... WHERE expired-or-same-holder — changes 1/0 is the CAS result; release conditional on holder.
- Integer paise: JS numbers are exact below 2^53; SUM overflow throws loudly; assert integer at boundaries.
- The plan's utr_key hash is replaced by a composite UNIQUE (account, utr, direction, amount, booked_at) — same dedupe, zero moving parts.
- Injected clock everywhere (never SQLite's clock); WAL + busy_timeout as seatbelts; PRAGMA foreign_keys is OFF by default and required.
- :memory: databases are per-connection; cross-connection SQLITE_BUSY needs a file DB.

## Result

- poc/ — Bun + TypeScript, **zero npm dependencies**, 11 source modules, 3 test suites.
- **28/28 tests green**, including: duplicate-UTR and one-credit-one-order backstops, both crash-recovery paths (never resubmitted), ambiguous payout with hold kept until positive evidence, gate mutual exclusion with expired-free and fencing semantics, both-checks account selection with active-hold reservation, idempotent resolutions that cannot mint money.
- Ledger invariants verified after every test: all groups zero-sum; tenant and account aggregates equal a recompute from the immutable ledger; held equals the sum of non-terminal payout holds.
- bun demo.ts runs the full narrative (both matcher sides, event-driven payout, ambiguous payout recovered on evidence, reversal) and ends with all invariants holding and zero open reviews.
