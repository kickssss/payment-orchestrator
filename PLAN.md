# Payment Orchestration System — Architecture & Implementation Plan

Status: **v2 — reviewed** by 4 independent agents (money-safety, simplicity/ops, requirements-fit, data-model); all verdicts "fixable", all CRITICAL items folded in. See §19 for the review record and what changed.
Stack: Node.js 22 (TypeScript) + MySQL 8 + Redis 7 · Review surface: Plannotator

---

## 1. What we are building

A multi-tenant payment orchestration platform. Each **tenant** (customer) connects multiple **bank accounts** through **bank connections**. Three operations:

- **Pay-in** — create an order, show the tenant's bank details (account number, IFSC) to the end customer, verify the customer's claimed UTR against real bank transactions, credit the tenant once bank evidence confirms.
- **Payout** — admin-initiated outbound transfer; pick an eligible account (balance + limits + bank restrictions), hold the money, submit once, reconcile every ambiguous outcome.
- **Check-in** — *(working definition, confirm in review)* bank reads: statement/transaction/balance fetch — the operation that conflicts with payouts at some banks.

Design goals:

1. **MySQL is the only source of truth** — balances, limits, states. Redis (locks/buckets/streams) is disposable coordination; a full Redis flush loses nothing.
2. **The database enforces the rules, not discipline** — conditional `UPDATE … WHERE status = <expected>`, unique constraints as backstops, `FOR UPDATE` serialization. App-level checks are advisory only.
3. **Every state change + its event commit in one MySQL transaction** (outbox); handlers are idempotent, guards make redelivery a no-op.
4. **Bank APIs are touched by one serialized, rate-limited, gated path**; writes are never blind-retried.
5. **Nothing wedges**: every non-terminal row has a deadline (`next_check_at`) and a sweeper that escalates it.

---

## 2. Glossary & assumptions

| Term | Meaning |
|---|---|
| Tenant | Customer business; every row carries `tenant_id`, isolation enforced in the repository layer. |
| Bank connection | Credentials + config for one bank/aggregator integration: provider, rate limits, gate scope, status. |
| Bank account | Real account (number + IFSC + holder) reachable via a connection; pay-in/payout capability flags; per-window limits. |
| Check-in | Bank reads: statement/transactions/balance. **Assumption to confirm** — this is the op that blocks payouts ("no payout while seeking a statement"). |
| Claim | End customer supplies the UTR against a pay-in order. |
| UTR | Unique Transaction Reference (NEFT/IMPS/RTGS). |
| Window | Limit period — day/week/month — evaluated in tenant timezone (default Asia/Kolkata). Timestamps stored UTC. |

Other assumptions:

- "Payouts will work by providing account information to the payer" is read as the pay-in display-details mechanic; payout requests carry beneficiary details from the admin.
- INR only; integer paise; no floats.
- Matching disambiguates by UTR with corroboration (amount, account, direction) — shared tenant accounts, not virtual accounts (§17 question).
- Banks via a connector interface; direct or aggregator (§17 question).

---

## 3. Architecture overview

**Event-driven modular monolith**: one codebase, one artifact, two process roles:

- `api` — Fastify HTTP: tenant API, admin API. Stateless, ×N.
- `worker` — **4 loops** (v2, down from 7): outbox relay, bank sync, payout executor, master sweeper. ×N replicas; safety from DB locks + consumer groups.

```
                        ┌──────────────────────────────────────────┐
 tenant / admin ──HTTP──▶  api role (Fastify)                       │
                        └───────┬──────────────────────────────────┘
                                │ same MySQL txn: state change + outbox row
                        ┌───────▼──────────────────────────────────┐
                        │  MySQL 8  (orders, ledger, balances,     │◀── source of truth
                        │  limits, gate, outbox, reviews, audit)    │
                        └───────┬──────────────────────────────────┘
                                │ outbox relay (claim → XADD → mark)
                        ┌───────▼──────────────────────────────────┐
                        │  Redis 7 — one 'events' stream,          │◀── coordination only
                        │  consumer groups, sync lock, rate limits  │
                        └───────┬──────────────────────────────────┘
                                │ consumers (worker role)
        ┌───────────────────────┼────────────────────────────┐
        ▼                       ▼                            ▼
  payout executor         webhook dispatcher            master sweeper
        │                 (tenant notifications)        (deadlines, retries,
        ▼                                                 janitor, verify)
  bank sync (per connection; gate + rate limit + inline match)
        │
  bank connectors ──▶ bank / aggregator APIs
```

| Decision | Choice | Rationale / rejected |
|---|---|---|
| Topology | Modular monolith, 2 roles | Ledger-shaped domain; one strongly-consistent DB wins. Microservices/Kafka rejected (stack constraint, ops burden). |
| Event bus | Redis Streams, **one `events` stream** | In-stack; consumer groups, PEL reclaim (XAUTOCLAIM), DLQ. v2: four topic streams cut — consumers filter by type. |
| Delivery | At-least-once; **guards are the dedupe** | v2: `processed_events` table dropped — conditional transitions + unique keys already make redelivery a no-op; redelivery is documented as normal. |
| Publication | Transactional outbox, claim-based relay | Relay claims rows (`claimed_by/claimed_at`), XADDs, then marks published — replicas never double-publish; a crashed claim self-expires (60 s) and is re-claimed; duplicates are safe. |
| Bank gate | **MySQL row as single authority** (`bank_gates`) | v2: Redis gate lock dropped — two leases that can disagree is worse than one authoritative compare-and-set row with expiry. Redis keeps: rate buckets, sync lock, streams. |
| Money | Integer paise; guarded aggregates | `tenant_balances` (pooled) + `account_balances` (per-account physical) both row-locked and maintained in the effect transaction. |
| Jobs | Interval loops + deadline-indexed sweeps | No scheduler dependency; N-replica safe; survives Redis restarts. |
| IDs | BIGINT PKs; public UUIDs on API-exposed entities | Index-friendly, no volume leakage. |

Dependencies: fastify, knex + mysql2, ioredis, zod, pino. Job scheduling by loops; BullMQ not needed.

---

## 4. Data model (MySQL)

Conventions: BIGINT PK; `tenant_id` where relevant; UTC `DATETIME(3)`; money columns `*_paise` BIGINT; statuses `VARCHAR(32)` + `CHECK` (extensible without coordinated DDL); ENUM only for closed sets (direction). Migrations: knex, forward-only in prod.

### Tenancy & banking

```sql
tenants(id, slug UNIQUE, status, timezone DEFAULT 'Asia/Kolkata', settings JSON)

bank_connections(
  id, tenant_id, provider VARCHAR(32) REFERENCES providers(name),
  credentials_enc BLOB,                    -- AES-256-GCM
  status, config JSON                       -- rate limits, gate_scope ('account'|'connection'),
                                            -- sync_interval_s, exclusions
)
providers(name PK, label, connector_class)  -- lookup table

bank_accounts(
  id, tenant_id, connection_id,
  account_number, ifsc, holder_name,
  supports_payin BOOL, supports_payout BOOL, status, priority INT,
  limits JSON                              -- §6 spec
)
INDEX (tenant_id), INDEX (connection_id)

api_keys(id, tenant_id, key_hash UNIQUE, status, last_used_at)
admin_users(id, email UNIQUE, role CHECK('ops','finance','superadmin'), status)

sync_state(
  account_id PK, connection_id, last_cursor DATETIME(3) NULL,
  last_fetch_started_at, last_sync_finished_at, last_error TEXT
)
INDEX (connection_id)                       -- due-sync scans

bank_gates(                                 -- v2: single authority for mutual exclusion
  scope CHECK('account','connection'), entity_id BIGINT,
  holder_op CHECK('statement','payout'), holder_id VARCHAR(64),
  acquired_at DATETIME(3), expires_at DATETIME(3),
  PRIMARY KEY (scope, entity_id)
)
```

Gate semantics: acquire = conditional `UPDATE … WHERE (expires_at < NOW(3) OR holder_id = ?) LIMIT 1` (compare-and-set; expired gate counts as free); release/renew = conditional on `holder_id = ?`. Holder re-verifies (fencing) before every bank-call step and aborts or defers on loss. A stale-gate sweep reports gates held past expiry × 2 for ops visibility.

### Orders

```sql
payin_orders(
  id, tenant_id, account_id,
  client_order_id,                          -- UNIQUE (tenant_id, client_order_id)
  amount_paise, currency CHAR(3) DEFAULT 'INR',
  customer_ref TEXT,
  status CHECK('pending','claimed','succeeded','mismatch_review','expired','cancelled','reversed'),
  claimed_utr VARCHAR(32), claimed_at, utr_deadline, expires_at,
  matched_bank_txn_id BIGINT,               -- UNIQUE: one bank credit can never credit two orders
  active_utr VARCHAR(40)                    -- generated: IF(status IN ('pending','claimed'), claimed_utr, NULL)
                                            -- UNIQUE (account_id, active_utr): duplicate live UTR claim
                                            --   → ER_DUP_ENTRY → utr_reuse review (DB backstop)
)
INDEX (account_id, status, created_at)       -- usage windows
INDEX (status, expires_at)                   -- expiry sweeper
INDEX (status, utr_deadline)                 -- claim-deadline sweeper
INDEX (account_id, claimed_utr)              -- ingest fast-path

payouts(
  id, tenant_id,
  account_id BIGINT NULL,                    -- NULL until selection; usage counts from held onward
  idempotency_key,                          -- UNIQUE (tenant_id, idempotency_key)
  amount_paise, currency CHAR(3) DEFAULT 'INR',
  beneficiary JSON, purpose_ref,
  status CHECK('created','held','in_flight','settled','failed','unknown','rejected','cancelled'),
  ledger_txn_id BIGINT NULL,                -- the hold group
  next_check_at DATETIME(3) NOT NULL,       -- mandatory on every non-terminal row
  bank_ref, bank_error JSON, attempts,
  selected_reason JSON                       -- per-account reasons when rejected
)
INDEX (status, next_check_at)                -- saga sweeper (all non-terminal)
INDEX (account_id, status, created_at)       -- usage windows
```

### Bank data & ledger

```sql
bank_transactions(
  id, tenant_id, account_id,
  bank_txn_id VARCHAR(64),                  -- UNIQUE (account_id, bank_txn_id)
  utr_key CHAR(40),                         -- generated: (account_id, utr, direction, amount, booked_at) hash
                                            -- UNIQUE: aggregator re-reports under new ids dedupe here
  direction CHECK('credit','debit'), amount_paise, utr VARCHAR(32),
  counterparty JSON, booked_at, raw JSON, ingested_at,
  match_status CHECK('unmatched','matched_payin','matched_payout','unknown_credit')
)
INDEX (account_id, utr), INDEX (account_id, booked_at)
INDEX (match_status, direction, ingested_at) -- unknown-credit sweep

ledger_txns(id, tenant_id, kind, ref_type, ref_id, created_at)   -- one row = one balanced group
ledger_accounts(id, tenant_id NULL, kind CHECK('tenant_operating','tenant_holds','bank_mirror','fee_income','escrow'),
                key VARCHAR(64), UNIQUE (kind, key))              -- key: 'bank_mirror:{account_id}', 'tenant_operating:{tenant_id}'
ledger_entries(
  id, ledger_txn_id, ledger_account_id,
  direction CHECK('debit','credit'), amount_paise, created_at
)                                            -- append-only, immutable
INDEX (ledger_txn_id), INDEX (ledger_account_id, created_at), INDEX (ref_type, ref_id) on ledger_txns

tenant_balances(tenant_id PK, available_paise, held_paise)     -- guarded aggregate (pooled)
account_balances(account_id PK, available_paise)               -- guarded aggregate (physical money at bank)
account_balance_snapshots(id, account_id, as_of, balance_paise, source CHECK('bank','ledger'))
```

### Events & ops

```sql
outbox(
  event_id UUID PK, tenant_id NULL, aggregate_type, aggregate_id,
  event_type, payload JSON, occurred_at,
  claimed_by VARCHAR(64) NULL, claimed_at DATETIME(3) NULL, published_at DATETIME(3) NULL
)
INDEX (claimed_at), INDEX (published_at)
-- claim: UPDATE … SET claimed_by=?, claimed_at=NOW(3) WHERE claimed_at IS NULL OR claimed_at < NOW(3)-INTERVAL 60 SECOND … LIMIT 100
-- crash safety: unmarked rows re-claim after 60 s; duplicate XADDs are safe (handlers idempotent)

webhook_endpoints(id, tenant_id, url, secret_enc BLOB, event_filter JSON, status)
webhook_deliveries(id, endpoint_id, event_id, status, attempts, next_attempt_at, UNIQUE (event_id, endpoint_id))
INDEX (status, next_attempt_at)

reviews(
  id, tenant_id, kind CHECK('utr_mismatch','utr_reuse','no_txn_found','late_claim','unknown_credit',
                            'payout_ambiguous','payin_reversed','reversal_overdraw'),
  ref_type, ref_id,                         -- what this review is about (order / payout / txn)
  payload JSON, status CHECK('open','resolved','rejected'),
  suggested_ref_id BIGINT NULL,             -- matcher's best guess (e.g. probable order for a credit)
  resolved_by, resolved_at, resolved_action JSON
)
INDEX (status, created_at), INDEX (tenant_id, status)

audit_log(id, actor_type, actor_id, tenant_id NULL, action, entity_type, entity_id, diff JSON)
INDEX (entity_type, entity_id)
```

Retention (janitor task): published outbox rows > 7 d; terminal webhook deliveries > 30 d; `bank_transactions.raw` stripped after 90 d (row kept); terminal reviews > 180 d; snapshots: latest 30 per account.

---

## 5. Money & ledger model

Double-entry, append-only; every group (`ledger_txns`) sums to **zero debits = credits** — asserted in code on every write, and by the nightly verifier over all history.

| Economic event | Entries |
|---|---|
| Pay-in confirmed | **debit bank_mirror:{account} X · credit tenant_operating X** (v2: signs fixed — money arrived at the bank, tenant claim grows) |
| Payout hold placed | debit tenant_operating X · credit tenant_holds X |
| Payout settled | **debit tenant_holds X · credit bank_mirror:{account} X** (v2: signs fixed — hold consumed, money left the bank) |
| Payout failed/cancelled | reverse group referencing the hold's `ledger_txn_id` |
| Pay-in reversed (technical reversal) | credit bank_mirror X · debit tenant_operating X (auto-storno on bank evidence; order → `reversed`; webhook + overdraw alert if tenant available < 0) |
| Review resolution moves money | one resolution-time group with `ref_type='review'`; unique per (review_id, action) — double resolution is a no-op |

Aggregates, both row-locked (`SELECT … FOR UPDATE`) and updated in the same transaction as the entries:

- `tenant_balances` — pooled tenant funds: available = settled pay-ins − payouts − fees − active holds.
- `account_balances` — physical money at each bank account, derived from `bank_mirror` entries. **v2 (requirements CRITICAL): payout selection requires BOTH tenant available ≥ amount AND account available ≥ amount** — pooled balance alone can pick an account with no physical funds.

Lock ordering is fixed everywhere: order/payout row → bank txn row → ledger aggregates → gate row. Deadlocks (InnoDB) are retried with backoff.

Nightly verifier recomputes both aggregates from the immutable ledger; drift or `held_paise > 0` with zero non-terminal payouts pages on-call.

---

## 6. Account limits & usage accounting

Limits as a zod-validated JSON spec per account + op:

```jsonc
{ "op": "payin", "max_txn_count": { "day": 200, "week": 1000, "month": 4000 },
  "max_amount_paise": { "week": 5000000000, "month": 20000000000 },
  "max_amount_per_txn_paise": 50000000 }
```

Usage computed from the DB (never cached counters):

- Pay-in committed capacity = orders with `status IN (pending, claimed, succeeded)` in the window (open orders hold displayed-account capacity until expiry). **The capacity check runs inside a txn holding the account row lock (`account_balances … FOR UPDATE`)**, so concurrent creates serialize and cannot both pass the last slot (money MAJOR 4).
- Payout usage = payouts with `account_id` set and `status IN (held, in_flight, settled)` in the window. **v2: re-verified inside the same hold transaction** — check-then-act race closed.
- Windows in tenant timezone: day = local midnight, week = Monday, month = 1st. Queries run on the §4 indexes; candidate sets are a tenant's own accounts, so cost is negligible.

---

## 7. State machines

**Enforcement**: every transition is a conditional statement — `UPDATE … SET status = next WHERE id = ? AND status = expected` (or a unique-constraint insert) inside the same MySQL transaction as the money effect; affected-rows checked; 0 rows = someone else moved it → no-op. This is what makes at-least-once events and XAUTOCLAIM redelivery safe (money CRITICAL 3).

### Pay-in order

```
pending ──claim(UTR)──▶ claimed ──bank evidence (UTR ∧ amount ∧ account)──▶ succeeded ──(reversal txn)──▶ reversed
   │                       │
   ├──tenant cancel─────▶ cancelled        ├─ amount ≠ / UTR reuse / deadline ─▶ mismatch_review
   └──expires_at────────▶ expired                                              │
                                                                resolution ──▶ succeeded (late exact match,
                                                    late claim accepted ───────▶│            or ops confirm)
                                                                               └──────────▶ cancelled (no credit /
                                                                                            return-to-sender payout)
```

- Success only on bank evidence. Claimed-but-unmatched never auto-fails; it escalates to review with context. Unclaimed credits park as `unknown_credit` reviews with the best-guess order attached (`suggested_ref_id`).
- Claim-after-expiry is accepted into review (kind `late_claim`), UTR preserved for matching (requirements MAJOR 5).
- UTR typo: audited re-claim replaces `claimed_utr` while claimed.
- Matching is **two-sided and inline** (ops SIMPLIFY 2): at claim time, search ingested credits by `(account, UTR)`; at ingest time, inside the sync commit, search live claims by `(account, UTR)`. Both paths share **one matcher module** (money MINOR 2) — corroborate: direction = credit, account = displayed, amount exact, single-use UTR. A 1-minute re-match sweep (claimed-unmatched ↔ unmatched credits) closes the read-race window before review escalation (money MAJOR 1).

### Payout

```
created ──(select acct: tenant+account balance, limits; gate acquired FIRST)──▶ held ──submit ok──▶ in_flight ──bank confirms──▶ settled
   │                                            │
   │ no eligible account ─▶ rejected            ├─ bank rejects ─▶ failed (hold released)
   └─ cancel ─────────────────▶ cancelled      └─ timeout/ambiguous ─▶ unknown ─┬─ evidence: settled
                                             (admin cancel from held ─▶ cancelled)├─ evidence: failed (hold released)
                                                                                   └─ max reconcile age exceeded,
                                                                                     no positive evidence ─▶
                                                                                     payout_ambiguous review (hold KEPT)
```

v2 changes: `reconciling` merged into `unknown` (three reviewers); **every non-terminal row carries `next_check_at`** and the sweeper escalates staleness (ops CRITICAL):

- `created` stale → re-drive to executor (idempotent).
- `held` stale (crash after hold, gate never freed) → query the bank by idempotency key → resolve to in_flight/settled, else failed + release hold. Never resubmit.
- `in_flight`/`unknown` → status check / statement search.
- `unknown → failed` only with **positive bank evidence of non-execution**; timeout alone is not evidence — otherwise `payout_ambiguous` review with the hold retained (money MAJOR 3: never release a hold without evidence, or a late landing transfer double-pays).

Writes to the bank are never blind-retried; submissions carry the payout's idempotency key.

---

## 8. Bank access discipline

All bank calls flow through one choke point per connection/account:

1. **Token-bucket rate limiter** (Redis Lua, atomic) keyed `rl:{connection_id}:{op}`, config per provider. Callers back off, never hammer. Redis flush resets buckets — documented accepted risk: gate + sync cadence bound steady-state burst (ops MINOR 4).
2. **Gate** — mutual exclusion ("no payout while statement generation runs"):
   - `bank_gates` row (MySQL, single authority) — compare-and-set acquire with expiry, renew by holder, release on completion; **expired gate = free** (acquire predicate). Scope per connection config: account-level or connection-level (data MAJOR 5 — some banks lock at session level).
   - **Both halves specified** (ops MAJOR 1): the statement sync sets gate=`statement` in its own txn before fetching, clears after; the payout executor acquires gate=`payout` **before placing the hold** (ops CRITICAL — tenant funds are never pinned behind a long statement run).
   - Fencing: the holder re-verifies ownership (holder still me, not expired) before each page/submit step; on loss it aborts or defers; bank "operation not allowed / busy" responses are retryable. The lease is best-effort by design and documented as such.
   - When in doubt, **payout defers; statements win**.
3. **Sync lock** — Redis `sync:{connection_id}` (SET NX PX + heartbeat): one sync per connection at a time; crash → TTL expiry → replica resumes; the watermark + unique constraints make re-ingest idempotent. **Per-tick page/time budget** so one slow account cannot stretch gate occupancy for the whole connection (ops MINOR 3).
4. **Connector interface** (per provider):

```ts
interface BankConnector {
  fetchTransactions(ctx, account, from: Date, to: Date): Promise<BankTxnPage>;  // paginated
  fetchBalance(ctx, account): Promise<Balance>;
  initiatePayout(ctx, account, req: PayoutRequest): Promise<PayoutSubmission>;  // idempotent by key
  getPayoutStatus(ctx, account, bankRef: string): Promise<PayoutStatus>;        // may be unsupported
}
```

   Reads retry with backoff + jitter; writes never blind-retry. `fetchBalance` is **wired** (ops MISSED 1, requirements MAJOR 1): each sync tick stores an `account_balance_snapshots` row (source=bank) and compares to the `account_balances` ledger aggregate — drift beyond tolerance alerts ops and is reviewable (catches late/missed transactions the watermark overlap cannot). The `mock` connector scripts the bad days: delayed txn, wrong amount, UTR reuse, submission timeout, mid-sync crash, reversal.

---

## 9. Event system

Envelope: `{ event_id, type, tenant_id, aggregate_type, aggregate_id, occurred_at, schema_version, payload }` — published to **one `events` stream** (v2) with one consumer group per handler kind.

| Event | Producer | Consumer |
|---|---|---|
| payin.order_created / payin.succeeded / payin.mismatch_review_created / payin.expired / payin.reversed | api / matcher / sweeper | webhook dispatcher |
| payout.created | admin api | payout executor |
| payout.submitted / settled / failed / unknown | executor | webhook dispatcher |
| payout lifecycle (settled/failed) | reconciler | webhook dispatcher |

v2 cuts (ops SIMPLIFY 3): `bank.txns_ingested` and `payin.claimed` events (matching is inline at claim + in sync's commit; the 1-min re-match sweep is the backstop); `payout.check_due` and `maintenance.sweep_done` (the DB sweep is the deadline mechanism; metrics are gauges, not events).

**Retry/DLQ policy** (ops MAJOR 2, pinned): handler error → no XACK; attempt count in a Redis hash by `event_id`; after **5 attempts** → XADD to `ev.dlq`, XACK, page (DLQ depth > 0); admin replay command re-XADDs and clears. Stuck PEL entries reclaimed by XAUTOCLAIM after 60 s idle. No `processed_events` table — guards are the dedupe.

**Outbound webhooks** are events: per-endpoint filter, HMAC-SHA256 signature + timestamp replay window, exponential backoff retries driven by the sweeper, `UNIQUE (event_id, endpoint_id)` = idempotent dispatch.

---

## 10. Background loops (worker role) — v2: 7 → 4

| Loop | Cadence | Job |
|---|---|---|
| **Outbox relay** | 100 ms | Claim unpublished rows (60 s claim expiry) → XADD to `events` → mark published. |
| **Bank sync** | per connection (30–60 s) | Rate-limit → gate=statement → `fetchTransactions(watermark − overlap, now)` → upsert (unique keys dedupe) → **inline match live claims in the same commit** → advance cursor → gate=statement released; every Nth tick: `fetchBalance` → snapshot + drift check. Page/time budget per tick. |
| **Payout executor** | stream consumer | On `payout.created`: select account (tenant+account balance, limits) → **acquire gate → place hold** (one txn: FOR UPDATE aggregates, re-verify limits, conditional transition, ledger group, outbox) → submit with idempotency key → state + events. Rejected/failed/unknown paths per §7. |
| **Master sweeper** | per-task interval | 30 s: order expiry · claim deadline · payout saga staleness (created/held/in_flight/unknown via `next_check_at`) · webhook retries · stale-gate report · DLQ attempts. 1 min: re-match backstop. 15 min: unknown-credit escalation. 24 h: balance verifier recompute + drift alert. |

All loops are N-replica safe (claims, conditional transitions, consumer groups). **Graceful drain** (ops MAJOR 5): on SIGTERM — stop claiming events; finish in-flight bank submissions and their state commits; release gates/locks held by this process; exit bounded at 30 s.

---

## 11. Core flow walkthroughs

### Pay-in (happy + delayed-sync)

```
1. POST /v1/payin-orders {amount, client_order_id}                [tenant, idempotent]
2. api txn: lock account_balances row → capacity vs windows (§6)
   → INSERT payin_order(pending, expires_at +30 m) + outbox(payin.order_created)
   → none fits → 422 NO_CAPACITY
3. Response: {order_id, bank:{account_number, ifsc, holder}, expires_at}
4. Customer pays in bank app → UTR in hand
5. POST /claim {utr}  → conditional UPDATE to claimed (re-checks expires_at in-txn)
   → inline match vs ingested credits (one matcher module):
       found ∧ amount ok    → succeeded NOW (ledger credit + webhook, one txn)
       found ∧ amount ≠     → mismatch_review(utr_mismatch)
       duplicate live UTR   → ER_DUP_ENTRY → mismatch_review(utr_reuse)
       not found            → claimed, utr_deadline = claimed_at + 48 h
6. Sync ingests the credit later → inline match in the sync commit → succeeded
7. Deadline, no evidence → mismatch_review(no_txn_found). Late exact credit →
   re-match sweep / ops resolution → succeeded. Very late credit to an expired
   order → unknown_credit review. Never auto-fail; never credit without evidence.
```

### Payout

```
1. Admin: POST /v1/payouts {amount, beneficiary, idempotency_key} → payouts(created, next_check_at=+1 m)
2. Executor (on payout.created):
   a. select account: status/capability → tenant available ≥ X ∧ account available ≥ X
      ∧ window limits (§6) ∧ gate acquirable. None → rejected, per-account reasons.
   b. ONE txn: acquire gate=payout → FOR UPDATE aggregates → re-verify limits/balances
      → hold group + conditional created→held + outbox.
   c. Submit (idempotency key): ok → in_flight · reject → failed (hold released)
      · timeout → unknown (gate released either way)
3. Reconciler (sweeper): unknown/in_flight past next_check_at → status check / statement
   search by idempotency key → settled | failed(evidence) | payout_ambiguous(review, hold kept)
4. settled → consume hold + webhook. failed → release hold + webhook.
```

Cancel: admin-only, valid in created/held, conditional transition, releases hold. Retry after failed: **new idempotency key** (same key returns the old terminal result — documented, not an error).

---

## 12. API surface (v1)

Tenant (api-key auth, idempotency keys): `POST /v1/payin-orders` · `POST /v1/payin-orders/:id/claim` · `POST /v1/payin-orders/:id/cancel` · `GET /v1/payin-orders/:id` · `POST /v1/payouts` · `GET /v1/payouts/:id` · `GET /v1/accounts` · `GET /v1/balance`

Admin (RBAC ops/finance/superadmin; every action audited): tenants/connections/accounts CRUD + limits; payout create/cancel; **review queue: list / resolve (money-moving kinds restricted to finance — §17 Q2) / reject**; manual sync trigger; **runbook endpoints** (ops MISSED 3): DLQ replay, force-clear stale gate, resolve stuck payout (evidence required), audited manual bank-transaction entry, account-disable policy (accounts with open orders/holds keep syncing until drained — requirements MAJOR 3).

Outbound: signed webhooks for terminal and notable intermediate states.

---

## 13. Security & multi-tenancy

- `tenant_id` scoping in one repository layer; cross-tenant access tests are first-class.
- Credentials/secrets AES-256-GCM at rest (env/KMS key), never logged; account numbers masked (last 4).
- RBAC + audit on every admin mutation; review resolutions are audited and idempotent.
- Webhooks: HMAC-SHA256 + timestamp replay window.
- zod on all input; parameterized SQL only.

## 14. Observability & testing

Metrics with **page thresholds and owner** (ops MISSED 2, on-call = ops):

| Signal | Warn | Page |
|---|---|---|
| Outbox unpublished oldest | > 5 s | > 60 s |
| Stream consumer lag (PEL) | > 200 | > 1 000 |
| Sync watermark age | > 2× interval | > 10× interval |
| DLQ depth | — | > 0 |
| Open reviews | > 50 | > 200 |
| `held_paise > 0` with no non-terminal payout | — | > 0 |
| Unknown credit unprocessed | > 1 h | > 4 h |

Tests: unit (selection, window math, matcher corroboration, guard matrix); integration via testcontainers MySQL+Redis + mock connector scenarios — delayed txn, wrong amount, UTR reuse (both orders), submission timeout, **kill-worker mid-hold → stuck-held recovery**, kill-worker mid-payout → unknown path, Redis flush mid-flight, reversal after success, deadlock retry; ledger invariant test: every group sums to zero, aggregates equal ledger at all times; connector contract tests against recorded fixtures.

docker-compose: mysql, redis, api, worker.

## 15. Repository layout

```
src/
  config/  db/  platform/          # locks, gate, rate limiters, clock, crypto, ids, errors
  modules/
    tenants/  bank-connections/ (connectors/)  bank-accounts/ (selection/)
    payin/    # orders, claims, ONE shared matcher module
    payout/   # orders, executor, reconcile
    ledger/   # ledger_txns, entries, holds, balances, verifier
    events/   # outbox, relay, consumer kit (claim → guard → effect → ack)
    reviews/  webhook/  admin/    # incl. runbook endpoints
  http/  workers/  app/           # entrypoints: api.ts / worker.ts
docker-compose.yml
```

## 16. Delivery milestones

| M | Deliverable | Acceptance |
|---|---|---|
| M1 | Foundation: schema + migrations, outbox→stream→consumer kit, gate, health | Event round-trip survives crash-replay + Redis-flush tests; group-sum-zero invariant test green |
| M2 | Tenancy: tenants/connections/accounts, RBAC, api_keys, webhook endpoints, audit, mock connector | Admin models a tenant with 2 accounts + limits |
| M3 | Pay-in: selection, claim, inline two-sided matching + re-match sweep, sync + balance drift, credits, webhooks, reversal path | All mock scenarios pass: delayed sync, mismatch, UTR reuse, late claim, reversal |
| M4 | Payout: holds, executor (gate-before-hold), saga sweeper, unknown resolution | Kill-worker mid-hold and mid-payout tests: no double-pay, no lost hold, no wedge > 1 min |
| M5 | Real bank connector(s) + provider rate limits + review-queue ops flows | Live sandbox sync + payout + one review round-trip |
| M6 | Hardening: DLQ replay, metrics + thresholds, load test, runbooks, retention | Lag SLOs green for 7 days; runbook drill done |

## 17. Open questions (annotate in Plannotator — answers change the design)

**New, raised by the reviews:**
1. Payout funding model: per-account float vs pooled — v2 requires **both** checks (tenant pooled AND account physical). Confirm, or relax to pooled-only with an auto inter-account sweep?
2. Who resolves reviews and moves money — ops, finance, or both with different scopes? (v2 default: finance for money-moving kinds.)
3. Technical reversal policy: v2 auto-applies the storno + webhook + overdraw alert. Confirm auto vs review-first.
4. Money received on expired/wrong payments: credit tenant vs return to sender — who decides, what's the default? (Ledger escrow is bookkeeping only — the physical bank policy is the question.)

**Standing:**
5. Confirm "check-in" = bank reads (statement/balance fetch), the op that blocks payouts.
6. Direct bank APIs or an aggregator (Decentro/Yes Bank-style)?
7. Shared tenant accounts matched by UTR vs virtual accounts per end-customer?
8. Does the bank give definitive payout status, or statement-only confirmation?
9. Defaults sanity: order expiry 30 m, claim window 48 h, unknown-payout max reconcile age 24 h?
10. Volume ballpark: orders/day, accounts per tenant, sync frequency?
11. Tenant auth: static API keys (v2 ships api_keys) vs OAuth2 client credentials?

## 18. Out of scope (v1)

Multi-currency · FX · chargebacks/disputes · Kafka federation · per-tenant physical DB isolation (door open via repository layer) · end-customer KYC · payer-facing review UX (tenant webhooks only).

## 19. Review record (v2 change log)

Four independent reviewers, all verdict **fixable**; core (MySQL-truth + outbox + streams + modular monolith + hold-first payouts + two-sided matching) endorsed by all four. Folded in:

| Reviewer | Key findings → v2 changes |
|---|---|
| Money-safety | Ledger sign fix (CRITICAL) · payout saga sweep for all non-terminal states (CRITICAL) · conditional-UPDATE guards + UNIQUE(matched_bank_txn_id) + FOR UPDATE on the matched bank txn (CRITICAL) · 1-min re-match sweep · gate fencing + best-effort lease documented · unknown→failed needs positive evidence · limits re-verified in hold txn · fixed lock ordering + deadlock retry · aggregator re-report dedupe · pay-in reversal path (MISSED) · guarded idempotent review resolutions (MISSED) · UNIQUE live-claims (MISSED) |
| Simplicity/ops | held-deadline recovery + gate-before-hold (CRITICAL) · both gate halves specified + stale-gate sweeper (now `bank_gates` table) · retry/DLQ policy pinned (5 attempts → DLQ + replay) · janitor spec + retention · ledger sign fix + invariant test · graceful drain · **7 loops → 4** (master sweeper) · one events stream, drop processed_events, inline matching · outbox claim-based relay · per-tick sync budgets · metrics thresholds + owner (MISSED) · runbook admin endpoints (MISSED) · balance drift check wired into sync (MISSED) |
| Requirements-fit | per-account available balance required at selection (CRITICAL) · mismatch_review wired exits + per-kind resolution actions + RBAC (CRITICAL) · fetchBalance → snapshots + drift recon · concurrent same-UTR claims → utr_reuse review on ingest · disable-mid-flight keeps syncing + manual bank-txn entry · admin cancel endpoint + retry = new idempotency key · late claims accepted into review · new open questions Q1–Q4 · dropped processed_events (SIMPLIFY) |
| Data-model | UNIQUE constraints for money dedupe (CRITICAL) · usage/sweeper indexes (account_id,status,created_at) + (status,expires_at) + (status,utr_deadline) · ledger/webhook/review/bank_accounts indexes · reviews ref_type/ref_id + resolved_* · webhook_endpoints table + UNIQUE(event_id,endpoint_id) · connection-scope gate flag · ENUM→VARCHAR+CHECK + providers lookup · payouts.currency · outbox.tenant_id · ledger_accounts UNIQUE(kind,key) + key format + ledger_txns table · audit_log.tenant_id · sync cursor timestamps · account_balance_snapshots (MISSED) · api_keys/admin_users (MISSED) · retention (MISSED) |
