import { insert, one, q, run, withTxn, type Db } from "./db.ts";
import type { Clock } from "./clock.ts";
import { postGroup } from "./ledger.ts";
import type { Outbox } from "./outbox.ts";
import type { Gate } from "./gate.ts";
import type { BankConnector, PayoutStatus, PayoutSubmission } from "../platform/connector.ts";
import { createReview, resolveReviewRow } from "./reviews.ts";

/** Payout saga (plan §7, §11) — ambiguity-safe by construction:
 *  - select account: BOTH tenant-available AND account-available minus active
 *    holds (review CRITICAL: pooled balance alone can pick an unfunded account)
 *  - ONE transaction: acquire gate → re-verify → hold (gate-before-hold; a
 *    rolled-back hold rolls the gate back with it)
 *  - submit exactly once, idempotency key carried to the bank; never blind-retried
 *  - timeout → unknown with the hold KEPT; failed only on positive non-execution
 *    evidence; the fake bank's not_found is contractually definitive
 *  - cancel ONLY from created (review CRITICAL: never release a hold that may
 *    be mid-submit)
 *  - every non-terminal row carries next_check_at; the sweeper escalates. */

// env-overridable timing so the e2e can shrink recovery windows (defaults = plan values)
export const EXEC_DELAY_MS = Number(process.env.EXEC_DELAY_MS ?? 1_000);
export const STALE_HELD_MS = Number(process.env.STALE_HELD_MS ?? 60_000);
export const RECONCILE_DELAY_MS = Number(process.env.RECONCILE_DELAY_MS ?? 30_000);
export const MAX_RECONCILE_AGE_MS = Number(process.env.MAX_RECONCILE_AGE_MS ?? 24 * 3_600_000);
export const DEFER_BACKOFF_MS = Number(process.env.DEFER_BACKOFF_MS ?? 5_000);

type PayoutRow = {
  id: number;
  tenant_id: number;
  account_id: number | null;
  idempotency_key: string;
  amount_paise: number;
  beneficiary: string;
  status: string;
  next_check_at: number;
  attempts: number;
  created_at: number;
  bank_ref: string | null;
};

const PAYOUT_COLS =
  "id, tenant_id, account_id, idempotency_key, amount_paise, beneficiary, status, next_check_at, attempts, created_at, bank_ref";

class Defer extends Error {}

export function makePayout({ db, clock, outbox, gate, bank, instanceId = "" }: { db: Db; clock: Clock; outbox: Outbox; gate: Gate; bank: BankConnector; instanceId?: string }) {
  function loadPayout(payoutId: number): PayoutRow | undefined {
    return one<PayoutRow>(db, "SELECT " + PAYOUT_COLS + " FROM payouts WHERE id = ?", payoutId);
  }

  /** Idempotent create: same (tenant, key) returns the existing payout — even
   *  terminal ones (retry after failure needs a NEW key, plan §11). */
  function create(e: { tenantId: number; amountPaise: number; beneficiary: string; idempotencyKey: string }):
    | { ok: true; payoutId: number; status: string; existing: boolean }
    | { ok: false; error: string } {
    return withTxn(db, () => {
      const existing = one<{ id: number; status: string }>(
        db,
        "SELECT id, status FROM payouts WHERE tenant_id = ? AND idempotency_key = ?",
        e.tenantId,
        e.idempotencyKey
      );
      if (existing) return { ok: true, payoutId: existing.id, status: existing.status, existing: true };
      const id = insert(
        db,
        "INSERT INTO payouts (tenant_id, idempotency_key, amount_paise, beneficiary, status, next_check_at, attempts, created_at) VALUES (?, ?, ?, ?, 'created', ?, 0, ?)",
        e.tenantId,
        e.idempotencyKey,
        e.amountPaise,
        e.beneficiary,
        clock.now() + EXEC_DELAY_MS,
        clock.now()
      );
      outbox.emit({
        type: "payout.created",
        tenantId: e.tenantId,
        aggregateType: "payout",
        aggregateId: id,
        payload: { payoutId: id, amountPaise: e.amountPaise },
      });
      return { ok: true, payoutId: id, status: "created", existing: false };
    });
  }

  function activeHoldsFor(accountId: number): number {
    return one<{ s: number }>(
      db,
      "SELECT COALESCE(SUM(amount_paise), 0) AS s FROM payouts WHERE account_id = ? AND status IN ('held','in_flight','unknown')",
      accountId
    )!.s;
  }

  function selectAccount(tenantId: number, amountPaise: number, tenantAvailable: number): number | null {
    if (tenantAvailable < amountPaise) return null;
    const accounts = q<{ id: number }>(
      db,
      "SELECT id FROM bank_accounts WHERE tenant_id = ? AND supports_payout = 1 AND status = 'active' ORDER BY priority ASC, id ASC",
      tenantId
    );
    for (const a of accounts) {
      const ab = one<{ available_paise: number }>(db, "SELECT available_paise FROM account_balances WHERE account_id = ?", a.id)!;
      if (ab.available_paise - activeHoldsFor(a.id) >= amountPaise) return a.id;
    }
    return null;
  }

  /** The executor: also the outbox handler for payout.created. Idempotent —
   *  the created→held guard is the dedupe for redelivery and concurrent runs. */
  async function execute(
    payoutId: number,
    opts: { crashAfterHold?: boolean; crashAfterSubmit?: boolean } = {}
  ): Promise<{ status: string }> {
    const p = loadPayout(payoutId);
    if (!p) return { status: "gone" };
    if (p.status !== "created") return { status: p.status }; // someone else moved it — no-op

    const tb = one<{ available_paise: number }>(db, "SELECT available_paise FROM tenant_balances WHERE tenant_id = ?", p.tenant_id)!;
    const account = selectAccount(p.tenant_id, p.amount_paise, tb.available_paise);
    if (account === null) {
      withTxn(db, () => {
        if (run(db, "UPDATE payouts SET status = 'rejected', next_check_at = ? WHERE id = ? AND status = 'created'", clock.now(), payoutId) === 1) {
          outbox.emit({
            type: "payout.rejected",
            tenantId: p.tenant_id,
            aggregateType: "payout",
            aggregateId: payoutId,
            payload: { payoutId, reason: "no eligible account (tenant or physical funds)" },
          });
        }
      });
      return { status: "rejected" };
    }

    // ONE transaction: acquire gate → re-verify funds → hold
    // instance-scoped holder: a second worker must never satisfy the gate's
    // same-holder branch on a lease another process holds
    const holder = "payout:exec:" + payoutId + (instanceId ? ":" + instanceId : "");
    try {
      withTxn(db, () => {
        if (!gate.acquire("account", account, "payout", holder)) throw new Defer("gate_busy"); // statements win; payouts defer
        const tbNow = one<{ available_paise: number }>(db, "SELECT available_paise FROM tenant_balances WHERE tenant_id = ?", p.tenant_id)!;
        if (tbNow.available_paise < p.amount_paise) throw new Defer("funds_changed");
        const ab = one<{ available_paise: number }>(db, "SELECT available_paise FROM account_balances WHERE account_id = ?", account)!;
        if (ab.available_paise - activeHoldsFor(account) < p.amount_paise) throw new Defer("account_funds_changed");
        postGroup(
          db,
          { tenantId: p.tenant_id, kind: "payout_hold", refType: "payout", refId: payoutId, at: clock.now() },
          [
            { account: { kind: "tenant_operating", tenantId: p.tenant_id }, direction: "debit", amountPaise: p.amount_paise },
            { account: { kind: "tenant_holds", tenantId: p.tenant_id }, direction: "credit", amountPaise: p.amount_paise },
          ]
        );
        const changed = run(
          db,
          "UPDATE payouts SET status = 'held', account_id = ?, next_check_at = ? WHERE id = ? AND status = 'created'",
          account,
          clock.now() + STALE_HELD_MS,
          payoutId
        );
        if (changed !== 1) throw new Defer("stale_created"); // gate acquire rolls back with the txn
        outbox.emit({
          type: "payout.held",
          tenantId: p.tenant_id,
          aggregateType: "payout",
          aggregateId: payoutId,
          payload: { payoutId, accountId: account, amountPaise: p.amount_paise },
        });
      });
    } catch (e) {
      if (e instanceof Defer) {
        withTxn(db, () => {
          run(db, "UPDATE payouts SET next_check_at = ? WHERE id = ? AND status = 'created'", clock.now() + DEFER_BACKOFF_MS, payoutId);
        });
        return { status: "deferred" };
      }
      throw e;
    }

    if (opts.crashAfterHold) throw new Error("WORKER_CRASH_AFTER_HOLD"); // gate row orphaned until TTL; sweeper recovers

    // fencing: re-verify we still hold the gate before touching the bank
    if (!gate.heldBy("account", account, holder)) return { status: "aborted_gate_lost" };

    let sub: PayoutSubmission;
    try {
      sub = await bank.initiatePayout(account, {
        idempotencyKey: p.idempotency_key,
        amountPaise: p.amount_paise,
        beneficiary: p.beneficiary,
      });
      if (opts.crashAfterSubmit) throw new Error("WORKER_CRASH_AFTER_SUBMIT");
    } catch (e) {
      if (opts.crashAfterSubmit || (e instanceof Error && e.message === "WORKER_CRASH_AFTER_SUBMIT")) {
        throw new Error("WORKER_CRASH_AFTER_SUBMIT"); // die exactly like a crashed worker: row stays held
      }
      // timeout / ambiguous outcome: → unknown, hold KEPT, gate released either way
      gate.release("account", account, holder);
      withTxn(db, () => {
        if (
          run(
            db,
            "UPDATE payouts SET status = 'unknown', attempts = attempts + 1, next_check_at = ? WHERE id = ? AND status = 'held'",
            clock.now() + RECONCILE_DELAY_MS,
            payoutId
          ) === 1
        ) {
          outbox.emit({
            type: "payout.unknown",
            tenantId: p.tenant_id,
            aggregateType: "payout",
            aggregateId: payoutId,
            payload: { payoutId, reason: "submit outcome ambiguous (timeout)" },
          });
        }
      });
      return { status: "unknown" };
    }

    if (sub.status === "rejected") {
      // positive bank evidence of non-execution → failed + release hold
      gate.release("account", account, holder);
      withTxn(db, () => {
        if (failGuard(payoutId, "held", "failed", "BANK_REJECTED")) {
          releaseHold(payoutId, p.tenant_id, p.amount_paise);
          outbox.emit({
            type: "payout.failed",
            tenantId: p.tenant_id,
            aggregateType: "payout",
            aggregateId: payoutId,
            payload: { payoutId, reason: "bank rejected", error: "BANK_REJECTED_INSUFFICIENT_FUNDS" },
          });
        }
      });
      return { status: "failed" };
    }

    withTxn(db, () => {
      if (
        run(
          db,
          "UPDATE payouts SET status = 'in_flight', bank_ref = ?, next_check_at = ? WHERE id = ? AND status = 'held'",
          sub.status === "submitted" ? sub.bankRef : "",
          clock.now() + RECONCILE_DELAY_MS,
          payoutId
        ) === 1
      ) {
        outbox.emit({
          type: "payout.submitted",
          tenantId: p.tenant_id,
          aggregateType: "payout",
          aggregateId: payoutId,
          payload: { payoutId, bankRef: sub.status === "submitted" ? sub.bankRef : "" },
        });
      }
    });
    gate.release("account", account, holder);
    return { status: "in_flight" };
  }

  function failGuard(payoutId: number, from: string, to: string, evidence: string): boolean {
    return run(
      db,
      "UPDATE payouts SET status = ?, bank_error = ?, next_check_at = ?, attempts = attempts + 1 WHERE id = ? AND status = ?",
      to,
      evidence,
      clock.now(),
      payoutId,
      from
    ) === 1;
  }

  /** settle: consume the hold, money has left the bank (tenant_holds → bank_mirror). */
  function settle(payoutId: number, evidence: string): boolean {
    return withTxn(db, () => {
      const p = loadPayout(payoutId);
      if (!p || !p.account_id) return false;
      if (
        run(
          db,
          "UPDATE payouts SET status = 'settled', bank_error = ?, next_check_at = ? WHERE id = ? AND status IN ('held','in_flight','unknown')",
          evidence,
          clock.now(),
          payoutId
        ) !== 1
      ) {
        return false;
      }
      postGroup(
        db,
        { tenantId: p.tenant_id, kind: "payout_settled", refType: "payout", refId: payoutId, at: clock.now() },
        [
          { account: { kind: "tenant_holds", tenantId: p.tenant_id }, direction: "debit", amountPaise: p.amount_paise },
          { account: { kind: "bank_mirror", accountId: p.account_id }, direction: "credit", amountPaise: p.amount_paise },
        ]
      );
      outbox.emit({
        type: "payout.settled",
        tenantId: p.tenant_id,
        aggregateType: "payout",
        aggregateId: payoutId,
        payload: { payoutId, evidence },
      });
      autoResolveAmbiguous(db, payoutId, "auto_settled_with_evidence");
      return true;
    });
  }

  /** failed only with positive non-execution evidence — release the hold. */
  function failWithEvidence(payoutId: number, evidence: string): boolean {
    return withTxn(db, () => {
      const p = loadPayout(payoutId);
      if (!p || !p.account_id) return false;
      if (
        run(
          db,
          "UPDATE payouts SET status = 'failed', bank_error = ?, next_check_at = ? WHERE id = ? AND status IN ('held','in_flight','unknown')",
          evidence,
          clock.now(),
          payoutId
        ) !== 1
      ) {
        return false;
      }
      releaseHold(payoutId, p.tenant_id, p.amount_paise);
      outbox.emit({
        type: "payout.failed",
        tenantId: p.tenant_id,
        aggregateType: "payout",
        aggregateId: payoutId,
        payload: { payoutId, evidence },
      });
      autoResolveAmbiguous(db, payoutId, "auto_failed_with_evidence");
      return true;
    });
  }

  function releaseHold(payoutId: number, tenantId: number, amountPaise: number): void {
    postGroup(
      db,
      { tenantId, kind: "payout_hold_released", refType: "payout", refId: payoutId, at: clock.now() },
      [
        { account: { kind: "tenant_holds", tenantId }, direction: "debit", amountPaise },
        { account: { kind: "tenant_operating", tenantId }, direction: "credit", amountPaise },
      ]
    );
  }

  function autoResolveAmbiguous(db2: Db, payoutId: number, action: string): void {
    run(
      db2,
      "UPDATE reviews SET status = 'resolved', resolved_action = ? WHERE kind = 'payout_ambiguous' AND ref_type = 'payout' AND ref_id = ? AND status = 'open'",
      action,
      payoutId
    );
  }

  function maybeAmbiguous(p: PayoutRow): void {
    if (p.status !== "unknown") return;
    if (clock.now() - p.created_at <= MAX_RECONCILE_AGE_MS) return;
    withTxn(db, () => {
      const r = createReview(db, {
        tenantId: p.tenant_id,
        kind: "payout_ambiguous",
        refType: "payout",
        refId: p.id,
        payload: { payoutId: p.id, idempotencyKey: p.idempotency_key, attempts: p.attempts, reason: "max reconcile age exceeded without positive evidence; hold kept" },
        at: clock.now(),
      });
      if (r.created) {
        outbox.emit({
          type: "payout.ambiguous_review_created",
          tenantId: p.tenant_id,
          aggregateType: "review",
          aggregateId: r.reviewId,
          payload: { kind: "payout_ambiguous", payoutId: p.id, reviewId: r.reviewId },
        });
      }
    });
  }

  /** Reconciler for in_flight / unknown rows past next_check_at. */
  async function reconcile(payoutId: number): Promise<{ status: string }> {
    const p = loadPayout(payoutId);
    if (!p) return { status: "gone" };
    if (p.status !== "in_flight" && p.status !== "unknown") return { status: p.status };
    if (p.next_check_at > clock.now()) return { status: p.status };

    let st: PayoutStatus;
    try {
      st = await bank.getPayoutStatus(p.idempotency_key);
    } catch {
      // inconclusive: never fail on this — unknown keeps its hold and ages toward review
      withTxn(db, () => {
        run(
          db,
          "UPDATE payouts SET attempts = attempts + 1, next_check_at = ? WHERE id = ? AND status IN ('in_flight','unknown')",
          clock.now() + RECONCILE_DELAY_MS,
          payoutId
        );
      });
      maybeAmbiguous(loadPayout(payoutId)!);
      return { status: p.status };
    }
    withTxn(db, () => {
      run(
        db,
        "UPDATE payouts SET attempts = attempts + 1, next_check_at = ? WHERE id = ? AND status IN ('in_flight','unknown')",
        clock.now() + RECONCILE_DELAY_MS,
        payoutId
      );
    });
    if (st === "executed") {
      settle(payoutId, "bank_status_executed");
      return { status: "settled" };
    }
    if (st === "rejected" || st === "not_found") {
      failWithEvidence(payoutId, "bank_status_" + st);
      return { status: "failed" };
    }
    maybeAmbiguous(loadPayout(payoutId)!);
    return { status: p.status };
  }

  /** Stale-held recovery (crash after hold): query the bank by idempotency key —
   *  NEVER resubmit. Unavailable status → unknown with the hold kept. */
  async function heldStale(payoutId: number): Promise<{ status: string }> {
    const p = loadPayout(payoutId);
    if (!p) return { status: "gone" };
    if (p.status !== "held") return { status: p.status };

    let st: PayoutStatus;
    try {
      st = await bank.getPayoutStatus(p.idempotency_key);
    } catch {
      withTxn(db, () => {
        if (failGuard(payoutId, "held", "unknown", "held_stale_status_unavailable")) {
          outbox.emit({
            type: "payout.unknown",
            tenantId: p.tenant_id,
            aggregateType: "payout",
            aggregateId: payoutId,
            payload: { payoutId, reason: "held stale; bank status unavailable" },
          });
        }
      });
      return { status: "unknown" };
    }
    if (st === "executed") {
      settle(payoutId, "held_stale_bank_executed");
      return { status: "settled" };
    }
    if (st === "not_found") {
      // fake-bank contract: every submit is recorded durably, so not_found is
      // definitive proof the transfer never reached the bank
      failWithEvidence(payoutId, "bank_not_found_never_submitted");
      return { status: "failed" };
    }
    if (st === "rejected") {
      failWithEvidence(payoutId, "bank_rejected");
      return { status: "failed" };
    }
    // pending: the bank has it — it may still execute; watch it (hold kept)
    withTxn(db, () => {
      if (failGuard(payoutId, "held", "unknown", "held_stale_bank_pending")) {
        outbox.emit({
          type: "payout.unknown",
          tenantId: p.tenant_id,
          aggregateType: "payout",
          aggregateId: payoutId,
          payload: { payoutId, reason: "held stale; bank reports pending" },
        });
      }
    });
    return { status: "unknown" };
  }

  /** Cancel: ONLY from created. A held payout exits exclusively via
   *  evidence-based reconciliation (review CRITICAL). */
  function cancel(payoutId: number): { ok: true; status: string } | { ok: false; error: string } {
    return withTxn(db, () => {
      const p = one<{ tenant_id: number; status: string }>(db, "SELECT tenant_id, status FROM payouts WHERE id = ?", payoutId);
      if (!p) return { ok: false, error: "not_found" };
      if (run(db, "UPDATE payouts SET status = 'cancelled', next_check_at = ? WHERE id = ? AND status = 'created'", clock.now(), payoutId) !== 1) {
        return { ok: false, error: "only_created_cancellable" };
      }
      outbox.emit({
        type: "payout.cancelled",
        tenantId: p.tenant_id,
        aggregateType: "payout",
        aggregateId: payoutId,
        payload: { payoutId },
      });
      return { ok: true, status: "cancelled" };
    });
  }

  /** Ops resolution of a payout_ambiguous review — requires fresh positive
   *  bank evidence; the entity guard is the dedupe. */
  async function resolveAmbiguous(reviewId: number, action: "confirm_failed" | "confirm_settled"): Promise<{ ok: boolean; status?: string; error?: string }> {
    const r = one<{ id: number; tenant_id: number; ref_id: number; status: string; kind: string }>(
      db,
      "SELECT id, tenant_id, ref_id, status, kind FROM reviews WHERE id = ?",
      reviewId
    );
    if (!r || r.kind !== "payout_ambiguous") return { ok: false, error: "review_not_found" };
    if (r.status !== "open") return { ok: true, status: "replay" };
    const p = loadPayout(r.ref_id);
    if (!p || p.status !== "unknown") return { ok: false, error: "payout_not_unknown" };
    let st: PayoutStatus;
    try {
      st = await bank.getPayoutStatus(p.idempotency_key);
    } catch {
      return { ok: false, error: "evidence_unavailable" };
    }
    if (action === "confirm_settled") {
      if (st !== "executed") return { ok: false, error: "no_settlement_evidence" };
      if (!settle(p.id, "ops_confirmed_with_bank_evidence")) return { ok: false, error: "settle_guard_failed" };
      resolveReviewRow(db, reviewId, "confirm_settled");
      return { ok: true, status: "settled" };
    }
    if (action === "confirm_failed") {
      if (st !== "rejected" && st !== "not_found") return { ok: false, error: "no_non_execution_evidence" };
      if (!failWithEvidence(p.id, "ops_confirmed_" + st)) return { ok: false, error: "fail_guard_failed" };
      resolveReviewRow(db, reviewId, "confirm_failed");
      return { ok: true, status: "failed" };
    }
    return { ok: false, error: "unknown_action" };
  }

  /** Saga sweep (plan §10): every non-terminal row is due somewhere. */
  async function sweep(): Promise<{ drove: number; deferred: number; heldStale: number; reconciled: number }> {
    const due = q<{ id: number; status: string }>(
      db,
      "SELECT id, status FROM payouts WHERE status IN ('created','held','in_flight','unknown') AND next_check_at <= ? ORDER BY id",
      clock.now()
    );
    const out = { drove: 0, deferred: 0, heldStale: 0, reconciled: 0 };
    for (const row of due) {
      if (row.status === "created") {
        const r = await execute(row.id);
        if (r.status === "deferred") out.deferred++;
        else out.drove++;
      } else if (row.status === "held") {
        await heldStale(row.id);
        out.heldStale++;
      } else {
        await reconcile(row.id);
        out.reconciled++;
      }
    }
    return out;
  }

  return { create, execute, cancel, reconcile, heldStale, resolveAmbiguous, sweep };
}

export type Payout = ReturnType<typeof makePayout>;
