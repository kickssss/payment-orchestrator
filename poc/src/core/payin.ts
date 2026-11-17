import { insert, isUniqueViolation, one, q, run, withTxn, MAX_OPEN_ORDERS_PER_ACCOUNT, type Db } from "./db.ts";
import type { Clock } from "./clock.ts";
import { postGroup } from "./ledger.ts";
import type { Outbox } from "./outbox.ts";
import { findClaimForCredit, findCreditForClaim, findExactCreditForClaim, findReversalTarget } from "./matcher.ts";
import { createReview, resolveReviewRow } from "./reviews.ts";
import type { BankTxn } from "../platform/fakebank.ts";

/** Pay-in flow (plan §7, §11): evidence-only credit. Success happens ONLY
 *  against a corroborated ingested bank credit (UTR + credit direction +
 *  displayed account + exact amount), through ONE guarded money move shared by
 *  every path (claim-time, ingest-time, rematch, ops resolution). Unique
 *  backstops: (account_id, active_utr) makes duplicate live UTR claims
 *  structurally impossible; matched_bank_txn_id UNIQUE makes one-credit-one-
 *  order structurally impossible. Unmatched money parks in escrow at ingest
 *  (review finding: bank_mirror must mirror physical reality, matched or not). */

export const ORDER_TTL_MS = 30 * 60_000;
export const CLAIM_WINDOW_MS = 48 * 3_600_000;
export const OLD_CREDIT_MS = 15 * 60_000;

type PayinOrder = {
  id: number;
  tenant_id: number;
  account_id: number;
  amount_paise: number;
  status: string;
  claimed_utr: string | null;
  expires_at: number;
  utr_deadline: number | null;
};

const ORDER_COLS = "id, tenant_id, account_id, amount_paise, status, claimed_utr, expires_at, utr_deadline";

export type ClaimResult =
  | { status: "succeeded" }
  | { status: "claimed" }
  | { status: "mismatch_review"; reviewId: number }
  | { status: "utr_reuse_review"; reviewId: number }
  | { status: "late_claim_review"; reviewId: number }
  | { status: "in_review" }
  | { status: "gone" };

export function makePayin({ db, clock, outbox }: { db: Db; clock: Clock; outbox: Outbox }) {
  function createOrder(e: { accountId: number; amountPaise: number; clientOrderId: string }):
    | { ok: true; orderId: number }
    | { ok: false; error: "ACCOUNT_NOT_FOUND" | "CLIENT_ORDER_EXISTS" | "NO_CAPACITY" } {
    return withTxn(db, () => {
      const account = one<{ tenant_id: number; supports_payin: number; status: string }>(
        db,
        "SELECT tenant_id, supports_payin, status FROM bank_accounts WHERE id = ?",
        e.accountId
      );
      if (!account || !account.supports_payin || account.status !== "active") return { ok: false, error: "ACCOUNT_NOT_FOUND" };
      if (one(db, "SELECT id FROM payin_orders WHERE tenant_id = ? AND client_order_id = ?", account.tenant_id, e.clientOrderId)) {
        return { ok: false, error: "CLIENT_ORDER_EXISTS" };
      }
      // lock-then-check-then-act: BEGIN IMMEDIATE holds the write lock, so two
      // concurrent creates cannot both pass the last open slot.
      const open = one<{ n: number }>(
        db,
        "SELECT COUNT(*) AS n FROM payin_orders WHERE account_id = ? AND status IN ('pending','claimed')",
        e.accountId
      )!.n;
      if (open >= MAX_OPEN_ORDERS_PER_ACCOUNT) return { ok: false, error: "NO_CAPACITY" };
      const orderId = insert(
        db,
        "INSERT INTO payin_orders (tenant_id, account_id, client_order_id, amount_paise, status, expires_at) VALUES (?, ?, ?, ?, 'pending', ?)",
        account.tenant_id,
        e.accountId,
        e.clientOrderId,
        e.amountPaise,
        clock.now() + ORDER_TTL_MS
      );
      outbox.emit({
        type: "payin.order_created",
        tenantId: account.tenant_id,
        aggregateType: "payin_order",
        aggregateId: orderId,
        payload: { orderId, amountPaise: e.amountPaise, accountId: e.accountId },
      });
      return { ok: true, orderId };
    });
  }

  function loadOrder(orderId: number): PayinOrder | undefined {
    return one<PayinOrder>(db, "SELECT " + ORDER_COLS + " FROM payin_orders WHERE id = ?", orderId);
  }

  function claim(e: { orderId: number; utr: string }): ClaimResult {
    return withTxn(db, () => {
      const o = loadOrder(e.orderId);
      if (!o) return { status: "gone" };
      const now = clock.now();
      if (o.status === "succeeded" || o.status === "reversed" || o.status === "cancelled") return { status: "gone" };
      if (o.status === "mismatch_review") return { status: "in_review" };
      if (o.status === "expired") return lateClaimReview(o, e.utr, now);

      if (o.status === "pending") {
        if (o.expires_at <= now) {
          if (run(db, "UPDATE payin_orders SET status = 'expired' WHERE id = ? AND status = 'pending'", o.id) === 1) {
            outbox.emit({ type: "payin.expired", tenantId: o.tenant_id, aggregateType: "payin_order", aggregateId: o.id, payload: { orderId: o.id } });
          }
          return lateClaimReview(o, e.utr, now);
        }
        try {
          const changed = run(
            db,
            "UPDATE payin_orders SET status = 'claimed', claimed_utr = ?, claimed_at = ?, utr_deadline = ? WHERE id = ? AND status = 'pending'",
            e.utr,
            now,
            now + CLAIM_WINDOW_MS,
            o.id
          );
          if (changed !== 1) return { status: "gone" };
        } catch (err) {
          if (isUniqueViolation(err, "payin_orders", "active_utr")) return reuseReview(o, e.utr, now);
          throw err;
        }
        return inlineMatch(o.id, now);
      }

      if (o.status === "claimed") {
        if (o.claimed_utr === e.utr) return { status: "claimed" }; // idempotent re-claim
        // audited re-claim: replace the UTR while claimed (plan §7)
        try {
          const changed = run(
            db,
            "UPDATE payin_orders SET claimed_utr = ? WHERE id = ? AND status = 'claimed' AND claimed_utr = ?",
            e.utr,
            o.id,
            o.claimed_utr
          );
          if (changed !== 1) return { status: "gone" };
        } catch (err) {
          if (isUniqueViolation(err, "payin_orders", "active_utr")) return reuseReview(o, e.utr, now);
          throw err;
        }
        return inlineMatch(o.id, now);
      }
      return { status: "gone" };
    });
  }

  function lateClaimReview(o: PayinOrder, utr: string, now: number): ClaimResult {
    // claim after expiry is accepted into review (plan: never auto-fail), UTR preserved
    const r = createReview(db, {
      tenantId: o.tenant_id,
      kind: "late_claim",
      refType: "payin_order",
      refId: o.id,
      payload: { orderId: o.id, utr, reason: "claim after expiry" },
      at: now,
    });
    outbox.emit({
      type: "payin.mismatch_review_created",
      tenantId: o.tenant_id,
      aggregateType: "review",
      aggregateId: r.reviewId,
      payload: { kind: "late_claim", orderId: o.id, reviewId: r.reviewId, utr },
    });
    return { status: "late_claim_review", reviewId: r.reviewId };
  }

  function reuseReview(o: PayinOrder, utr: string, now: number): ClaimResult {
    // duplicate live UTR — the (account_id, active_utr) unique index fired
    const r = createReview(db, {
      tenantId: o.tenant_id,
      kind: "utr_reuse",
      refType: "payin_order",
      refId: o.id,
      payload: { orderId: o.id, utr, reason: "UTR already claimed by a live order on this account" },
      at: now,
    });
    outbox.emit({
      type: "payin.mismatch_review_created",
      tenantId: o.tenant_id,
      aggregateType: "review",
      aggregateId: r.reviewId,
      payload: { kind: "utr_reuse", orderId: o.id, reviewId: r.reviewId, utr },
    });
    return { status: "utr_reuse_review", reviewId: r.reviewId };
  }

  function inlineMatch(orderId: number, now: number): ClaimResult {
    const o = loadOrder(orderId)!;
    const credit = findCreditForClaim(db, o.account_id, o.claimed_utr!);
    if (!credit) return { status: "claimed" };
    if (credit.amountPaise !== o.amount_paise) {
      const changed = run(db, "UPDATE payin_orders SET status = 'mismatch_review' WHERE id = ? AND status = 'claimed'", orderId);
      if (changed !== 1) throw new Error("claim guard failed unexpectedly for order " + orderId);
      const r = createReview(db, {
        tenantId: o.tenant_id,
        kind: "utr_mismatch",
        refType: "payin_order",
        refId: orderId,
        payload: { expectedPaise: o.amount_paise, actualPaise: credit.amountPaise, utr: o.claimed_utr },
        suggestedRefId: credit.txnId,
        at: now,
      });
      outbox.emit({
        type: "payin.mismatch_review_created",
        tenantId: o.tenant_id,
        aggregateType: "review",
        aggregateId: r.reviewId,
        payload: { kind: "utr_mismatch", orderId, reviewId: r.reviewId, utr: o.claimed_utr },
      });
      return { status: "mismatch_review", reviewId: r.reviewId };
    }
    succeedOrder(orderId, credit.txnId, credit.amountPaise, now); // guard is the dedupe
    return { status: "succeeded" };
  }

  /** THE one guarded money move: order → succeeded against a specific bank credit.
   *  Returns false when the guard or a unique backstop says someone else already
   *  did it — callers treat that as a no-op, never an error. */
  function succeedOrder(orderId: number, bankTxnId: number, amountPaise: number, now: number): boolean {
    let changed = 0;
    try {
      changed = run(
        db,
        "UPDATE payin_orders SET status = 'succeeded', matched_bank_txn_id = ? WHERE id = ? AND status IN ('claimed','mismatch_review','expired') AND matched_bank_txn_id IS NULL",
        bankTxnId,
        orderId
      );
    } catch (err) {
      if (isUniqueViolation(err, "payin_orders", "matched_bank_txn_id")) return false; // credit already credited another order
      throw err;
    }
    if (changed !== 1) return false; // already succeeded/reversed — idempotent no-op
    const o = loadOrder(orderId)!;
    if (run(db, "UPDATE bank_txns SET match_status = 'matched_payin' WHERE id = ? AND match_status IN ('unmatched','unknown_credit')", bankTxnId) !== 1) {
      throw new Error("bank txn " + bankTxnId + " not claimable for order " + orderId);
    }
    // money: consume the parked escrow into the tenant's operating balance,
    // crediting the BANK amount (resolution rule: never the order's amount)
    postGroup(
      db,
      { tenantId: o.tenant_id, kind: "payin_credited", refType: "payin_order", refId: orderId, at: now },
      [
        { account: { kind: "escrow", tenantId: o.tenant_id }, direction: "debit", amountPaise },
        { account: { kind: "tenant_operating", tenantId: o.tenant_id }, direction: "credit", amountPaise },
      ]
    );
    // an exact credit landed on an order with an open review — close the moot
    // review (utr_reuse reviews belong to the OTHER order and stay open for ops)
    run(
      db,
      "UPDATE reviews SET status = 'resolved', resolved_action = 'auto_matched' WHERE ref_type = 'payin_order' AND ref_id = ? AND status = 'open' AND kind IN ('utr_mismatch','no_txn_found','late_claim')",
      orderId
    );
    outbox.emit({
      type: "payin.succeeded",
      tenantId: o.tenant_id,
      aggregateType: "payin_order",
      aggregateId: orderId,
      payload: { orderId, amountPaise, bankTxnId },
    });
    return true;
  }

  function insertTxn(tenantId: number, accountId: number, t: BankTxn, matchStatus: string): number | null {
    try {
      const id = insert(
        db,
        "INSERT INTO bank_txns (tenant_id, account_id, bank_txn_id, utr, direction, amount_paise, booked_at, ingested_at, match_status, ref) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        tenantId,
        accountId,
        t.bank_txn_id,
        t.utr,
        t.direction,
        t.amount_paise,
        t.booked_at,
        clock.now(),
        matchStatus,
        t.ref
      );
      return id;
    } catch (err) {
      if (isUniqueViolation(err, "bank_txns")) return null; // already ingested (re-report or same bank txn id)
      throw err;
    }
  }

  type IngestResult = { inserted: number; skippedDuplicates: number; matched: number; reviews: number; payoutDebits: number; reversals: number };

  /** Bank sync ingest — ONE transaction (plan: inline match in the sync commit):
   *  dedupe → escrow park → two-sided match → reversal storno. */
  function ingest(accountId: number, txns: BankTxn[]): IngestResult {
    return withTxn(db, () => {
      const account = one<{ tenant_id: number }>(db, "SELECT tenant_id FROM bank_accounts WHERE id = ?", accountId);
      if (!account) throw new Error("no such account: " + accountId);
      const now = clock.now();
      const out: IngestResult = { inserted: 0, skippedDuplicates: 0, matched: 0, reviews: 0, payoutDebits: 0, reversals: 0 };

      for (const t of txns) {
        // payout debits were already posted by the executor at bank confirmation —
        // recognize them, never post twice
        if (t.direction === "debit" && t.ref && one(db, "SELECT id FROM payouts WHERE account_id = ? AND idempotency_key = ?", accountId, t.ref)) {
          if (insertTxn(account.tenant_id, accountId, t, "matched_payout") === null) out.skippedDuplicates++;
          else out.payoutDebits++;
          continue;
        }

        const txnId = insertTxn(account.tenant_id, accountId, t, "unmatched");
        if (txnId === null) {
          out.skippedDuplicates++;
          continue;
        }
        out.inserted++;

        if (t.direction === "credit") {
          // park in escrow FIRST — bank_mirror mirrors physical reality, matched or not
          postGroup(
            db,
            { tenantId: account.tenant_id, kind: "bank_ingest_park", refType: "bank_txn", refId: txnId, at: now },
            [
              { account: { kind: "bank_mirror", accountId }, direction: "debit", amountPaise: t.amount_paise },
              { account: { kind: "escrow", tenantId: account.tenant_id }, direction: "credit", amountPaise: t.amount_paise },
            ]
          );
          // two-sided matcher: ingest side, against live claims, in this same commit
          if (t.utr) {
            const live = findClaimForCredit(db, accountId, t.utr);
            if (live) {
              if (live.amountPaise === t.amount_paise) {
                if (succeedOrder(live.orderId, txnId, t.amount_paise, now)) out.matched++;
              } else {
                if (run(db, "UPDATE payin_orders SET status = 'mismatch_review' WHERE id = ? AND status = 'claimed'", live.orderId) === 1) {
                  const r = createReview(db, {
                    tenantId: account.tenant_id,
                    kind: "utr_mismatch",
                    refType: "payin_order",
                    refId: live.orderId,
                    payload: { expectedPaise: live.amountPaise, actualPaise: t.amount_paise, utr: t.utr },
                    suggestedRefId: txnId,
                    at: now,
                  });
                  outbox.emit({
                    type: "payin.mismatch_review_created",
                    tenantId: account.tenant_id,
                    aggregateType: "review",
                    aggregateId: r.reviewId,
                    payload: { kind: "utr_mismatch", orderId: live.orderId, reviewId: r.reviewId, utr: t.utr },
                  });
                  out.reviews++;
                }
              }
            }
          }
          continue;
        }

        // debit: reversal of a succeeded pay-in? (UTR + amount + account corroboration)
        if (t.utr) {
          const target = findReversalTarget(db, accountId, t.utr, t.amount_paise);
          if (target) {
            postGroup(
              db,
              { tenantId: account.tenant_id, kind: "payin_reversal_storno", refType: "payin_order", refId: target.orderId, at: now },
              [
                { account: { kind: "bank_mirror", accountId }, direction: "credit", amountPaise: t.amount_paise },
                { account: { kind: "tenant_operating", tenantId: account.tenant_id }, direction: "debit", amountPaise: t.amount_paise },
              ]
            );
            if (run(db, "UPDATE payin_orders SET status = 'reversed' WHERE id = ? AND status = 'succeeded'", target.orderId) !== 1) {
              throw new Error("reversal guard failed for order " + target.orderId);
            }
            run(db, "UPDATE bank_txns SET match_status = 'matched_reversal' WHERE id = ?", txnId);
            outbox.emit({
              type: "payin.reversed",
              tenantId: account.tenant_id,
              aggregateType: "payin_order",
              aggregateId: target.orderId,
              payload: { orderId: target.orderId, amountPaise: t.amount_paise },
            });
            const tb = one<{ available_paise: number }>(db, "SELECT available_paise FROM tenant_balances WHERE tenant_id = ?", account.tenant_id)!;
            if (tb.available_paise < 0) {
              createReview(db, {
                tenantId: account.tenant_id,
                kind: "reversal_overdraw",
                refType: "payin_order",
                refId: target.orderId,
                payload: { availablePaise: tb.available_paise, utr: t.utr },
                at: now,
              });
              out.reviews++;
            }
            out.reversals++;
            continue;
          }
        }

        // unrecognized debit: park it (escrow goes short) and raise a review
        postGroup(
          db,
          { tenantId: account.tenant_id, kind: "unmatched_debit_park", refType: "bank_txn", refId: txnId, at: now },
          [
            { account: { kind: "escrow", tenantId: account.tenant_id }, direction: "debit", amountPaise: t.amount_paise },
            { account: { kind: "bank_mirror", accountId }, direction: "credit", amountPaise: t.amount_paise },
          ]
        );
        run(db, "UPDATE bank_txns SET match_status = 'unknown_debit' WHERE id = ?", txnId);
        createReview(db, {
          tenantId: account.tenant_id,
          kind: "unknown_debit",
          refType: "bank_txn",
          refId: txnId,
          payload: { utr: t.utr, amountPaise: t.amount_paise },
          at: now,
        });
        out.reviews++;
      }
      return out;
    });
  }

  /** Ops resolution of a review. Idempotent: a resolved review is a replay;
   *  every money move additionally sits behind the entity's own guard.
   *  Credit-to-succeeded REQUIRES a distinct unmatched bank credit — no credit
   *  row, no money (review CRITICAL: resolutions cannot mint). */
  function resolveReview(
    reviewId: number,
    action: "credit" | "cancel" | "note",
    opts: { bankTxnId?: number } = {}
  ): { ok: true; moved: boolean; replay: boolean } | { ok: false; error: string } {
    return withTxn(db, () => {
      const r = one<{ id: number; tenant_id: number; kind: string; ref_type: string; ref_id: number; status: string }>(
        db,
        "SELECT id, tenant_id, kind, ref_type, ref_id, status FROM reviews WHERE id = ?",
        reviewId
      );
      if (!r) return { ok: false, error: "review_not_found" };
      if (r.status !== "open") return { ok: true, moved: false, replay: true };
      if (r.ref_type === "payout") return { ok: false, error: "use payout.resolveAmbiguous" };
      const now = clock.now();

      if (r.kind === "unknown_credit") {
        if (action === "credit") {
          const txn = one<{ amount_paise: number }>(
            db,
            "SELECT amount_paise FROM bank_txns WHERE id = ? AND match_status = 'unknown_credit'",
            r.ref_id
          );
          if (!txn) return { ok: false, error: "bank_txn_not_in_unknown_credit_state" };
          postGroup(
            db,
            { tenantId: r.tenant_id, kind: "unknown_credit_released", refType: "review", refId: reviewId, at: now },
            [
              { account: { kind: "escrow", tenantId: r.tenant_id }, direction: "debit", amountPaise: txn.amount_paise },
              { account: { kind: "tenant_operating", tenantId: r.tenant_id }, direction: "credit", amountPaise: txn.amount_paise },
            ]
          );
          resolveReviewRow(db, reviewId, "credit_tenant");
          return { ok: true, moved: true, replay: false };
        }
        resolveReviewRow(db, reviewId, action);
        return { ok: true, moved: false, replay: false };
      }
      if (r.kind === "unknown_debit" || r.kind === "reversal_overdraw") {
        resolveReviewRow(db, reviewId, action === "note" ? "note" : action);
        return { ok: true, moved: false, replay: false };
      }

      // order-backed kinds: utr_mismatch | utr_reuse | no_txn_found | late_claim
      const order = loadOrder(r.ref_id);
      if (!order) return { ok: false, error: "order_not_found" };

      if (action === "credit") {
        if (!opts.bankTxnId) return { ok: false, error: "bank_txn_required" };
        const txn = one<{ id: number; amount_paise: number; account_id: number }>(
          db,
          "SELECT id, amount_paise, account_id FROM bank_txns WHERE id = ? AND direction = 'credit' AND match_status IN ('unmatched','unknown_credit')",
          opts.bankTxnId
        );
        if (!txn || txn.account_id !== order.account_id) return { ok: false, error: "bank_txn_invalid" };
        const did = succeedOrder(order.id, txn.id, txn.amount_paise, now); // credits the BANK amount
        if (!did) {
          const nowOrder = loadOrder(order.id)!;
          if (nowOrder.status === "succeeded" || nowOrder.status === "reversed") {
            // already applied via another path — close the review, move nothing
            resolveReviewRow(db, reviewId, "credit_already_applied");
            return { ok: true, moved: false, replay: false };
          }
          return { ok: false, error: "order_not_resolvable" };
        }
        resolveReviewRow(db, reviewId, "credit");
        return { ok: true, moved: true, replay: false };
      }

      if (action === "cancel") {
        if (run(db, "UPDATE payin_orders SET status = 'cancelled' WHERE id = ? AND status IN ('pending','claimed','mismatch_review')", order.id) !== 1) {
          return { ok: false, error: "order_not_cancellable" };
        }
        outbox.emit({
          type: "payin.cancelled",
          tenantId: order.tenant_id,
          aggregateType: "payin_order",
          aggregateId: order.id,
          payload: { orderId: order.id },
        });
        resolveReviewRow(db, reviewId, "cancel");
        return { ok: true, moved: false, replay: false };
      }

      return { ok: false, error: "unknown_action" };
    });
  }

  /** Sweeper tasks (plan §10): order expiry, claim deadlines, the 1-minute
   *  re-match backstop, old-credit escalation. All guarded, all idempotent. */
  function sweep(now: number): { expired: number; claimDeadlines: number; rematched: number; oldCredits: number } {
    const out = { expired: 0, claimDeadlines: 0, rematched: 0, oldCredits: 0 };

    for (const o of q<PayinOrder>(db, "SELECT " + ORDER_COLS + " FROM payin_orders WHERE status = 'pending' AND expires_at <= ?", now)) {
      withTxn(db, () => {
        if (run(db, "UPDATE payin_orders SET status = 'expired' WHERE id = ? AND status = 'pending'", o.id) === 1) {
          outbox.emit({ type: "payin.expired", tenantId: o.tenant_id, aggregateType: "payin_order", aggregateId: o.id, payload: { orderId: o.id } });
          out.expired++;
        }
      });
    }

    for (const o of q<PayinOrder>(db, "SELECT " + ORDER_COLS + " FROM payin_orders WHERE status = 'claimed' AND utr_deadline <= ? AND matched_bank_txn_id IS NULL", now)) {
      withTxn(db, () => {
        if (run(db, "UPDATE payin_orders SET status = 'mismatch_review' WHERE id = ? AND status = 'claimed'", o.id) === 1) {
          const r = createReview(db, {
            tenantId: o.tenant_id,
            kind: "no_txn_found",
            refType: "payin_order",
            refId: o.id,
            payload: { orderId: o.id, utr: o.claimed_utr, reason: "claim deadline passed with no bank evidence" },
            at: now,
          });
          outbox.emit({
            type: "payin.mismatch_review_created",
            tenantId: o.tenant_id,
            aggregateType: "review",
            aggregateId: r.reviewId,
            payload: { kind: "no_txn_found", orderId: o.id, reviewId: r.reviewId },
          });
          out.claimDeadlines++;
        }
      });
    }

    // re-match backstop: closes the claim-vs-sync read race before review escalation
    for (const o of q<PayinOrder>(
      db,
      "SELECT " + ORDER_COLS + " FROM payin_orders WHERE status IN ('claimed','mismatch_review') AND claimed_utr IS NOT NULL AND matched_bank_txn_id IS NULL"
    )) {
      const credit = findExactCreditForClaim(db, o.account_id, o.claimed_utr!, o.amount_paise);
      if (credit) {
        let did = false;
        withTxn(db, () => {
          did = succeedOrder(o.id, credit.txnId, o.amount_paise, now);
        });
        if (did) out.rematched++;
      }
    }

    for (const t of q<{ id: number; tenant_id: number; account_id: number; utr: string | null; amount_paise: number }>(
      db,
      "SELECT id, tenant_id, account_id, utr, amount_paise FROM bank_txns WHERE direction = 'credit' AND match_status = 'unmatched' AND ingested_at <= ?",
      now - OLD_CREDIT_MS
    )) {
      withTxn(db, () => {
        if (run(db, "UPDATE bank_txns SET match_status = 'unknown_credit' WHERE id = ? AND match_status = 'unmatched'", t.id) === 1) {
          createReview(db, {
            tenantId: t.tenant_id,
            kind: "unknown_credit",
            refType: "bank_txn",
            refId: t.id,
            payload: { utr: t.utr, amountPaise: t.amount_paise, accountId: t.account_id, reason: "no live claim matched this credit" },
            at: now,
          });
          out.oldCredits++;
        }
      });
    }

    return out;
  }

  return { createOrder, claim, ingest, resolveReview, sweep };
}

export type Payin = ReturnType<typeof makePayin>;
