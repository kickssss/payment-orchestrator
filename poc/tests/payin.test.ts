import { beforeEach, describe, expect, test } from "bun:test";
import { createSystem, type System } from "../src/platform/system.ts";
import { ACC, one, q } from "../src/core/db.ts";

let s: System;

beforeEach(() => {
  s = createSystem();
});

function mkOrder(amountPaise: number, clientOrderId: string, accountId: number = ACC.primary): number {
  const r = s.payin.createOrder({ accountId, amountPaise, clientOrderId });
  if (!r.ok) throw new Error("createOrder failed: " + r.error);
  return r.orderId;
}

function orderRow(id: number) {
  return one<{ status: string; matched_bank_txn_id: number | null; claimed_utr: string | null }>(
    s.db,
    "SELECT status, matched_bank_txn_id, claimed_utr FROM payin_orders WHERE id = ?",
    id
  )!;
}

function openReviews(kind: string): { id: number; ref_id: number }[] {
  return q<{ id: number; ref_id: number }>(s.db, "SELECT id, ref_id FROM reviews WHERE kind = ? AND status = 'open'", kind);
}

function tenantAvailable(): number {
  return one<{ available_paise: number }>(s.db, "SELECT available_paise FROM tenant_balances WHERE tenant_id = 1")!.available_paise;
}

function accountAvailable(accountId: number = ACC.primary): number {
  return one<{ available_paise: number }>(s.db, "SELECT available_paise FROM account_balances WHERE account_id = ?", accountId)!.available_paise;
}

function expectInvariantsOk() {
  const v = s.verify();
  expect(v.problems).toEqual([]);
}

describe("pay-in: evidence-only credit", () => {
  test("1. immediate match at claim; repeat claim and re-sync are no-ops", async () => {
    s.bank.addCredit(ACC.primary, { utr: "UTR1001", amountPaise: 500_00, bookedAt: s.clock.now() - 1000 });
    const orderId = mkOrder(500_00, "co-1");

    const sync1 = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync1.result).toBe("ok");
    // credit parked in escrow: the bank mirror moves, the tenant does NOT
    expect(accountAvailable()).toBe(500_00);
    expect(tenantAvailable()).toBe(0);

    const claim = s.payin.claim({ orderId, utr: "UTR1001" });
    expect(claim.status).toBe("succeeded");
    expect(tenantAvailable()).toBe(500_00);
    expect(accountAvailable()).toBe(500_00);

    expect(s.payin.claim({ orderId, utr: "UTR1001" }).status).toBe("gone");

    const sync2 = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync2.result).toBe("ok");
    if (sync2.result === "ok") expect(sync2.skippedDuplicates).toBe(1);

    expect(one<{ n: number }>(s.db, "SELECT COUNT(*) AS n FROM ledger_txns WHERE kind = 'payin_credited'")!.n).toBe(1);
    const v = s.verify();
    expect(v.ok).toBe(true);
    expect(v.escrowNetPaise).toBe(0);
  });

  test("2. delayed bank sync: claim first, credit arrives later — matched inside the sync commit", async () => {
    const orderId = mkOrder(500_00, "co-2");
    expect(s.payin.claim({ orderId, utr: "UTR2002" }).status).toBe("claimed");

    s.bank.addCredit(ACC.primary, { utr: "UTR2002", amountPaise: 500_00 });
    const sync = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync.result).toBe("ok");
    if (sync.result === "ok") expect(sync.matched).toBe(1);

    expect(orderRow(orderId).status).toBe("succeeded");
    expect(tenantAvailable()).toBe(500_00);
    expectInvariantsOk();
  });

  test("3a. amount mismatch -> utr_mismatch review, never auto-fail; money stays parked", async () => {
    const orderId = mkOrder(500_00, "co-3a");
    s.payin.claim({ orderId, utr: "UTR3003" });
    s.bank.addCredit(ACC.primary, { utr: "UTR3003", amountPaise: 400_00 });
    const sync = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync.result).toBe("ok");
    if (sync.result === "ok") expect(sync.reviews).toBe(1);

    expect(orderRow(orderId).status).toBe("mismatch_review");
    const reviews = openReviews("utr_mismatch");
    expect(reviews.length).toBe(1);
    expect(reviews[0].ref_id).toBe(orderId);
    expect(tenantAvailable()).toBe(0);
    expect(accountAvailable()).toBe(400_00);
    expect(s.verify().escrowNetPaise).toBe(400_00);
    expectInvariantsOk();
  });

  test("3b. duplicate live UTR -> unique index -> utr_reuse review for the loser", async () => {
    const o1 = mkOrder(500_00, "co-3b1");
    const o2 = mkOrder(500_00, "co-3b2");
    expect(s.payin.claim({ orderId: o1, utr: "UTR4004" }).status).toBe("claimed");
    const second = s.payin.claim({ orderId: o2, utr: "UTR4004" });
    expect(second.status).toBe("utr_reuse_review");
    if (second.status !== "utr_reuse_review") throw new Error("unreachable");

    expect(one<{ n: number }>(s.db, "SELECT COUNT(*) AS n FROM payin_orders WHERE account_id = ? AND active_utr = 'UTR4004'", ACC.primary)!.n).toBe(1);
    expect(orderRow(o2).status).toBe("pending");
    expect(openReviews("utr_reuse").length).toBe(1);
    expectInvariantsOk();
  });

  test("3c. claim after expiry -> late_claim review, UTR preserved", async () => {
    const orderId = mkOrder(500_00, "co-3c");
    s.clock.advance(31 * 60_000);
    const claim = s.payin.claim({ orderId, utr: "UTR5005" });
    expect(claim.status).toBe("late_claim_review");
    expect(orderRow(orderId).status).toBe("expired");
    const reviews = openReviews("late_claim");
    expect(reviews.length).toBe(1);
    const payload = JSON.parse(one<{ payload: string }>(s.db, "SELECT payload FROM reviews WHERE id = ?", reviews[0].id)!.payload);
    expect(payload.utr).toBe("UTR5005");
    expectInvariantsOk();
  });

  test("4. aggregator re-report dedupes on the composite unique key", async () => {
    const at = s.clock.now() - 5000;
    s.bank.addCredit(ACC.primary, { utr: "UTR6006", amountPaise: 500_00, bookedAt: at });
    s.bank.addCredit(ACC.primary, { utr: "UTR6006", amountPaise: 500_00, bookedAt: at });
    const sync = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync.result).toBe("ok");
    if (sync.result === "ok") {
      expect(sync.inserted).toBe(1);
      expect(sync.skippedDuplicates).toBe(1);
    }
    expect(accountAvailable()).toBe(500_00);
    expectInvariantsOk();
  });

  test("one credit can never credit two orders", async () => {
    const o1 = mkOrder(500_00, "co-7a");
    const o2 = mkOrder(500_00, "co-7b");
    s.payin.claim({ orderId: o1, utr: "UTR7007" });
    s.payin.claim({ orderId: o2, utr: "UTR8008" });
    s.bank.addCredit(ACC.primary, { utr: "UTR7007", amountPaise: 500_00 });
    s.bank.addCredit(ACC.primary, { utr: "UTR8008", amountPaise: 700_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());

    expect(orderRow(o1).status).toBe("succeeded");
    expect(orderRow(o2).status).toBe("mismatch_review");
    const usedTxnId = orderRow(o1).matched_bank_txn_id;
    expect(usedTxnId).toBeGreaterThan(0);

    const reviewId = openReviews("utr_mismatch")[0].id;
    const res = s.payin.resolveReview(reviewId, "credit", { bankTxnId: usedTxnId });
    expect(res.ok).toBe(false);
    expect(tenantAvailable()).toBe(500_00);
    expectInvariantsOk();
  });

  test("10. resolution: needs a credit row, is idempotent, money moves exactly once", async () => {
    const orderId = mkOrder(500_00, "co-10");
    s.payin.claim({ orderId, utr: "UTR9009" });
    s.bank.addCredit(ACC.primary, { utr: "UTR9009", amountPaise: 400_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    const reviewId = openReviews("utr_mismatch")[0].id;

    const noCredit = s.payin.resolveReview(reviewId, "credit");
    expect(noCredit.ok).toBe(false);
    expect("error" in noCredit && noCredit.error).toBe("bank_txn_required");

    s.bank.addCredit(ACC.primary, { utr: "UTR9009", amountPaise: 500_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    const txn = one<{ id: number }>(
      s.db,
      "SELECT id FROM bank_txns WHERE utr = 'UTR9009' AND amount_paise = 500_00 AND match_status = 'unmatched'"
    )!;
    const res = s.payin.resolveReview(reviewId, "credit", { bankTxnId: txn.id });
    expect(res.ok).toBe(true);
    expect(orderRow(orderId).status).toBe("succeeded");
    expect(tenantAvailable()).toBe(500_00);

    const replay = s.payin.resolveReview(reviewId, "credit", { bankTxnId: txn.id });
    expect(replay.ok).toBe(true);
    expect("replay" in replay && replay.replay).toBe(true);
    expect(tenantAvailable()).toBe(500_00);
    expect(one<{ n: number }>(s.db, "SELECT COUNT(*) AS n FROM ledger_txns WHERE kind = 'payin_credited'")!.n).toBe(1);
    expectInvariantsOk();
  });

  test("10b. reversal after success -> storno, order reversed; overdraw raises a review", async () => {
    s.fund(ACC.primary, 1000_00);
    const orderId = mkOrder(500_00, "co-10b");
    s.payin.claim({ orderId, utr: "UTR11" });
    s.bank.addCredit(ACC.primary, { utr: "UTR11", amountPaise: 500_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(orderRow(orderId).status).toBe("succeeded");
    expect(tenantAvailable()).toBe(1500_00);

    const c = s.payout.create({ tenantId: 1, amountPaise: 1200_00, beneficiary: "{}", idempotencyKey: "idem-rev" });
    if (!c.ok) throw new Error("payout create failed");
    await s.payout.execute(c.payoutId);
    s.clock.advance(31_000);
    await s.sweeper.tick();
    expect(tenantAvailable()).toBe(300_00);

    s.bank.addDebit(ACC.primary, { utr: "UTR11", amountPaise: 500_00 });
    const sync = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync.result).toBe("ok");
    if (sync.result === "ok") expect(sync.reversals).toBe(1);

    expect(orderRow(orderId).status).toBe("reversed");
    expect(tenantAvailable()).toBe(-200_00);
    expect(openReviews("reversal_overdraw").length).toBe(1);
    expectInvariantsOk();
  });

  test("sweep: claim deadline -> no_txn_found; expiry; old-credit escalation; rematch backstop", async () => {
    const o1 = mkOrder(500_00, "co-s1");
    s.payin.claim({ orderId: o1, utr: "UTR998" });
    s.clock.advance(48 * 3_600_000 + 60_000);
    const t1 = await s.sweeper.tick();
    expect(t1.payin.claimDeadlines).toBe(1);
    expect(orderRow(o1).status).toBe("mismatch_review");
    expect(openReviews("no_txn_found").length).toBe(1);

    const o2 = mkOrder(500_00, "co-s2");
    s.clock.advance(31 * 60_000);
    const t2 = await s.sweeper.tick();
    expect(t2.payin.expired).toBe(1);
    expect(orderRow(o2).status).toBe("expired");

    s.bank.addCredit(ACC.primary, { utr: "UTR997", amountPaise: 300_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    s.clock.advance(16 * 60_000);
    const t3 = await s.sweeper.tick();
    expect(t3.payin.oldCredits).toBe(1);
    const review = openReviews("unknown_credit")[0];
    expect(review).toBeDefined();
    const res = s.payin.resolveReview(review.id, "credit");
    expect(res.ok).toBe(true);
    expect(tenantAvailable()).toBe(300_00);

    const o3 = mkOrder(700_00, "co-s3");
    s.payin.claim({ orderId: o3, utr: "UTR996" });
    s.bank.addCredit(ACC.primary, { utr: "UTR996", amountPaise: 123_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(orderRow(o3).status).toBe("mismatch_review");
    s.bank.addCredit(ACC.primary, { utr: "UTR996", amountPaise: 700_00 });
    await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    const t4 = await s.sweeper.tick();
    expect(t4.payin.rematched).toBe(1);
    expect(orderRow(o3).status).toBe("succeeded");
    expect(openReviews("utr_mismatch").filter((r) => r.ref_id === o3).length).toBe(0);
    expectInvariantsOk();
  });

  test("capacity: the 4th open order on an account is refused inside the create txn", () => {
    mkOrder(100_00, "cap-1");
    mkOrder(100_00, "cap-2");
    mkOrder(100_00, "cap-3");
    const r = s.payin.createOrder({ accountId: ACC.primary, amountPaise: 100_00, clientOrderId: "cap-4" });
    expect(r.ok).toBe(false);
    expect("error" in r && r.error).toBe("NO_CAPACITY");
    const r2 = s.payin.createOrder({ accountId: ACC.secondary, amountPaise: 100_00, clientOrderId: "cap-5" });
    expect(r2.ok).toBe(true);
  });
});
