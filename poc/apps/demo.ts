import { createSystem } from "../src/platform/system.ts";
import { ACC, TENANT_ID, one, q } from "../src/core/db.ts";

/** Narrative run of the headline flows — the same code paths the tests assert on.
 *  Run: bun demo.ts */

const INR = (paise: number) => "Rs." + (paise / 100).toLocaleString("en-IN", { minimumFractionDigits: 2 });

const s = createSystem();
const log = (...a: unknown[]) => console.log(...a);
const line = () => console.log("\n" + "-".repeat(76));

function balances(): string {
  const t = one<{ available_paise: number; held_paise: number }>(
    s.db,
    "SELECT available_paise, held_paise FROM tenant_balances WHERE tenant_id = ?",
    TENANT_ID
  )!;
  const a1 = one<{ available_paise: number }>(s.db, "SELECT available_paise FROM account_balances WHERE account_id = ?", ACC.primary)!.available_paise;
  const a2 = one<{ available_paise: number }>(s.db, "SELECT available_paise FROM account_balances WHERE account_id = ?", ACC.secondary)!.available_paise;
  return "tenant available=" + INR(t.available_paise) + " held=" + INR(t.held_paise) + " | bank a1=" + INR(a1) + " a2=" + INR(a2);
}

function payoutStatus(id: number): string {
  return one<{ status: string }>(s.db, "SELECT status FROM payouts WHERE id = ?", id)!.status;
}

function orderStatus(id: number): string {
  return one<{ status: string }>(s.db, "SELECT status FROM payin_orders WHERE id = ?", id)!.status;
}

log("=".repeat(76));
log("PAYMENT ORCHESTRATOR POC — narrative run (Bun + bun:sqlite, zero npm deps)");
log("=".repeat(76));

line();
log("1. Opening float wired into account a1");
s.fund(ACC.primary, 50_000_00);
log("   " + balances());

line();
log("2. Pay-in Rs.2,500 — credit already ingested; the claim matches at claim time");
const o1 = s.payin.createOrder({ accountId: ACC.primary, amountPaise: 2_500_00, clientOrderId: "demo-o1" });
if (!o1.ok) throw new Error("order create failed: " + o1.error);
s.bank.addCredit(ACC.primary, { utr: "UTRDEMO1", amountPaise: 2_500_00, bookedAt: s.clock.now() - 1000 });
const sync1 = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
log("   sync parks the credit in escrow: " + (sync1.result === "ok" ? sync1.inserted : 0) + " inserted — " + balances());
const c1 = s.payin.claim({ orderId: o1.orderId, utr: "UTRDEMO1" });
log("   claim(UTRDEMO1) -> " + c1.status);
log("   " + balances());

line();
log("3. Pay-in Rs.2,500 — claim FIRST, credit lands with the next sync (two-sided matcher)");
const o2 = s.payin.createOrder({ accountId: ACC.primary, amountPaise: 2_500_00, clientOrderId: "demo-o2" });
if (!o2.ok) throw new Error("order create failed: " + o2.error);
const c2 = s.payin.claim({ orderId: o2.orderId, utr: "UTRDEMO2" });
log("   claim(UTRDEMO2) -> " + c2.status + " (no bank evidence yet — never auto-fails)");
s.bank.addCredit(ACC.primary, { utr: "UTRDEMO2", amountPaise: 2_500_00 });
const sync2 = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
log(
  "   sync: inserted=" + (sync2.result === "ok" ? sync2.inserted : 0) +
    " deduped=" + (sync2.result === "ok" ? sync2.skippedDuplicates : 0) +
    " matched=" + (sync2.result === "ok" ? sync2.matched : 0)
);
log("   order 2 -> " + orderStatus(o2.orderId) + " (matched inside the sync commit)");
log("   " + balances());

line();
log("4. Payout Rs.7,500 — event-driven: outbox -> executor -> bank -> reconciler");
const p1 = s.payout.create({
  tenantId: TENANT_ID,
  amountPaise: 7_500_00,
  beneficiary: '{"ifsc":"HDFC0000123","account":"9876543210"}',
  idempotencyKey: "demo-p1",
});
if (!p1.ok) throw new Error("payout create failed");
log("   create -> " + payoutStatus(p1.payoutId) + " (+ outbox payout.created)");
await s.tick();
log("   tick 1: relay dispatched the event, executor selected a1, gate, held, submitted");
log("   status=" + payoutStatus(p1.payoutId) + " | bank submissions for key: " + s.bank.submitCount("demo-p1"));
log("   " + balances());
s.clock.advance(31_000);
await s.tick();
log("   tick 2: reconciler queried the bank (never resubmitted) -> " + payoutStatus(p1.payoutId));
log("   bank submissions for key: " + s.bank.submitCount("demo-p1"));
log("   " + balances());

line();
log("5. Payout Rs.1,000 — submit times out. A timeout is NOT evidence: unknown, hold kept");
s.bank.setFaults("timeout_submit");
const p2 = s.payout.create({
  tenantId: TENANT_ID,
  amountPaise: 1_000_00,
  beneficiary: '{"ifsc":"HDFC0000123","account":"9876543210"}',
  idempotencyKey: "demo-p2",
});
if (!p2.ok) throw new Error("payout create failed");
const r5 = await s.payout.execute(p2.payoutId);
log("   execute -> " + r5.status);
log("   " + balances());
s.bank.setStatus("demo-p2", "executed", ACC.primary);
s.clock.advance(31_000);
await s.tick();
log("   bank later reports executed -> " + payoutStatus(p2.payoutId) + " | submissions for key: " + s.bank.submitCount("demo-p2"));
log("   " + balances());

line();
log("6. Bank reverses the first pay-in — corroborated storno with overdraw check");
s.bank.addDebit(ACC.primary, { utr: "UTRDEMO1", amountPaise: 2_500_00 });
const sync6 = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
log(
  "   sync: reversals=" + (sync6.result === "ok" ? sync6.reversals : 0) +
    " payout-debits-recognized=" + (sync6.result === "ok" ? sync6.payoutDebits : 0) +
    " deduped=" + (sync6.result === "ok" ? sync6.skippedDuplicates : 0)
);
log("   order 1 -> " + orderStatus(o1.orderId));
log("   " + balances());

line();
log("7. Final invariant check");
await s.tick();
const v = s.verify();
log("   ledger groups=" + v.groups + " | all zero-sum + aggregates equal recompute: " + v.ok + " | escrow net=" + INR(v.escrowNetPaise));
if (!v.ok) log("   PROBLEMS: " + JSON.stringify(v.problems));
const openReviews = q<{ id: number; kind: string }>(s.db, "SELECT id, kind FROM reviews WHERE status = 'open'");
log("   open reviews: " + openReviews.length + (openReviews.length ? " (" + openReviews.map((r) => r.kind).join(", ") + ")" : ""));
const evs = q<{ event_type: string }>(s.db, "SELECT event_type FROM outbox ORDER BY occurred_at, event_id");
log("   outbox events (" + evs.length + ", all dispatched):");
for (const e of evs) log("     - " + e.event_type);

line();
log(v.ok ? "RESULT: ALL LEDGER INVARIANTS HOLD — no double-pay, no lost hold, no wedge." : "RESULT: INVARIANT VIOLATION — see problems above");
