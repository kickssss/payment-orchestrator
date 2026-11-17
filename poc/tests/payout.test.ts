import { beforeEach, describe, expect, test } from "bun:test";
import { createSystem, type System } from "../src/platform/system.ts";
import { ACC, TENANT_ID, one, q } from "../src/core/db.ts";

let s: System;

beforeEach(() => {
  s = createSystem();
});

function mkPayout(amountPaise: number, idempotencyKey: string) {
  const r = s.payout.create({
    tenantId: TENANT_ID,
    amountPaise,
    beneficiary: '{"ifsc":"HDFC0000123","account":"9999999999"}',
    idempotencyKey,
  });
  if (!r.ok) throw new Error("payout create failed");
  return r;
}

function payoutRow(id: number) {
  return one<{ status: string; account_id: number | null; attempts: number }>(
    s.db,
    "SELECT status, account_id, attempts FROM payouts WHERE id = ?",
    id
  )!;
}

function balances() {
  const t = one<{ available_paise: number; held_paise: number }>(
    s.db,
    "SELECT available_paise, held_paise FROM tenant_balances WHERE tenant_id = 1"
  )!;
  const a1 = one<{ available_paise: number }>(s.db, "SELECT available_paise FROM account_balances WHERE account_id = 1")!.available_paise;
  const a2 = one<{ available_paise: number }>(s.db, "SELECT available_paise FROM account_balances WHERE account_id = 2")!.available_paise;
  return { available: t.available_paise, held: t.held_paise, a1, a2 };
}

function events(): string[] {
  return q<{ event_type: string }>(s.db, "SELECT event_type FROM outbox ORDER BY occurred_at, event_id").map((r) => r.event_type);
}

describe("payout: hold-first, exactly-once, ambiguity-safe", () => {
  test("5. happy path: idempotent create, one hold, one submit, settle consumes hold", async () => {
    s.fund(ACC.primary, 1000_00);
    const c = mkPayout(400_00, "idem-1");

    const again = s.payout.create({ tenantId: TENANT_ID, amountPaise: 400_00, beneficiary: "{}", idempotencyKey: "idem-1" });
    expect(again.ok && again.existing && again.payoutId === c.payoutId).toBe(true);

    const r = await s.payout.execute(c.payoutId);
    expect(r.status).toBe("in_flight");
    expect(s.bank.submitCount("idem-1")).toBe(1);
    expect(balances()).toEqual({ available: 600_00, held: 400_00, a1: 1000_00, a2: 0 });

    s.clock.advance(31_000);
    const sweep = await s.sweeper.tick();
    expect(sweep.payouts.reconciled).toBe(1);
    expect(payoutRow(c.payoutId).status).toBe("settled");
    expect(balances()).toEqual({ available: 600_00, held: 0, a1: 600_00, a2: 0 });
    expect(s.bank.submitCount("idem-1")).toBe(1);

    const evs = events();
    for (const expected of ["payout.created", "payout.held", "payout.submitted", "payout.settled"]) {
      expect(evs).toContain(expected);
    }
    expect(s.verify().ok).toBe(true);
  });

  test("5b. fully event-driven: create -> tick executes via the outbox", async () => {
    s.fund(ACC.primary, 1000_00);
    const c = mkPayout(100_00, "idem-evt");
    expect(payoutRow(c.payoutId).status).toBe("created");

    const t1 = await s.tick();
    expect(t1.relay.dispatched).toBe(1);
    expect(payoutRow(c.payoutId).status).toBe("in_flight");

    s.clock.advance(31_000);
    await s.tick();
    expect(payoutRow(c.payoutId).status).toBe("settled");
    expect(s.verify().ok).toBe(true);
  });

  test("5c. concurrent double-execute: exactly one submission, no double hold", async () => {
    s.fund(ACC.primary, 1000_00);
    const c = mkPayout(400_00, "idem-race");
    const [a, b] = await Promise.all([s.payout.execute(c.payoutId), s.payout.execute(c.payoutId)]);
    const statuses = [a.status, b.status].sort();
    expect(statuses).toEqual(["held", "in_flight"]);
    expect(s.bank.submitCount("idem-race")).toBe(1);
    expect(balances().held).toBe(400_00);
    expect(s.verify().ok).toBe(true);
  });

  test("6. submit timeout -> unknown -> evidence -> settled; never resubmitted", async () => {
    s.fund(ACC.primary, 1000_00);
    s.bank.setFaults("timeout_submit");
    const c = mkPayout(400_00, "idem-to");
    const r = await s.payout.execute(c.payoutId);
    expect(r.status).toBe("unknown");
    expect(balances()).toEqual({ available: 600_00, held: 400_00, a1: 1000_00, a2: 0 });
    expect(s.bank.submitCount("idem-to")).toBe(1);

    s.bank.setStatus("idem-to", "executed", ACC.primary);
    s.clock.advance(31_000);
    await s.sweeper.tick();
    expect(payoutRow(c.payoutId).status).toBe("settled");
    expect(balances()).toEqual({ available: 600_00, held: 0, a1: 600_00, a2: 0 });
    expect(s.bank.submitCount("idem-to")).toBe(1);
    expect(s.verify().ok).toBe(true);
  });

  test("6b. the settled payout's debit on the statement is recognized — never double-posted", async () => {
    s.fund(ACC.primary, 1000_00);
    s.bank.setFaults("timeout_submit");
    const c = mkPayout(400_00, "idem-stmt");
    await s.payout.execute(c.payoutId);
    s.bank.setStatus("idem-stmt", "executed", ACC.primary);
    s.clock.advance(31_000);
    await s.sweeper.tick();
    expect(payoutRow(c.payoutId).status).toBe("settled");
    const a1Before = balances().a1;

    const sync = await s.sync.syncAccount(ACC.primary, 0, s.clock.now());
    expect(sync.result).toBe("ok");
    if (sync.result === "ok") {
      expect(sync.payoutDebits).toBe(1);
      expect(sync.inserted).toBe(0);
    }
    expect(balances().a1).toBe(a1Before); // bank_mirror unchanged: the executor already posted at settlement
    const txn = one<{ match_status: string }>(s.db, "SELECT match_status FROM bank_txns WHERE ref = 'idem-stmt'")!;
    expect(txn.match_status).toBe("matched_payout");
    expect(s.verify().ok).toBe(true);
  });

  test("7. unknown without evidence -> payout_ambiguous review with hold kept; ops resolution needs evidence", async () => {
    s.fund(ACC.primary, 1000_00);
    s.bank.setFaults("timeout_submit");
    const c = mkPayout(400_00, "idem-amb");
    await s.payout.execute(c.payoutId);

    s.bank.setFaults("status_error");
    s.clock.advance(31_000);
    await s.sweeper.tick();
    expect(payoutRow(c.payoutId).status).toBe("unknown");
    expect(balances().held).toBe(400_00);

    s.clock.advance(24 * 3_600_000 + 31_000);
    await s.sweeper.tick();
    const review = one<{ id: number }>(s.db, "SELECT id FROM reviews WHERE kind = 'payout_ambiguous' AND status = 'open'");
    expect(review).toBeDefined();
    expect(balances().held).toBe(400_00);

    const no = await s.payout.resolveAmbiguous(review!.id, "confirm_failed");
    expect(no.ok).toBe(false);

    s.bank.setStatus("idem-amb", "rejected");
    const yes = await s.payout.resolveAmbiguous(review!.id, "confirm_failed");
    expect(yes.ok).toBe(true);
    expect(payoutRow(c.payoutId).status).toBe("failed");
    expect(balances()).toEqual({ available: 1000_00, held: 0, a1: 1000_00, a2: 0 });

    const replay = await s.payout.resolveAmbiguous(review!.id, "confirm_failed");
    expect(replay.ok).toBe(true);
    expect(s.verify().ok).toBe(true);
  });

  test("8a. crash after hold (never submitted): sweeper queries the bank, fails with evidence, releases hold", async () => {
    s.fund(ACC.primary, 1000_00);
    const c = mkPayout(400_00, "idem-crash1");
    await expect(s.payout.execute(c.payoutId, { crashAfterHold: true })).rejects.toThrow("WORKER_CRASH_AFTER_HOLD");
    expect(payoutRow(c.payoutId).status).toBe("held");
    expect(balances().held).toBe(400_00);
    expect(s.bank.submitCount("idem-crash1")).toBe(0);

    s.clock.advance(61_000);
    const sweep = await s.sweeper.tick();
    expect(sweep.payouts.heldStale).toBe(1);
    expect(payoutRow(c.payoutId).status).toBe("failed");
    expect(balances()).toEqual({ available: 1000_00, held: 0, a1: 1000_00, a2: 0 });
    expect(s.bank.submitCount("idem-crash1")).toBe(0);
    expect(s.verify().ok).toBe(true);
  });

  test("8b. crash after submit before mark: sweeper finds it executed -> settled, never resubmitted", async () => {
    s.fund(ACC.primary, 1000_00);
    const c = mkPayout(400_00, "idem-crash2");
    await expect(s.payout.execute(c.payoutId, { crashAfterSubmit: true })).rejects.toThrow("WORKER_CRASH_AFTER_SUBMIT");
    expect(payoutRow(c.payoutId).status).toBe("held");
    expect(s.bank.submitCount("idem-crash2")).toBe(1);

    s.clock.advance(61_000);
    await s.sweeper.tick();
    expect(payoutRow(c.payoutId).status).toBe("settled");
    expect(balances()).toEqual({ available: 600_00, held: 0, a1: 600_00, a2: 0 });
    expect(s.bank.submitCount("idem-crash2")).toBe(1);
    expect(s.verify().ok).toBe(true);
  });

  test("9. gate: statements win (payout defers); expired gate is free; stale holder cannot release", async () => {
    s.fund(ACC.primary, 1000_00);
    expect(s.gate.acquire("account", ACC.primary, "statement", "stmt:1")).toBe(true);

    const c = mkPayout(400_00, "idem-gate");
    const r = await s.payout.execute(c.payoutId);
    expect(r.status).toBe("deferred");
    expect(payoutRow(c.payoutId).status).toBe("created");
    expect(s.bank.submitCount("idem-gate")).toBe(0);
    expect(balances().held).toBe(0);

    s.clock.advance(61_000);
    expect(s.gate.acquire("account", ACC.primary, "payout", "stealer")).toBe(true);
    expect(s.gate.heldBy("account", ACC.primary, "stmt:1")).toBe(false);
    expect(s.gate.heldBy("account", ACC.primary, "stealer")).toBe(true);
    expect(s.gate.release("account", ACC.primary, "stmt:1")).toBe(false);
    expect(s.gate.release("account", ACC.primary, "stealer")).toBe(true);

    const r2 = await s.payout.execute(c.payoutId);
    expect(r2.status).toBe("in_flight");
    expect(s.bank.submitCount("idem-gate")).toBe(1);
    expect(s.verify().ok).toBe(true);
  });

  test("11. cancel: only from created — a held payout is NEVER cancellable", async () => {
    s.fund(ACC.primary, 1000_00);
    const c1 = mkPayout(100_00, "idem-can1");
    const cancelOk = s.payout.cancel(c1.payoutId);
    expect("ok" in cancelOk && cancelOk.ok).toBe(true);

    const c2 = mkPayout(100_00, "idem-can2");
    await s.payout.execute(c2.payoutId, { crashAfterHold: true }).catch(() => {});
    const cancelHeld = s.payout.cancel(c2.payoutId);
    expect("ok" in cancelHeld && cancelHeld.ok).toBe(false);
    expect("error" in cancelHeld && cancelHeld.error).toBe("only_created_cancellable");
    expect(balances().held).toBe(100_00);
    expect(s.verify().ok).toBe(true);
  });

  test("12. bank rejects the submit -> failed with positive evidence, hold released", async () => {
    s.fund(ACC.primary, 1000_00);
    s.bank.setFaults("reject_submit");
    const c = mkPayout(400_00, "idem-rej");
    const r = await s.payout.execute(c.payoutId);
    expect(r.status).toBe("failed");
    expect(balances()).toEqual({ available: 1000_00, held: 0, a1: 1000_00, a2: 0 });
    expect(s.bank.submitCount("idem-rej")).toBe(1);
    expect(s.verify().ok).toBe(true);
  });

  test("13. account selection requires BOTH tenant-pooled AND per-account physical funds", async () => {
    s.fund(ACC.primary, 500_00);
    s.fund(ACC.secondary, 300_00);
    const c = mkPayout(600_00, "idem-phys");
    const r = await s.payout.execute(c.payoutId);
    expect(r.status).toBe("rejected");
    expect(balances().held).toBe(0);
    expect(s.verify().ok).toBe(true);
  });

  test("14. active holds reserve physical funds at selection (no overcommit)", async () => {
    s.fund(ACC.primary, 1000_00);
    s.fund(ACC.secondary, 2000_00);

    const c1 = mkPayout(600_00, "idem-h1");
    const r1 = await s.payout.execute(c1.payoutId);
    expect(r1.status).toBe("in_flight");
    expect(payoutRow(c1.payoutId).account_id).toBe(ACC.primary);

    const c2 = mkPayout(500_00, "idem-h2");
    const r2 = await s.payout.execute(c2.payoutId);
    expect(r2.status).toBe("in_flight");
    expect(payoutRow(c2.payoutId).account_id).toBe(ACC.secondary);

    // tenant-pooled would allow 1600 (1900 available) but every account is
    // overcommit-reserved — physical check refuses
    const c3 = mkPayout(1600_00, "idem-h3");
    const r3 = await s.payout.execute(c3.payoutId);
    expect(r3.status).toBe("rejected");
    expect(balances().held).toBe(1100_00);
    expect(s.verify().ok).toBe(true);
  });
});
