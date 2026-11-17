/** e2e/scale.ts — the multi-process scaling proof (plain script, NOT bun:test).
 *
 * Spawns the REAL processes — 1 simbank, 2 apis, N workers — all on ONE shared
 * SQLite file (WAL + busy_timeout; no Redis: DB claims/guards ARE the
 * coordination) and drives real money through them, asserting EXACTLY-ONCE
 * EFFECTS only: pay-in via bank sync, payout via outbox relay, a 25-way
 * concurrent same-key burst against BOTH apis, and a deterministic
 * crash-after-hold recovered by a DIFFERENT worker on bank evidence.
 *
 * Honesty rules: outbox delivery is at-least-once, so we assert effects
 * (statuses, bank submit counts, ledger verify, balances) — never relay
 * invocation counts, latencies, or worker-distribution fairness. Every wait is
 * bounded polling (waitUntil); no sleep-and-hope.
 *
 * Exit 0 = PASS, exit 1 = FAIL. Run: bun e2e/scale.ts (from the poc root).
 * The DB file is kept for postmortem (path printed in the SUMMARY).
 */

import { rmSync } from "node:fs";
import { resolve } from "node:path";

const ROOT = resolve(import.meta.dir, ".."); // poc root, regardless of caller cwd
const DB = "run/e2e-" + process.pid + ".db"; // fresh file per run
const DB_ABS = resolve(ROOT, DB);
const KEY = "e2e-key";
const BANK = 4100; // simbank
const API1 = 4200; // api replica 1
const API2 = 4201; // api replica 2 — SAME DB: proves stateless api xN
const BEN = { ifsc: "HDFC0000123", account: "9876543210" };

/** Shared child env: one DB file, one auth key, recovery windows shrunk to seconds. */
const CHILD_ENV: Record<string, string> = {
  API_KEY: KEY,
  DB_PATH: DB,
  BANK_URL: "http://127.0.0.1:" + BANK,
  EXEC_DELAY_MS: "500",
  STALE_HELD_MS: "2000",
  RECONCILE_DELAY_MS: "1500",
  DEFER_BACKOFF_MS: "500",
  CLAIM_TTL_MS: "2000",
  GATE_TTL_MS: "10000",
  DRAIN_DEADLINE_MS: "3000",
};

// ---------- assert bookkeeping: [PASS]/[FAIL] as we go, SUMMARY at the end ----------
const log: { label: string; pass: boolean }[] = [];
const failures: string[] = [];
function check(cond: boolean, label: string, detail?: string): boolean {
  log.push({ label, pass: cond });
  console.log((cond ? "[PASS] " : "[FAIL] ") + label + (cond ? "" : "  << actual: " + (detail ?? "condition false") + " >>"));
  if (!cond) failures.push(label);
  return cond;
}
/** A failed prerequisite makes every downstream step meaningless — abort the scenario. */
function abortIf(cond: boolean, label: string, detail?: string): void {
  if (!check(cond, label, detail)) throw new Error("prerequisite failed: " + label);
}

// ---------- HTTP helpers ----------
type R = { status: number; json: any };
async function call(port: number, path: string, method: string, body: unknown, auth: boolean): Promise<R> {
  for (let attempt = 0; ; attempt++) {
    const res = await fetch("http://127.0.0.1:" + port + path, {
      method,
      headers: { ...(body === undefined ? {} : { "content-type": "application/json" }), ...(auth ? { "x-api-key": KEY } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    let json: any = null;
    try { json = await res.json(); } catch { /* non-JSON body */ }
    if (res.status !== 503 || attempt >= 4) return { status: res.status, json };
    await Bun.sleep(60); // 503 = "busy, come back later" (writes are idempotent by key)
  }
}
const api = (method: string, port: number, path: string, body?: unknown): Promise<R> => call(port, path, method, body, true);
const sim = (method: string, path: string, body?: unknown): Promise<R> => call(BANK, path, method, body, false);

// ---------- bounded polling ----------
async function waitUntil(fn: () => Promise<boolean>, budgetMs: number, label: string, stepMs = 25): Promise<void> {
  const t0 = Date.now();
  while (!(await fn().catch(() => false))) {
    if (Date.now() - t0 > budgetMs) throw new Error(label + ": not achieved within " + budgetMs + "ms");
    await Bun.sleep(stepMs);
  }
}
async function waitHealth(port: number, label: string): Promise<void> {
  await waitUntil(async () => {
    try { return (await fetch("http://127.0.0.1:" + port + "/healthz")).ok; }
    catch { return false; } // connection refused costs ~0.1ms per attempt
  }, 5000, label + " never became healthy on :" + port, 10);
  check(true, label + " healthy on :" + port);
}
/** Await with a deadline; handlers attach immediately, so no dangling rejections. */
function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(label + ": timed out after " + ms + "ms")), ms);
    p.then((v) => { clearTimeout(timer); resolve(v); }, (e) => { clearTimeout(timer); reject(e); });
  });
}

// ---------- child processes ----------
type Kid = { name: string; proc: any; killed: boolean; drains: boolean; settled: boolean };
const kids: Kid[] = [];
function spawn(name: string, file: string, extra: Record<string, string>, drains: boolean): any {
  const proc = Bun.spawn({
    cmd: [process.execPath, file],
    cwd: ROOT,
    env: { ...process.env, ...CHILD_ENV, ...extra },
    stdout: "inherit", // line-atomic across processes; children self-prefix logs
    stderr: "inherit",
  });
  kids.push({ name, proc, killed: false, drains, settled: false });
  return proc;
}
/** Exit code with a wedge-breaker: escalate to SIGKILL so no orphan can survive. */
async function waitExit(k: Kid, budgetMs = 6000): Promise<number | null> {
  for (let i = 0; i < 2; i++) {
    try { return await withTimeout(k.proc.exited, budgetMs, k.name + " exit"); }
    catch { try { k.proc.kill("SIGKILL"); } catch {} }
  }
  return null;
}
async function stopKid(name: string): Promise<number | null> {
  const k = kids.find((x) => x.name === name)!;
  if (!k.killed) { k.killed = true; try { k.proc.kill(); } catch {} } // SIGTERM once; double-kill guarded
  const code = await waitExit(k);
  k.settled = true;
  return code;
}
/** Final sweep: SIGTERM everything still alive, then await and check them all. */
async function teardown(): Promise<void> {
  for (const k of kids) if (!k.killed) { k.killed = true; try { k.proc.kill(); } catch {} }
  for (const k of kids) {
    const code = await waitExit(k);
    if (!k.settled && k.drains) {
      check(code === 0, "teardown: SIGTERM " + k.name + " -> graceful drain, exit 0", "exit code " + code);
    } else {
      console.log("        " + k.name + " exit code " + code + (k.settled ? " (asserted above)" : " (default SIGTERM disposition: no drain handler)"));
    }
  }
}
// external signals must not orphan children
process.on("SIGTERM", () => { for (const k of kids) { try { k.proc.kill(); } catch {} } process.exit(143); });
process.on("SIGINT", () => { for (const k of kids) { try { k.proc.kill(); } catch {} } process.exit(130); });

const payoutBy = (id: number, port: number): Promise<R> => api("GET", port, "/v1/payouts/" + id);

// ---------- the scenario ----------
async function main(): Promise<void> {
  for (const suffix of ["", "-wal", "-shm"]) rmSync(DB_ABS + suffix, { force: true }); // fresh DB per run

  // topology: bank FIRST, then 2 apis on ONE db, then 3 workers
  spawn("bank", "apps/simbank.ts", { BANK_PORT: String(BANK) }, false);
  await waitHealth(BANK, "simbank");
  spawn("api-1", "apps/api.ts", { PORT: String(API1) }, false);
  spawn("api-2", "apps/api.ts", { PORT: String(API2) }, false);
  await waitHealth(API1, "api-1");
  await waitHealth(API2, "api-2");
  spawn("w1", "apps/worker.ts", { WORKER_ID: "w1", WORKER_HEALTH_PORT: "4211" }, true);
  spawn("w2", "apps/worker.ts", { WORKER_ID: "w2", WORKER_HEALTH_PORT: "4212" }, true);
  spawn("w3", "apps/worker.ts", { WORKER_ID: "w3", WORKER_HEALTH_PORT: "4213" }, true);
  await waitHealth(4211, "worker w1");
  await waitHealth(4212, "worker w2");
  await waitHealth(4213, "worker w3");

  // (0) fund the tenant
  const fund = await api("POST", API1, "/admin/fund", { account_id: 1, amount_paise: 10000000 });
  abortIf(fund.status === 200 && fund.json?.available_paise === 10000000, "fund: available_paise == 10000000", JSON.stringify(fund.json));

  // (a) pay-in end-to-end: order -> bank credit -> claim -> sync/matcher -> succeeded
  const po = await api("POST", API1, "/v1/payin-orders", { account_id: 1, amount_paise: 25000, client_order_id: "e2e-a" });
  abortIf(po.status === 201 && typeof po.json?.order_id === "number" && !!po.json?.bank_details, "payin order e2e-a created (201 + bank_details)", "status " + po.status + " " + JSON.stringify(po.json));
  const orderId = po.json.order_id;
  const credit = await sim("POST", "/sim/credits", { account_id: 1, utr: "E2E-A", amount_paise: 25000 });
  abortIf(credit.status === 200 && typeof credit.json?.bank_txn_id === "string", "bank credit E2E-A recorded", "status " + credit.status + " " + JSON.stringify(credit.json));
  const claim = await api("POST", API1, "/v1/payin-orders/" + orderId + "/claim", { utr: "E2E-A" });
  abortIf(claim.status === 200 && (claim.json?.status === "succeeded" || claim.json?.status === "claimed"), "claim accepted (succeeded OR claimed — either matcher side may win)", JSON.stringify(claim.json));
  await waitUntil(async () => (await api("GET", API2, "/v1/payin-orders/" + orderId)).json?.status === "succeeded", 10000, "payin e2e-a succeeded");
  check(true, "payin order e2e-a reached status=succeeded (bank sync + matcher)");
  const balA = await api("GET", API1, "/v1/balance");
  check(balA.json?.available_paise === 10000000 + 25000, "balance +25000 after payin", JSON.stringify(balA.json));
  const verA = await api("GET", API2, "/admin/verify");
  check(verA.json?.ok === true && verA.json?.escrow_net_paise === 0, "verify ok:true AND escrow_net_paise == 0 after payin", JSON.stringify(verA.json));

  // (b) single payout end-to-end: created on api-2, settled by a worker
  const payB = await api("POST", API2, "/v1/payouts", { amount_paise: 10000, beneficiary: BEN, idempotency_key: "e2e-b" });
  abortIf(payB.status === 201 && typeof payB.json?.payout_id === "number", "payout e2e-b created (201) on api-2", "status " + payB.status + " " + JSON.stringify(payB.json));
  const idB = payB.json.payout_id;
  await waitUntil(async () => (await payoutBy(idB, API1)).json?.status === "settled", 10000, "payout e2e-b settled");
  check(true, "payout e2e-b settled (outbox relay -> bank submit -> reconcile)");
  const subsB = await sim("GET", "/sim/submissions");
  const subB = (subsB.json?.submissions ?? []).find((s: any) => s.idempotency_key === "e2e-b");
  check(subB?.submit_count === 1, "bank saw e2e-b exactly once (submit_count 1)", JSON.stringify(subsB.json?.submissions));
  const balB = await api("GET", API2, "/v1/balance");
  check(balB.json?.available_paise === 10000000 + 25000 - 10000, "balance -10000 after e2e-b", JSON.stringify(balB.json));

  // (c1) THE scaling proof: 25 CONCURRENT creates, ONE idempotency key, BOTH apis
  const burst = await Promise.all(Array.from({ length: 25 }, (_, i) =>
    api("POST", i % 2 === 0 ? API1 : API2, "/v1/payouts", { amount_paise: 10000, beneficiary: BEN, idempotency_key: "e2e-burst" })));
  const burstIds = new Set(burst.map((r) => r.json?.payout_id));
  abortIf(burstIds.size === 1 && typeof burst[0].json?.payout_id === "number", "burst: 25 concurrent same-key creates -> ONE payout id", "statuses " + burst.map((r) => r.status).join(",") + " ids " + [...burstIds].join(","));
  const idBurst = burst[0].json.payout_id;
  await waitUntil(async () => (await payoutBy(idBurst, API2)).json?.status === "settled", 15000, "burst payout settled");
  check(true, "burst payout e2e-burst settled");
  const subsBurst = await sim("GET", "/sim/submissions");
  const subBurst = (subsBurst.json?.submissions ?? []).find((s: any) => s.idempotency_key === "e2e-burst");
  check(subBurst?.submit_count === 1, "bank saw e2e-burst EXACTLY once (submit_count 1)", JSON.stringify(subBurst));

  // (c2) 25 more with DISTINCT keys, spread across BOTH apis
  const many = await Promise.all(Array.from({ length: 25 }, (_, i) =>
    api("POST", i % 2 === 0 ? API2 : API1, "/v1/payouts", { amount_paise: 10000, beneficiary: BEN, idempotency_key: "e2e-d" + (i + 1) })));
  const manyIds = many.map((r) => r.json?.payout_id);
  abortIf(new Set(manyIds).size === 25 && many.every((r) => r.status === 201), "25 distinct-key creates -> 25 distinct payout ids (both apis)", "statuses " + many.map((r) => r.status).join(","));
  await waitUntil(async () => {
    const rs = await Promise.all(manyIds.map((id, i) => payoutBy(id, i % 2 === 0 ? API1 : API2)));
    return rs.every((r) => r.json?.status === "settled");
  }, 25000, "all 25 distinct payouts settled", 100);
  check(true, "all 25 distinct payouts settled");
  const subs = await sim("GET", "/sim/submissions");
  check(subs.json?.total_submits === 27, "bank total submissions == 27 (1+1+25 — exactly-once effects)", JSON.stringify(subs.json));
  const verC = await api("GET", API1, "/admin/verify");
  check(verC.json?.ok === true, "ledger verify ok:true after the burst", JSON.stringify(verC.json?.problems));
  const balC = await api("GET", API2, "/v1/balance");
  const expectAvail = 10000000 + 25000 - 27 * 10000;
  check(balC.json?.available_paise === expectAvail && balC.json?.held_paise === 0, "balance == funded+payin-27*10000 (" + expectAvail + ") AND held_paise == 0", JSON.stringify(balC.json));
  const availBeforeCrash = balC.json?.available_paise as number;

  // multi-worker visibility (log-only: distribution is NOT a correctness claim)
  const evC = await api("GET", API1, "/admin/events");
  const claimedWorkers = new Set((evC.json?.events ?? [])
    .filter((e: any) => String(e.event_type).startsWith("payout.") && e.claimed_by)
    .map((e: any) => String(e.claimed_by).replace(/-\d+$/, "")));
  console.log("[INFO] payout outbox events were relay-claimed by " + claimedWorkers.size + " distinct worker(s): " + [...claimedWorkers].sort().join(", "));

  // (d) deterministic cross-process crash + recovery by a DIFFERENT worker
  const drained: string[] = [];
  for (const n of ["w1", "w2", "w3"]) drained.push(n + ":" + (await stopKid(n)));
  check(drained.every((s) => s.endsWith(":0")), "SIGTERM w1/w2/w3 -> graceful drain, exit 0 each", drained.join(" "));
  spawn("w4-crash", "apps/worker.ts", { WORKER_ID: "w4-crash", WORKER_HEALTH_PORT: "4214", FAULT_CRASH_AFTER_HOLD: "1" }, false);
  await waitHealth(4214, "worker w4-crash (fault armed)");
  const payC = await api("POST", API1, "/v1/payouts", { amount_paise: 10000, beneficiary: BEN, idempotency_key: "e2e-crash" });
  abortIf(payC.status === 201 && typeof payC.json?.payout_id === "number", "payout e2e-crash created (w4-crash is the only live worker)", "status " + payC.status + " " + JSON.stringify(payC.json));
  const idCrash = payC.json.payout_id;
  const w4 = kids.find((k) => k.name === "w4-crash")!;
  const w4code = await withTimeout(w4.proc.exited, 10000, "w4-crash exited"); // the fault kills it — no SIGTERM from us
  w4.killed = true; w4.settled = true; // already dead; teardown must not signal it again
  check(w4code === 1, "w4-crash died for real (exit 1) right after placing the hold", "exit code " + w4code);
  const subsCrash = await sim("GET", "/sim/submissions");
  check(!(subsCrash.json?.submissions ?? []).some((s: any) => s.idempotency_key === "e2e-crash"), "bank NEVER saw e2e-crash (no submission — the money never left)", JSON.stringify(subsCrash.json?.submissions));
  spawn("w5", "apps/worker.ts", { WORKER_ID: "w5", WORKER_HEALTH_PORT: "4215" }, true);
  spawn("w6", "apps/worker.ts", { WORKER_ID: "w6", WORKER_HEALTH_PORT: "4216" }, true);
  await waitHealth(4215, "worker w5");
  await waitHealth(4216, "worker w6");
  await waitUntil(async () => (await payoutBy(idCrash, API2)).json?.status === "failed", 15000, "e2e-crash recovered to failed");
  check(true, "e2e-crash recovered: stale held row -> bank not_found (definitive) -> failed — by a DIFFERENT worker");
  const dead = await payoutBy(idCrash, API1);
  check(String(dead.json?.bank_error ?? "").includes("bank_not_found_never_submitted"), "bank_error carries bank_not_found_never_submitted", JSON.stringify(dead.json));
  const balD = await api("GET", API1, "/v1/balance");
  check(balD.json?.held_paise === 0 && balD.json?.available_paise === availBeforeCrash, "crash hold refunded: held_paise 0, available_paise restored to " + availBeforeCrash, JSON.stringify(balD.json));
  const verD = await api("GET", API2, "/admin/verify");
  check(verD.json?.ok === true, "ledger verify ok:true after crash recovery", JSON.stringify(verD.json?.problems));
  await waitUntil(async () => {
    const ev = await api("GET", API1, "/admin/events");
    const created = (ev.json?.events ?? []).filter((e: any) => e.event_type === "payout.created");
    return created.length > 0 && created[created.length - 1].dispatched_at !== null; // last payout.created == e2e-crash: nothing is created after it
  }, 10000, "orphaned payout.created event dispatched");
  check(true, "orphaned e2e-crash payout.created event re-claimed and dispatched by a live worker (dispatched_at non-null)");
}

// ---------- run: scenario, then guaranteed teardown, then SUMMARY ----------
try {
  await main();
} catch (e) {
  const msg = e instanceof Error ? e.message : String(e);
  log.push({ label: "scenario aborted: " + msg, pass: false });
  failures.push("scenario aborted: " + msg);
  console.log("[FAIL] scenario aborted: " + msg);
} finally {
  try { await teardown(); } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    failures.push("teardown error: " + msg);
    console.log("[FAIL] teardown error: " + msg);
  }
}

console.log("");
console.log("==== E2E SUMMARY ====");
for (const e of log) console.log((e.pass ? "  [PASS] " : "  [FAIL] ") + e.label);
console.log("checks: " + log.length + "  failed: " + failures.length);
console.log("postmortem db kept at: " + DB_ABS);
console.log("OVERALL: " + (failures.length === 0 ? "PASS" : "FAIL"));
process.exit(failures.length === 0 ? 0 : 1);
