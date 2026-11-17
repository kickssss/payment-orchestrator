import { makeFakeBank, type PayoutStatus } from "../src/platform/fakebank.ts";
import { config } from "../src/platform/config.ts";
import type { Clock } from "../src/core/clock.ts";

/** Simbank — the fake bank as its own HTTP process: the provider stand-in,
 *  external to the orchestrator DB (no sqlite, no runtime wiring). The fake
 *  records every submission durably BEFORE a fault can eat the response, so
 *  a 5xx here is always ambiguous, never definitive evidence. */

const clock: Clock = { now: () => Date.now(), setNow: () => {}, advance: () => {} };
const bank = makeFakeBank(clock);

const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const badRequest = () => json(400, { error: "bad_request" });
const msg = (e: unknown) => (e instanceof Error ? e.message : String(e));
const isInt = (v: unknown): v is number => Number.isInteger(v);
const toInt = (v: string | null): number | null => { const n = v === null ? NaN : Number(v); return Number.isInteger(n) ? n : null; };

async function readJson(req: Request): Promise<Record<string, unknown>> {
  try {
    const v: unknown = await req.json(); // throws SyntaxError on malformed/empty bodies -> 400 below
    return v && typeof v === "object" ? (v as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

const seen = new Map<string, { status: PayoutStatus; bank_ref: string }>(); // shadow of the fake's private submissions Map: status from the submit outcome
let refSeq = 0;
const nextRef = () => "BR" + String(++refSeq).padStart(5, "0"); // mirrors the fake's one-ref-per-new-key counter ("BR00001", ...)

async function handle(req: Request): Promise<Response> {
  const u = new URL(req.url);
  const seg = u.pathname.split("/").filter(Boolean);

  if (req.method === "GET" && seg.length === 1 && seg[0] === "healthz") return json(200, { ok: true, role: "bank" });

  if (req.method === "GET" && seg.length === 2 && seg[0] === "txns") {
    const accountId = toInt(seg[1]);
    const from = toInt(u.searchParams.get("from"));
    const to = toInt(u.searchParams.get("to"));
    if (accountId === null || from === null || to === null) return badRequest();
    return json(200, await bank.fetchTransactions(accountId, from, to));
  }

  if (req.method === "POST" && seg.length === 2 && seg[0] === "payouts") {
    const accountId = toInt(seg[1]);
    const { idempotencyKey, amountPaise, beneficiary } = await readJson(req);
    if (accountId === null || typeof idempotencyKey !== "string" || idempotencyKey === "" || !isInt(amountPaise) || typeof beneficiary !== "string") return badRequest();
    const duplicate = seen.has(idempotencyKey);
    try {
      const sub = await bank.initiatePayout(accountId, { idempotencyKey, amountPaise, beneficiary });
      if (!duplicate) seen.set(idempotencyKey, { status: sub.status === "submitted" ? "executed" : "rejected", bank_ref: sub.status === "submitted" ? sub.bankRef : nextRef() });
      return json(200, sub);
    } catch (e) {
      if (!duplicate) seen.set(idempotencyKey, { status: "pending", bank_ref: nextRef() }); // the fake recorded it before throwing
      console.error(new Date().toISOString() + " [bank] submit error: " + msg(e));
      return json(500, { error: msg(e) }); // ambiguous — never a definitive answer
    }
  }

  if (req.method === "GET" && seg.length === 2 && seg[0] === "payout-status") return json(200, await bank.getPayoutStatus(seg[1]));

  if (req.method === "POST" && seg.length === 2 && seg[0] === "sim" && seg[1] === "credits") {
    const { account_id, utr, amount_paise, booked_at } = await readJson(req);
    if (!isInt(account_id) || typeof utr !== "string" || !isInt(amount_paise) || (booked_at !== undefined && !isInt(booked_at))) return badRequest();
    const txn = bank.addCredit(account_id, { utr, amountPaise: amount_paise, bookedAt: isInt(booked_at) ? booked_at : undefined });
    return json(200, { bank_txn_id: txn.bank_txn_id });
  }

  if (req.method === "GET" && seg.length === 2 && seg[0] === "sim" && seg[1] === "submissions") {
    const submissions = [...seen.entries()].map(([idempotency_key, r]) => ({ idempotency_key, status: r.status, bank_ref: r.bank_ref, submit_count: bank.submitCount(idempotency_key) }));
    return json(200, { submissions, total_submits: bank.submitLog.length });
  }

  return json(404, { error: "not_found" });
}

async function safeHandle(req: Request): Promise<Response> {
  try {
    return await handle(req);
  } catch (e) {
    // any unexpected route failure answers a plain JSON 500 (with development:false, never a dev HTML page)
    console.error(new Date().toISOString() + " [bank] unhandled route error: " + msg(e));
    return json(500, { error: msg(e) });
  }
}

Bun.serve({ port: config.bankPort, hostname: "127.0.0.1", development: false, fetch: safeHandle });
console.log(new Date().toISOString() + " [bank] listening on port " + config.bankPort);
