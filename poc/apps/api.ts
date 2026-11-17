import { timingSafeEqual } from "node:crypto";
import { isBusy, one, q, TENANT_ID } from "../src/core/db.ts";
import { config } from "../src/platform/config.ts";
import { makeHttpBank } from "../src/platform/connector.ts";
import { createRuntime } from "../src/platform/runtime.ts";
import { asObj, asPosInt, asString, HttpError, json, readBody, route } from "../src/platform/httpkit.ts";

/** The tenant/admin API (plan §6): STATELESS and xN-safe — every api process
 *  opens the SAME SQLite file through createRuntime and keeps no state of its
 *  own, so N of them can run behind anything. The api role NEVER runs the
 *  outbox relay (the worker owns dispatch; the api only emits rows inside the
 *  effect transactions) and NEVER talks to the bank — the runtime's bank
 *  parameter satisfies the wiring signature, nothing more. All money safety
 *  lives in the guarded SQL of the core modules this file only calls. */

/** Cold-boot race (found in the xN smoke): db.ts installs busy_timeout AFTER
 *  the WAL pragma, so N processes opening a FRESH db file in the same instant
 *  can crash the losers with SQLITE_BUSY at boot. Same policy as request
 *  handling below: small jittered retry. (Proper fix = reorder the PRAGMAs in
 *  db.ts — not this file's to touch.) */
async function bootRuntime(attempt = 0) {
  try {
    return createRuntime({ dbPath: config.dbPath, instanceId: "api-" + process.pid, bank: makeHttpBank(config.bankUrl) });
  } catch (e) {
    if (attempt < 8 && isBusy(e)) {
      await new Promise((resolve) => setTimeout(resolve, 30 + Math.random() * 50));
      return bootRuntime(attempt + 1);
    }
    throw e;
  }
}

const runtime = await bootRuntime();
const { db, payin, payout } = runtime;

function log(msg: string): void {
  console.log(new Date().toISOString() + " [api] " + msg);
}

/** x-api-key check, constant-time: length first (timingSafeEqual throws on a length mismatch). */
function authorized(req: Request): boolean {
  const given = Buffer.from(req.headers.get("x-api-key") ?? "");
  const want = Buffer.from(config.apiKey);
  return given.length === want.length && timingSafeEqual(given, want);
}

type Ctx = { segs: string[]; body: unknown };
type Handler = (ctx: Ctx) => Response | Promise<Response>;

/** The ":id" slot sits at index 2 in every id-bearing route. */
function idParam(ctx: Ctx): number {
  return asPosInt(Number(ctx.segs[2]), "id");
}

/** The body shared by GET /v1/balance and POST /admin/fund. */
function balanceBody() {
  const t = one<{ available_paise: number; held_paise: number }>(
    db, "SELECT available_paise, held_paise FROM tenant_balances WHERE tenant_id = ?", TENANT_ID
  )!;
  const accounts = q<{ account_id: number; available_paise: number }>(
    db, "SELECT account_id, available_paise FROM account_balances ORDER BY account_id"
  );
  return { available_paise: t.available_paise, held_paise: t.held_paise, accounts };
}

const healthz: Handler = () => json(200, { ok: true, role: "api" });

const createPayinOrder: Handler = (ctx) => {
  const b = asObj(ctx.body, "body");
  const accountId = asPosInt(b.account_id, "account_id");
  const amountPaise = asPosInt(b.amount_paise, "amount_paise");
  const clientOrderId = asString(b.client_order_id, "client_order_id");
  const r = payin.createOrder({ accountId, amountPaise, clientOrderId });
  if (!r.ok) {
    if (r.error === "CLIENT_ORDER_EXISTS") throw new HttpError(409, "client_order_exists");
    throw new HttpError(422, r.error === "NO_CAPACITY" ? "no_capacity" : "account_not_found");
  }
  const order = one<{ expires_at: number }>(db, "SELECT expires_at FROM payin_orders WHERE id = ?", r.orderId);
  const acct = one<{ account_number: string; ifsc: string; holder_name: string }>(
    db, "SELECT account_number, ifsc, holder_name FROM bank_accounts WHERE id = ?", accountId
  );
  if (!order || !acct) throw new HttpError(500, "internal"); // both rows just committed — unreachable
  return json(201, {
    order_id: r.orderId,
    status: "pending",
    bank_details: { account_number: acct.account_number, ifsc: acct.ifsc, holder_name: acct.holder_name },
    expires_at: order.expires_at,
  });
};

const claimPayinOrder: Handler = (ctx) => {
  const orderId = idParam(ctx);
  const b = asObj(ctx.body, "body");
  const utr = asString(b.utr, "utr");
  if (!one(db, "SELECT id FROM payin_orders WHERE id = ?", orderId)) throw new HttpError(404, "not_found");
  const r = payin.claim({ orderId, utr });
  const body: Record<string, unknown> = { status: r.status };
  if ("reviewId" in r) body.review_id = r.reviewId;
  return json(200, body);
};

const getPayinOrder: Handler = (ctx) => {
  const o = one<{ id: number; status: string; amount_paise: number; claimed_utr: string | null; expires_at: number; utr_deadline: number | null }>(
    db,
    "SELECT id, status, amount_paise, claimed_utr, expires_at, utr_deadline FROM payin_orders WHERE id = ?",
    idParam(ctx)
  );
  if (!o) throw new HttpError(404, "not_found");
  return json(200, { order_id: o.id, status: o.status, amount_paise: o.amount_paise, claimed_utr: o.claimed_utr, expires_at: o.expires_at, utr_deadline: o.utr_deadline });
};

const createPayout: Handler = (ctx) => {
  const b = asObj(ctx.body, "body");
  const r = payout.create({
    tenantId: TENANT_ID,
    amountPaise: asPosInt(b.amount_paise, "amount_paise"),
    beneficiary: JSON.stringify(asObj(b.beneficiary, "beneficiary")),
    idempotencyKey: asString(b.idempotency_key, "idempotency_key"),
  });
  if (!r.ok) throw new HttpError(422, r.error); // create has no failure path today; the door stays shut
  return json(r.existing ? 200 : 201, { payout_id: r.payoutId, status: r.status, existing: r.existing });
};

const getPayout: Handler = (ctx) => {
  const p = one<{ id: number; status: string; amount_paise: number; account_id: number | null; idempotency_key: string; attempts: number; bank_ref: string | null; bank_error: string | null }>(
    db,
    "SELECT id, status, amount_paise, account_id, idempotency_key, attempts, bank_ref, bank_error FROM payouts WHERE id = ?",
    idParam(ctx)
  );
  if (!p) throw new HttpError(404, "not_found");
  return json(200, { payout_id: p.id, status: p.status, amount_paise: p.amount_paise, account_id: p.account_id, idempotency_key: p.idempotency_key, attempts: p.attempts, bank_ref: p.bank_ref, bank_error: p.bank_error });
};

const balance: Handler = () => json(200, balanceBody());

const fund: Handler = (ctx) => {
  const b = asObj(ctx.body, "body");
  const accountId = asPosInt(b.account_id, "account_id");
  const amountPaise = asPosInt(b.amount_paise, "amount_paise");
  if (!one(db, "SELECT id FROM bank_accounts WHERE id = ?", accountId)) throw new HttpError(422, "account_not_found");
  runtime.fund(accountId, amountPaise);
  return json(200, balanceBody());
};

const verify: Handler = () => {
  const v = runtime.verify();
  return json(200, { ok: v.ok, problems: v.problems, groups: v.groups, escrow_net_paise: v.escrowNetPaise });
};

const events: Handler = () =>
  json(200, { events: q(db, "SELECT event_id, event_type, occurred_at, claimed_by, dispatched_at FROM outbox ORDER BY occurred_at, event_id") });

/** The route table IS the router (":x" = parameter slot). */
const routes: { method: string; pattern: string[]; hasBody: boolean; run: Handler }[] = [
  { method: "GET", pattern: ["healthz"], hasBody: false, run: healthz },
  { method: "POST", pattern: ["v1", "payin-orders"], hasBody: true, run: createPayinOrder },
  { method: "POST", pattern: ["v1", "payin-orders", ":id", "claim"], hasBody: true, run: claimPayinOrder },
  { method: "GET", pattern: ["v1", "payin-orders", ":id"], hasBody: false, run: getPayinOrder },
  { method: "POST", pattern: ["v1", "payouts"], hasBody: true, run: createPayout },
  { method: "GET", pattern: ["v1", "payouts", ":id"], hasBody: false, run: getPayout },
  { method: "GET", pattern: ["v1", "balance"], hasBody: false, run: balance },
  { method: "POST", pattern: ["admin", "fund"], hasBody: true, run: fund },
  { method: "GET", pattern: ["admin", "verify"], hasBody: false, run: verify },
  { method: "GET", pattern: ["admin", "events"], hasBody: false, run: events },
];

function matchRoute(method: string, segs: string[]): (typeof routes)[number] | null {
  for (const cand of routes) {
    if (cand.method !== method || cand.pattern.length !== segs.length) continue;
    let ok = true;
    for (let i = 0; i < cand.pattern.length; i++) {
      if (!cand.pattern[i].startsWith(":") && cand.pattern[i] !== segs[i]) { ok = false; break; }
    }
    if (ok) return cand;
  }
  return null;
}

async function dispatch(req: Request): Promise<Response> {
  const parsed = route(req);
  if (!parsed) return json(400, { error: "bad_url" });
  if (!(parsed.segments.length === 1 && parsed.segments[0] === "healthz") && !authorized(req)) {
    return json(401, { error: "unauthorized" });
  }
  const m = matchRoute(req.method, parsed.segments);
  if (!m) return json(404, { error: "not_found" });
  const body = m.hasBody ? await readBody(req) : undefined; // bad_json -> HttpError(400) via respond()
  try {
    return await m.run({ segs: parsed.segments, body });
  } catch (e) {
    if (e instanceof HttpError || !runtime.isBusy(e)) throw e; // classified once, in respond()
    // SQLITE_BUSY: ONE small jittered retry, then a clean "come back later".
    // Retrying a body route is safe: creates are idempotent by client key.
    await new Promise((resolve) => setTimeout(resolve, 30 + Math.random() * 50));
    try {
      return await m.run({ segs: parsed.segments, body });
    } catch (e2) {
      if (e2 instanceof HttpError) return json(e2.status, { error: e2.code });
      if (runtime.isBusy(e2)) return json(503, { error: "busy", retry_after_ms: 50 });
      throw e2; // logged + 500 in respond()
    }
  }
}

/** Error policy: HttpError -> its status {error: code}; busy -> one jittered
 *  retry then 503; anything unexpected -> stack to console.error + 500. Never throws. */
async function respond(req: Request): Promise<Response> {
  try {
    return await dispatch(req);
  } catch (e) {
    if (e instanceof HttpError) return json(e.status, { error: e.code });
    console.error(new Date().toISOString() + " [api] unexpected error:", e instanceof Error ? e.stack : e);
    return json(500, { error: "internal" });
  }
}

// development: false — an uncaught throw must never render a dev HTML page
// that leaks source lines. respond() itself never throws (everything is
// classified inside); the .catch is the last-resort backstop anyway.
Bun.serve({
  hostname: "127.0.0.1",
  port: config.port,
  development: false,
  fetch: (req) => respond(req).catch(() => json(500, { error: "internal" })),
});
log("listening on port " + config.port);
