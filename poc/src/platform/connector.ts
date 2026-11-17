import type { BankTxn, PayoutRequest, PayoutSubmission, PayoutStatus } from "./fakebank.ts";

/** The provider seam (plan §8): everything bank-facing flows through this
 *  interface. The in-process fake bank (unit tests / demo) and the HTTP client
 *  (workers -> simbank process) both satisfy it — workers never know which
 *  bank implementation is behind it. */

export type { BankTxn, PayoutRequest, PayoutSubmission, PayoutStatus };

export interface BankConnector {
  fetchTransactions(accountId: number, fromMs: number, toMs: number): Promise<BankTxn[]>;
  initiatePayout(accountId: number, req: PayoutRequest): Promise<PayoutSubmission>;
  getPayoutStatus(idempotencyKey: string): Promise<PayoutStatus>;
}

/** HTTP client for the simbank process. Money rules:
 *  - the timeout (10 s) MUST stay below STALE_HELD_MS (60 s) so a slow submit
 *    can never outlive stale-held recovery;
 *  - NEVER retries initiatePayout — a network error or 5xx maps to the
 *    executor's ambiguous path (unknown, hold kept), exactly like a bank
 *    timeout; only the bank's own definitive answers are trusted. */
export function makeHttpBank(baseUrl: string, timeoutMs: number = 10_000): BankConnector {
  const base = baseUrl.replace(/\/+$/, "");
  async function call(path: string, init?: RequestInit): Promise<Response> {
    return fetch(base + path, {
      ...init,
      signal: AbortSignal.timeout(timeoutMs),
      headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
    });
  }
  return {
    async fetchTransactions(accountId, fromMs, toMs) {
      const r = await call("/txns/" + accountId + "?from=" + fromMs + "&to=" + toMs);
      if (!r.ok) throw new Error("simbank " + r.status + " on txns");
      return (await r.json()) as BankTxn[];
    },
    async initiatePayout(accountId, req) {
      const r = await call("/payouts/" + accountId, { method: "POST", body: JSON.stringify(req) });
      if (!r.ok) throw new Error("simbank " + r.status + " on payout submit");
      return (await r.json()) as PayoutSubmission;
    },
    async getPayoutStatus(idempotencyKey) {
      const r = await call("/payout-status/" + idempotencyKey);
      if (!r.ok) throw new Error("simbank " + r.status + " on payout status");
      return (await r.json()) as PayoutStatus;
    },
  };
}
