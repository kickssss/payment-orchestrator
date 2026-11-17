import type { Clock } from "../core/clock.ts";

/** Scripted stand-in for a real bank / aggregator connector (plan §8 interface,
 *  POC subset). The idempotency contract is the core of the saga tests:
 *  every submission is recorded durably BEFORE anything else can happen, and
 *  repeat calls with the same key return the same answer — the bank can never
 *  execute the same payout twice. A "timeout" is recorded as pending and the
 *  response is lost, exactly like a real ambiguous submit.
 *  not_found is contractually definitive here: every submit is recorded. */

export type BankTxn = {
  account_id: number;
  bank_txn_id: string;
  utr: string | null;
  direction: "credit" | "debit";
  amount_paise: number;
  booked_at: number;
  ref: string | null; // payout idempotency key on payout debits
};

export type PayoutRequest = { idempotencyKey: string; amountPaise: number; beneficiary: string };
export type PayoutSubmission = { bankRef: string; status: "submitted" } | { status: "rejected"; error: string };
export type PayoutStatus = "executed" | "rejected" | "pending" | "not_found";

/** FIFO fault scripts consumed at the call site:
 *  - reject_submit:  bank rejects the payout (positive evidence)
 *  - timeout_submit: bank records the submission, response is lost (ambiguous)
 *  - status_error:   status call fails (no evidence either way) */
export type Fault = "reject_submit" | "timeout_submit" | "status_error";

export function makeFakeBank(clock: Clock) {
  const txns: BankTxn[] = [];
  const submissions = new Map<string, { status: PayoutStatus; request: PayoutRequest; bankRef: string }>();
  const submitLog: string[] = [];
  const statusLog: string[] = [];
  let faults: Fault[] = [];
  let txnSeq = 0;
  let refSeq = 0;

  function addCredit(accountId: number, e: { utr: string | null; amountPaise: number; bookedAt?: number }): BankTxn {
    const txn: BankTxn = {
      account_id: accountId,
      bank_txn_id: "BTX" + String(++txnSeq).padStart(6, "0"),
      utr: e.utr,
      direction: "credit",
      amount_paise: e.amountPaise,
      booked_at: e.bookedAt ?? clock.now(),
      ref: null,
    };
    txns.push(txn);
    return txn;
  }

  function addDebit(accountId: number, e: { utr: string | null; amountPaise: number; bookedAt?: number; ref?: string | null }): BankTxn {
    const txn: BankTxn = {
      account_id: accountId,
      bank_txn_id: "BTX" + String(++txnSeq).padStart(6, "0"),
      utr: e.utr,
      direction: "debit",
      amount_paise: e.amountPaise,
      booked_at: e.bookedAt ?? clock.now(),
      ref: e.ref ?? null,
    };
    txns.push(txn);
    return txn;
  }

  function setFaults(...f: Fault[]): void {
    faults = [...f];
  }

  async function fetchTransactions(accountId: number, fromMs: number, toMs: number): Promise<BankTxn[]> {
    // inclusive [from, to]: a statement pulled "to now" must include txns
    // booked at this instant (the plan's watermark minus overlap to now)
    return txns
      .filter((t) => t.account_id === accountId && t.booked_at >= fromMs && t.booked_at <= toMs)
      .map((t) => ({ ...t }));
  }

  async function initiatePayout(accountId: number, req: PayoutRequest): Promise<PayoutSubmission> {
    const existing = submissions.get(req.idempotencyKey);
    if (existing) {
      if (existing.status === "rejected") return { status: "rejected", error: "duplicate of rejected submission" };
      return { bankRef: existing.bankRef, status: "submitted" };
    }
    submitLog.push(req.idempotencyKey); // recorded durably, first thing
    const bankRef = "BR" + String(++refSeq).padStart(5, "0");
    const fault = faults.shift();
    if (fault === "reject_submit") {
      submissions.set(req.idempotencyKey, { status: "rejected", request: req, bankRef });
      return { status: "rejected", error: "BANK_REJECTED_INSUFFICIENT_FUNDS" };
    }
    if (fault === "timeout_submit") {
      submissions.set(req.idempotencyKey, { status: "pending", request: req, bankRef }); // recorded; response lost
      throw new Error("BANK_TIMEOUT");
    }
    submissions.set(req.idempotencyKey, { status: "executed", request: req, bankRef });
    addDebit(accountId, { utr: "PAYOUT-" + req.idempotencyKey, amountPaise: req.amountPaise, ref: req.idempotencyKey });
    return { bankRef, status: "submitted" };
  }

  async function getPayoutStatus(idempotencyKey: string): Promise<PayoutStatus> {
    statusLog.push(idempotencyKey);
    const fault = faults.shift();
    if (fault === "status_error") throw new Error("STATUS_UNSUPPORTED");
    const s = submissions.get(idempotencyKey);
    return s ? s.status : "not_found";
  }

  /** Test hook: flip a recorded (timed-out) submission once evidence exists. */
  function setStatus(idempotencyKey: string, status: PayoutStatus, accountId?: number): void {
    const s = submissions.get(idempotencyKey);
    if (!s) throw new Error("no such submission: " + idempotencyKey);
    s.status = status;
    if (status === "executed" && accountId !== undefined) {
      addDebit(accountId, { utr: "PAYOUT-" + idempotencyKey, amountPaise: s.request.amountPaise, ref: idempotencyKey });
    }
  }

  function submitCount(idempotencyKey: string): number {
    return submitLog.filter((k) => k === idempotencyKey).length;
  }

  return { addCredit, addDebit, setFaults, fetchTransactions, initiatePayout, getPayoutStatus, setStatus, submitCount, submitLog, statusLog };
}

export type Bank = ReturnType<typeof makeFakeBank>;
