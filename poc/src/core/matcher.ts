import { one, type Db } from "./db.ts";

/** ONE corroborator, used two-sided (plan §7): claim-time looks for an
 *  ingested credit; ingest-time looks for a live claim. Both share the same
 *  matching key (account, UTR) and the same success rule: credit direction +
 *  displayed account + exact amount. This module only READS — the guarded
 *  money moves live in payin.ts, so there is exactly one place money moves. */

export type CreditCandidate = { txnId: number; amountPaise: number };

export function findCreditForClaim(db: Db, accountId: number, utr: string): CreditCandidate | undefined {
  return one<CreditCandidate>(
    db,
    "SELECT id AS txnId, amount_paise AS amountPaise FROM bank_txns " +
      "WHERE account_id = ? AND utr = ? AND direction = 'credit' AND match_status = 'unmatched' " +
      "ORDER BY booked_at, id LIMIT 1",
    accountId,
    utr
  );
}

/** Rematch backstop search: only an EXACT-amount credit auto-succeeds a
 *  review-parked order — anything ambiguous stays for ops. */
export function findExactCreditForClaim(db: Db, accountId: number, utr: string, amountPaise: number): CreditCandidate | undefined {
  return one<CreditCandidate>(
    db,
    "SELECT id AS txnId, amount_paise AS amountPaise FROM bank_txns " +
      "WHERE account_id = ? AND utr = ? AND direction = 'credit' AND match_status = 'unmatched' AND amount_paise = ? " +
      "ORDER BY booked_at, id LIMIT 1",
    accountId,
    utr,
    amountPaise
  );
}

export type ClaimCandidate = { orderId: number; amountPaise: number };

export function findClaimForCredit(db: Db, accountId: number, utr: string): ClaimCandidate | undefined {
  return one<ClaimCandidate>(
    db,
    "SELECT id AS orderId, amount_paise AS amountPaise FROM payin_orders " +
      "WHERE account_id = ? AND claimed_utr = ? AND status = 'claimed' " +
      "ORDER BY claimed_at, id LIMIT 1",
    accountId,
    utr
  );
}

/** Reversal matching (plan §5 storno): a debit referencing a succeeded pay-in's
 *  UTR with amount + account corroboration. */
export function findReversalTarget(db: Db, accountId: number, utr: string, amountPaise: number): { orderId: number } | undefined {
  return one<{ orderId: number }>(
    db,
    "SELECT id AS orderId FROM payin_orders " +
      "WHERE account_id = ? AND claimed_utr = ? AND status = 'succeeded' AND amount_paise = ? " +
      "ORDER BY id LIMIT 1",
    accountId,
    utr,
    amountPaise
  );
}
