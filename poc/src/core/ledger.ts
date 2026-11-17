import { insert, one, q, run, withTxn, type Db } from "./db.ts";

/** Double-entry ledger. Every group sums to zero — asserted on write, before
 *  anything is inserted. Aggregates (tenant_balances / account_balances) are
 *  guarded rows updated in the SAME transaction as the entries.
 *  POC rule: postGroup() must be called inside withTxn().
 *
 *  Sign conventions (plan §5, v2 fix):
 *    pay-in confirmed:    debit bank_mirror X      · credit tenant_operating X
 *    payout hold:        debit tenant_operating X · credit tenant_holds X
 *    payout settled:     debit tenant_holds X     · credit bank_mirror X
 *    hold released:      debit tenant_holds X     · credit tenant_operating X
 *    reversal (storno):  credit bank_mirror X     · debit tenant_operating X
 *  POC addition (review finding): every ingested credit first parks in escrow
 *    (debit bank_mirror · credit escrow) and consumes escrow on match, so
 *    bank_mirror always mirrors the physical bank, matched or not. */

export type TenantLedgerRef = { kind: "tenant_operating" | "tenant_holds" | "escrow"; tenantId: number };
export type BankLedgerRef = { kind: "bank_mirror"; accountId: number };
export type LedgerRef = TenantLedgerRef | BankLedgerRef;

export type Leg = { account: LedgerRef; direction: "debit" | "credit"; amountPaise: number };

export function ledgerKey(ref: LedgerRef): string {
  return ref.kind === "bank_mirror" ? "bank_mirror:" + ref.accountId : ref.kind + ":" + ref.tenantId;
}

function ensureLedgerAccount(db: Db, ref: LedgerRef): number {
  const key = ledgerKey(ref);
  run(db, "INSERT OR IGNORE INTO ledger_accounts (kind, key) VALUES (?, ?)", ref.kind, key);
  const row = one<{ id: number }>(db, "SELECT id FROM ledger_accounts WHERE kind = ? AND key = ?", ref.kind, key);
  if (!row) throw new Error("ledger account missing after ensure: " + key);
  return row.id;
}

function assertAmount(n: number, what: string): void {
  if (!Number.isSafeInteger(n) || n <= 0) throw new Error("invalid " + what + ": " + String(n));
}

/** Post one balanced group + update aggregates atomically. Returns the ledger_txn id. */
export function postGroup(
  db: Db,
  meta: { tenantId: number; kind: string; refType: string; refId: number; at: number },
  legs: Leg[]
): number {
  if (legs.length < 2) throw new Error("ledger group needs at least 2 legs");
  let debits = 0;
  let credits = 0;
  for (const leg of legs) {
    assertAmount(leg.amountPaise, "amount_paise");
    if (leg.direction === "debit") debits += leg.amountPaise;
    else credits += leg.amountPaise;
  }
  if (debits !== credits) {
    throw new Error("unbalanced ledger group: debits " + debits + " != credits " + credits);
  }

  const txnId = insert(
    db,
    "INSERT INTO ledger_txns (tenant_id, kind, ref_type, ref_id, created_at) VALUES (?, ?, ?, ?, ?)",
    meta.tenantId, meta.kind, meta.refType, meta.refId, meta.at
  );
  for (const leg of legs) {
    const accountId = ensureLedgerAccount(db, leg.account);
    run(
      db,
      "INSERT INTO ledger_entries (ledger_txn_id, ledger_account_id, direction, amount_paise) VALUES (?, ?, ?, ?)",
      txnId, accountId, leg.direction, leg.amountPaise
    );
    applyToAggregate(db, leg);
  }
  return txnId;
}

function applyToAggregate(db: Db, leg: Leg): void {
  const amt = leg.amountPaise;
  if (leg.account.kind === "tenant_operating") {
    const delta = leg.direction === "credit" ? amt : -amt;
    if (run(db, "UPDATE tenant_balances SET available_paise = available_paise + ? WHERE tenant_id = ?", delta, leg.account.tenantId) !== 1) {
      throw new Error("tenant_balances row missing for tenant " + leg.account.tenantId);
    }
  } else if (leg.account.kind === "tenant_holds") {
    const delta = leg.direction === "credit" ? amt : -amt;
    if (run(db, "UPDATE tenant_balances SET held_paise = held_paise + ? WHERE tenant_id = ?", delta, leg.account.tenantId) !== 1) {
      throw new Error("tenant_balances row missing for tenant " + leg.account.tenantId);
    }
  } else if (leg.account.kind === "bank_mirror") {
    const delta = leg.direction === "debit" ? amt : -amt;
    if (run(db, "UPDATE account_balances SET available_paise = available_paise + ? WHERE account_id = ?", delta, leg.account.accountId) !== 1) {
      throw new Error("account_balances row missing for account " + leg.account.accountId);
    }
  }
  // escrow: parked money, no aggregate row — the verifier reports its net
}

/** Test/demo convenience: an opening float the tenant wires in. */
export function openingDeposit(db: Db, tenantId: number, accountId: number, amountPaise: number, at: number): number {
  return withTxn(db, () =>
    postGroup(
      db,
      { tenantId, kind: "opening_deposit", refType: "seed", refId: accountId, at },
      [
        { account: { kind: "bank_mirror", accountId }, direction: "debit", amountPaise },
        { account: { kind: "tenant_operating", tenantId }, direction: "credit", amountPaise },
      ]
    )
  );
}

export type Verification = {
  ok: boolean;
  problems: string[];
  groups: number;
  escrowNetPaise: number;
};

/** Recompute everything from the immutable ledger and compare with the guarded aggregates. */
export function verifyLedger(db: Db): Verification {
  const problems: string[] = [];

  const groups = q<{ id: number; net: number }>(
    db,
    "SELECT ledger_txn_id AS id, SUM(CASE direction WHEN 'debit' THEN amount_paise ELSE -amount_paise END) AS net " +
      "FROM ledger_entries GROUP BY ledger_txn_id"
  );
  for (const g of groups) {
    if (g.net !== 0) problems.push("ledger group " + g.id + " does not sum to zero (net " + g.net + ")");
  }

  const tenantNets = new Map<string, number>();
  for (const r of q<{ k: string; net: number }>(
    db,
    "SELECT la.key AS k, SUM(CASE le.direction WHEN 'credit' THEN le.amount_paise ELSE -le.amount_paise END) AS net " +
      "FROM ledger_entries le JOIN ledger_accounts la ON la.id = le.ledger_account_id " +
      "WHERE la.kind IN ('tenant_operating','tenant_holds','escrow') GROUP BY la.key"
  )) {
    tenantNets.set(r.k, r.net);
  }

  const payoutHolds = new Map<number, number>();
  for (const r of q<{ tenant_id: number; s: number }>(
    db,
    "SELECT tenant_id, COALESCE(SUM(amount_paise), 0) AS s FROM payouts WHERE status IN ('held','in_flight','unknown') GROUP BY tenant_id"
  )) {
    payoutHolds.set(r.tenant_id, r.s);
  }

  for (const tb of q<{ tenant_id: number; available_paise: number; held_paise: number }>(
    db,
    "SELECT tenant_id, available_paise, held_paise FROM tenant_balances"
  )) {
    const avail = tenantNets.get("tenant_operating:" + tb.tenant_id) ?? 0;
    const held = tenantNets.get("tenant_holds:" + tb.tenant_id) ?? 0;
    const activeHolds = payoutHolds.get(tb.tenant_id) ?? 0;
    if (tb.available_paise !== avail) {
      problems.push("tenant " + tb.tenant_id + " available_paise " + tb.available_paise + " != ledger " + avail);
    }
    if (tb.held_paise !== held) {
      problems.push("tenant " + tb.tenant_id + " held_paise " + tb.held_paise + " != ledger tenant_holds " + held);
    }
    if (tb.held_paise !== activeHolds) {
      problems.push("tenant " + tb.tenant_id + " held_paise " + tb.held_paise + " != active payout holds " + activeHolds);
    }
  }

  const accountNets = new Map<string, number>();
  for (const r of q<{ k: string; net: number }>(
    db,
    "SELECT la.key AS k, SUM(CASE le.direction WHEN 'debit' THEN le.amount_paise ELSE -le.amount_paise END) AS net " +
      "FROM ledger_entries le JOIN ledger_accounts la ON la.id = le.ledger_account_id " +
      "WHERE la.kind = 'bank_mirror' GROUP BY la.key"
  )) {
    accountNets.set(r.k, r.net);
  }
  for (const ab of q<{ account_id: number; available_paise: number }>(
    db,
    "SELECT account_id, available_paise FROM account_balances"
  )) {
    const avail = accountNets.get("bank_mirror:" + ab.account_id) ?? 0;
    if (ab.available_paise !== avail) {
      problems.push("account " + ab.account_id + " available_paise " + ab.available_paise + " != ledger " + avail);
    }
  }

  let escrowNetPaise = 0;
  for (const r of tenantNets) {
    if (r[0].startsWith("escrow:")) escrowNetPaise += r[1];
  }

  return { ok: problems.length === 0, problems, groups: groups.length, escrowNetPaise };
}
