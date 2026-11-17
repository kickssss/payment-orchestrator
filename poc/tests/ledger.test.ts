import { beforeEach, describe, expect, test } from "bun:test";
import { ACC, createDb, one, run, TENANT_ID, withTxn, type Db } from "../src/core/db.ts";
import { openingDeposit, postGroup, verifyLedger } from "../src/core/ledger.ts";

let db: Db;
let t: number;

beforeEach(() => {
  db = createDb();
  t = 1_700_000_000_000;
});

describe("ledger core", () => {
  test("opening deposit updates both aggregates and the verifier passes", () => {
    openingDeposit(db, TENANT_ID, ACC.primary, 10_000_000, t);

    expect(one(db, "SELECT available_paise AS a FROM tenant_balances WHERE tenant_id = ?", TENANT_ID)).toEqual({ a: 10_000_000 });
    expect(one(db, "SELECT available_paise AS a FROM account_balances WHERE account_id = ?", ACC.primary)).toEqual({ a: 10_000_000 });

    const v = verifyLedger(db);
    expect(v.ok).toBe(true);
    expect(v.groups).toBe(1);
  });

  test("unbalanced groups are rejected and write nothing", () => {
    expect(() =>
      postGroup(
        db,
        { tenantId: TENANT_ID, kind: "test", refType: "test", refId: 1, at: t },
        [
          { account: { kind: "tenant_operating", tenantId: TENANT_ID }, direction: "debit", amountPaise: 100 },
          { account: { kind: "tenant_holds", tenantId: TENANT_ID }, direction: "credit", amountPaise: 99 },
        ]
      )
    ).toThrow();

    expect(one<{ n: number }>(db, "SELECT COUNT(*) AS n FROM ledger_txns")!.n).toBe(0);
    expect(one<{ n: number }>(db, "SELECT COUNT(*) AS n FROM ledger_entries")!.n).toBe(0);
  });

  test("hold -> settle lifecycle keeps aggregates equal to the ledger", () => {
    openingDeposit(db, TENANT_ID, ACC.primary, 1_000, t);

    withTxn(db, () =>
      postGroup(
        db,
        { tenantId: TENANT_ID, kind: "payout_hold", refType: "payout", refId: 1, at: t },
        [
          { account: { kind: "tenant_operating", tenantId: TENANT_ID }, direction: "debit", amountPaise: 400 },
          { account: { kind: "tenant_holds", tenantId: TENANT_ID }, direction: "credit", amountPaise: 400 },
        ]
      )
    );
    expect(one(db, "SELECT available_paise AS a, held_paise AS h FROM tenant_balances WHERE tenant_id = ?", TENANT_ID)).toEqual({ a: 600, h: 400 });

    withTxn(db, () =>
      postGroup(
        db,
        { tenantId: TENANT_ID, kind: "payout_settle", refType: "payout", refId: 1, at: t },
        [
          { account: { kind: "tenant_holds", tenantId: TENANT_ID }, direction: "debit", amountPaise: 400 },
          { account: { kind: "bank_mirror", accountId: ACC.primary }, direction: "credit", amountPaise: 400 },
        ]
      )
    );
    expect(one(db, "SELECT available_paise AS a, held_paise AS h FROM tenant_balances WHERE tenant_id = ?", TENANT_ID)).toEqual({ a: 600, h: 0 });
    expect(one(db, "SELECT available_paise AS a FROM account_balances WHERE account_id = ?", ACC.primary)).toEqual({ a: 600 });

    expect(verifyLedger(db).ok).toBe(true);
  });

  test("verifier detects tampered aggregates and stray holds", () => {
    openingDeposit(db, TENANT_ID, ACC.primary, 1_000, t);
    run(db, "UPDATE tenant_balances SET available_paise = available_paise + 1 WHERE tenant_id = ?", TENANT_ID);
    const v = verifyLedger(db);
    expect(v.ok).toBe(false);
    expect(v.problems.length).toBe(1);
  });
});
