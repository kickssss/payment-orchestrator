import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

/** bun:sqlite stands in for MySQL: an ACID, single source of truth.
 *  BEGIN IMMEDIATE takes the DB-wide write lock up front — the plan's FOR UPDATE
 *  serialization analog (SQLite has exactly one writer at a time, so check-then-act
 *  inside a write transaction is atomic against every other writer).
 *  All money-safety comes from conditional UPDATEs, UNIQUE backstops and CHECK
 *  constraints, which SQLite enforces identically (verified empirically on Bun 1.4.4). */

export type Db = Database;
export type Param = string | number | bigint | null;
export type Row = Record<string, unknown>;

export const TENANT_ID = 1;
export const ACC = { primary: 1, secondary: 2 } as const;
/** Capacity demo — the plan's per-window limits are cut for the POC; this is the
 *  lock-then-check-then-act stand-in inside the create transaction. */
export const MAX_OPEN_ORDERS_PER_ACCOUNT = 3;

const DDL = `
CREATE TABLE IF NOT EXISTS tenants (
  id INTEGER PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  timezone TEXT NOT NULL DEFAULT 'Asia/Kolkata'
);

CREATE TABLE IF NOT EXISTS bank_accounts (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES tenants(id),
  account_number TEXT NOT NULL,
  ifsc TEXT NOT NULL,
  holder_name TEXT NOT NULL,
  supports_payin INTEGER NOT NULL DEFAULT 1,
  supports_payout INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  priority INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS payin_orders (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES tenants(id),
  account_id INTEGER NOT NULL REFERENCES bank_accounts(id),
  client_order_id TEXT NOT NULL,
  amount_paise INTEGER NOT NULL CHECK (amount_paise > 0),
  status TEXT NOT NULL CHECK (status IN ('pending','claimed','succeeded','mismatch_review','expired','cancelled','reversed')),
  claimed_utr TEXT,
  claimed_at INTEGER,
  expires_at INTEGER NOT NULL,
  utr_deadline INTEGER,
  matched_bank_txn_id INTEGER UNIQUE,
  active_utr TEXT GENERATED ALWAYS AS (CASE WHEN status IN ('pending','claimed') THEN claimed_utr END)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_client ON payin_orders(tenant_id, client_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_active_utr ON payin_orders(account_id, active_utr);
CREATE INDEX IF NOT EXISTS ix_orders_expiry ON payin_orders(status, expires_at);
CREATE INDEX IF NOT EXISTS ix_orders_utrdeadline ON payin_orders(status, utr_deadline);
CREATE INDEX IF NOT EXISTS ix_orders_acct_utr ON payin_orders(account_id, claimed_utr);

CREATE TABLE IF NOT EXISTS payouts (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES tenants(id),
  account_id INTEGER REFERENCES bank_accounts(id),
  idempotency_key TEXT NOT NULL,
  amount_paise INTEGER NOT NULL CHECK (amount_paise > 0),
  beneficiary TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('created','held','in_flight','settled','failed','unknown','rejected','cancelled')),
  next_check_at INTEGER NOT NULL,
  bank_ref TEXT,
  bank_error TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_payouts_idem ON payouts(tenant_id, idempotency_key);
CREATE INDEX IF NOT EXISTS ix_payouts_due ON payouts(status, next_check_at);

CREATE TABLE IF NOT EXISTS bank_txns (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES tenants(id),
  account_id INTEGER NOT NULL REFERENCES bank_accounts(id),
  bank_txn_id TEXT NOT NULL,
  utr TEXT,
  direction TEXT NOT NULL CHECK (direction IN ('credit','debit')),
  amount_paise INTEGER NOT NULL CHECK (amount_paise > 0),
  booked_at INTEGER NOT NULL,
  ingested_at INTEGER NOT NULL,
  match_status TEXT NOT NULL DEFAULT 'unmatched' CHECK (match_status IN ('unmatched','matched_payin','matched_payout','matched_reversal','unknown_credit','unknown_debit')),
  ref TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_banktxn ON bank_txns(account_id, bank_txn_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_txn_dedupe ON bank_txns(account_id, utr, direction, amount_paise, booked_at);
CREATE INDEX IF NOT EXISTS ix_txns_acct_utr ON bank_txns(account_id, utr);
CREATE INDEX IF NOT EXISTS ix_txns_match ON bank_txns(match_status, direction, ingested_at);

CREATE TABLE IF NOT EXISTS ledger_txns (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES tenants(id),
  kind TEXT NOT NULL,
  ref_type TEXT NOT NULL,
  ref_id INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ledgertxns_ref ON ledger_txns(ref_type, ref_id);

CREATE TABLE IF NOT EXISTS ledger_accounts (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('tenant_operating','tenant_holds','bank_mirror','escrow')),
  key TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_ledgeraccounts ON ledger_accounts(kind, key);

CREATE TABLE IF NOT EXISTS ledger_entries (
  id INTEGER PRIMARY KEY,
  ledger_txn_id INTEGER NOT NULL REFERENCES ledger_txns(id),
  ledger_account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
  direction TEXT NOT NULL CHECK (direction IN ('debit','credit')),
  amount_paise INTEGER NOT NULL CHECK (amount_paise > 0)
);
CREATE INDEX IF NOT EXISTS ix_entries_txn ON ledger_entries(ledger_txn_id);

CREATE TABLE IF NOT EXISTS tenant_balances (
  tenant_id INTEGER PRIMARY KEY REFERENCES tenants(id),
  available_paise INTEGER NOT NULL DEFAULT 0,
  held_paise INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS account_balances (
  account_id INTEGER PRIMARY KEY REFERENCES bank_accounts(id),
  available_paise INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bank_gates (
  scope TEXT NOT NULL CHECK (scope IN ('account','connection')),
  entity_id INTEGER NOT NULL,
  holder_op TEXT CHECK (holder_op IN ('statement','payout')),
  holder_id TEXT,
  acquired_at INTEGER,
  expires_at INTEGER,
  PRIMARY KEY (scope, entity_id)
);

CREATE TABLE IF NOT EXISTS outbox (
  event_id TEXT PRIMARY KEY,
  tenant_id INTEGER,
  aggregate_type TEXT NOT NULL,
  aggregate_id INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  occurred_at INTEGER NOT NULL,
  claimed_by TEXT,
  claimed_at INTEGER,
  dispatched_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox(dispatched_at, occurred_at);

CREATE TABLE IF NOT EXISTS reviews (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL REFERENCES tenants(id),
  kind TEXT NOT NULL CHECK (kind IN ('utr_mismatch','utr_reuse','no_txn_found','late_claim','unknown_credit','unknown_debit','payout_ambiguous','reversal_overdraw')),
  ref_type TEXT NOT NULL,
  ref_id INTEGER NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','resolved','rejected')),
  suggested_ref_id INTEGER,
  resolved_action TEXT,
  created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_open_review ON reviews(kind, ref_type, ref_id) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS ix_reviews_open ON reviews(status, created_at);

CREATE TABLE IF NOT EXISTS sync_state (
  account_id INTEGER PRIMARY KEY REFERENCES bank_accounts(id),
  last_cursor INTEGER NOT NULL DEFAULT 0,
  last_fetch_started INTEGER
);
`;

export function q<T = Row>(db: Db, sql: string, ...params: Param[]): T[] {
  return db.query(sql).all(...params) as T[];
}

export function one<T = Row>(db: Db, sql: string, ...params: Param[]): T | undefined {
  return db.query(sql).get(...params) as T | undefined;
}

export function run(db: Db, sql: string, ...params: Param[]): number {
  return db.run(sql, ...params).changes;
}

export function insert(db: Db, sql: string, ...params: Param[]): number {
  return Number(db.run(sql, ...params).lastInsertRowid);
}

/** The one transaction helper. BEGIN IMMEDIATE = take the write lock up front;
 *  every effect (guard + ledger + aggregates + outbox) happens inside.
 *  Synchronous by construction (bun:sqlite is sync) — never spans an await.
 *  Never nested. */
export function withTxn<T>(db: Db, fn: () => T): T {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.exec("COMMIT");
    return result;
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

/** Classify SQLite constraint errors by code (see tests: unique-violation has
 *  e.code SQLITE_CONSTRAINT_UNIQUE and a message naming table.columns). */
export function isUniqueViolation(e: unknown, ...column: string[]): boolean {
  if (!(e instanceof Error)) return false;
  const err = e as Error & { code?: string };
  if (err.code !== "SQLITE_CONSTRAINT_UNIQUE") return false;
  return column.every((c) => err.message.includes(c));
}

/** SQLITE_BUSY classification for the loop-level skip policy: busy_timeout can
 *  still throw under write spikes — the next timer tick IS the backoff. */
export function isBusy(e: unknown): boolean {
  if (!(e instanceof Error)) return false;
  const err = e as Error & { code?: string };
  return err.code === "SQLITE_BUSY" || e.message.includes("database is locked");
}

export function createDb(path: ":memory:" | string = ":memory:"): Db {
  if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true }); // file DBs live in run/ etc.
  const db = new Database(path);
  // busy_timeout FIRST: the WAL pragma below takes a lock, and N processes
  // racing to open a fresh file would hit SQLITE_BUSY before the timeout exists
  db.exec("PRAGMA busy_timeout = 5000;");
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec(DDL);
  seed(db);
  return db;
}

/** Idempotent seed: N processes boot concurrently on one file DB — BEGIN
 *  IMMEDIATE serializes the seeds and OR IGNORE makes re-runs no-ops. */
function seed(db: Db): void {
  withTxn(db, () => {
    run(db, "INSERT OR IGNORE INTO tenants (id, slug) VALUES (?, ?)", TENANT_ID, "acme");
    run(
      db,
      "INSERT OR IGNORE INTO bank_accounts (id, tenant_id, account_number, ifsc, holder_name, priority) VALUES (?, ?, ?, ?, ?, ?)",
      ACC.primary, TENANT_ID, "50100112345611", "HDFC0000123", "Acme Pvt Ltd", 10
    );
    run(
      db,
      "INSERT OR IGNORE INTO bank_accounts (id, tenant_id, account_number, ifsc, holder_name, priority) VALUES (?, ?, ?, ?, ?, ?)",
      ACC.secondary, TENANT_ID, "50100112345622", "ICIC0000456", "Acme Pvt Ltd", 20
    );
    run(db, "INSERT OR IGNORE INTO tenant_balances (tenant_id) VALUES (?)", TENANT_ID);
    run(db, "INSERT OR IGNORE INTO account_balances (account_id) VALUES (?)", ACC.primary);
    run(db, "INSERT OR IGNORE INTO account_balances (account_id) VALUES (?)", ACC.secondary);
  });
}
