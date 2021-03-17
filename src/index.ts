import * as pg from 'pg';

export interface Connection extends pg.Client {
  begin(): Promise<void>;
  rollback(): Promise<void>;
  commit(): Promise<void>;
  inTransaction: boolean;
  transact<T>(cb: (con: Connection) => Promise<T>): Promise<T>;
  sql<T = any>(string: TemplateStringsArray, ...parts: any[]): Promise<pg.QueryResult & { rows: T[] }>;
  lit(sql: string): SQL;
  /**
   * Execute the given callback immediately after the current transaction
   * commits. If there is no current transaction, the callback will be run
   * immediately.
   * */
  onCommit(run: () => void|Promise<void>): Promise<void>;
  /**
   * Execute the given callback immediately after the current transaction
   * is rolled back. If there is no current transaction, the callback will
   * be discarded.
   */
  onRollback(run: () => void|Promise<void>): Promise<void>;
}

export function enhance(client: pg.Client): Connection {
  if ('ts-pg-dao' in client) return client as Connection;
  const res = client as any;
  res['ts-pg-dao'] = true;

  res.inTransaction = false;

  res.begin = begin;
  res.rollback = rollback;
  res.commit = commit;
  res.transact = transact;
  res.sql = sql;
  res.lit = lit;
  res.onCommit = onCommit;
  res.onRollback = onRollback;

  return res as Connection;
}

async function begin(this: Connection) {
  if (this.inTransaction) return;
  await this.query('begin');
  this.inTransaction = true;
}

async function rollback(this: Connection) {
  if (!this.inTransaction) throw new Error(`Can't rollback when not in transaction`);
  await this.query('rollback');
  const t = this as any;
  // process rollback callbacks
  if (Array.isArray(t.__rruns)) {
    for (const r of t.__rruns) try { await r(); } catch {}
    t.__cruns = [];
  }
  // discard commit callbacks
  if (Array.isArray(t.__cruns)) t.__cruns = [];
  this.inTransaction = false;
}

async function commit(this: Connection) {
  if (!this.inTransaction) throw new Error(`Can't commit when not in transaction`);
  await this.query('commit');
  const t = this as any;
  // process commit callbacks
  if (Array.isArray(t.__cruns)) {
    for (const r of t.__cruns) try { await r(); } catch {}
    t.__cruns = [];
  }
  // discard rollback callbacks
  if (Array.isArray(t.__rruns)) t.__rruns = [];
  this.inTransaction = false;
}

async function transact<T>(this: Connection, cb: (con: Connection) => Promise<T>) {
  const init = !this.inTransaction;
  if (init) await this.begin();
  try {
    const res = await cb(this);
    if (init) await this.commit();
    return res;
  } catch (ex) {
    if (init) await this.rollback();
    throw ex;
  }
}

async function sql(this: Connection, strings: TemplateStringsArray, ...parts: any[]): Promise<pg.QueryResult> {
  let sql = strings[0];
  const params: any[] = [];
  for (let i = 1; i < strings.length; i++) {
    if (parts[i - 1] instanceof SQL) sql += parts[i - 1].sql;
    else {
      params.push(parts[i - 1]);
      sql += `$${params.length}`;
    }
    sql += strings[i];
  }

  return this.query(sql, params);
}

async function onCommit(this: Connection, run: () => void|Promise<void>): Promise<void> {
  if (!this.inTransaction) return run();
  else {
    const t = this as any;
    (t.__cruns || (t.__cruns = [])).push(run);
  }
}

async function onRollback(this: Connection, run: () => void|Promise<void>): Promise<void> {
  if (!this.inTransaction) return;
  else {
    const t = this as any;
    (t.__rruns || (t.__rruns = [])).push(run);
  }
}

export class SQL {
  sql: string;
  constructor(sql: string) {
    this.sql = sql;
  }
}
function lit(sql: string): SQL {
  return new SQL(sql);
}

export interface Cache {
  [key: string]: any;
}
