import * as pg from 'pg';

/**
 * A thin wrapper on top of pg.Client that provides basic transaction support, 
 * and a convenient way to immediately execute a query with a template string.
 */
export interface Connection extends pg.Client {
  /** 
   * Start a transaction for this connection. If there is already an active
   * transaction for this connection, this is a noop.
   * */
  begin(): Promise<void>;
  /** Set a savepoint that can be rolled back to any number of times without
   * fully aborting the transaction.
   */
  savepoint(): Promise<SavePoint>;
  /** Roll back the current transaction for this connection. If a savepoint
   * is given and can be rolled back to, the surrounding transaction will 
   * not be aborted. If the savepoint fails or is not supplied, the whole
   * transaction will be aborted.
   * */
  rollback(savepoint?: SavePoint): Promise<void>;
  /** Complete the current transaction for this connection. */
  commit(): Promise<void>;
  /** Determine whether there is an active transaction for this connection. */
  inTransaction: boolean;
  /** Execute the given block in a transaction. */
  transact<T>(cb: (con: Connection) => Promise<T>): Promise<T>;
  /**
   * Execute the given template string with its interpolated values as
   * parameters. If you need to do literal text interpolation inside the query
   * e.g. add a part to a where clause, you can use the `lit` function of the
   * connection, which will cause the query to be changed rather than adding
   * a parameter.
   *
   * @returns a pg.QueryResult with any resulting rows attached
   */
  sql<T = any>(string: TemplateStringsArray, ...parts: any[]): Promise<pg.QueryResult & { rows: T[] }>;
  /**
   * Wrap a string such that it can be included directly in a tagged `sql` template
   * without being turned into a parameter.
   */
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

/**
 * A savepoint genereated from a transaction.
 */
export interface SavePoint {
  point: string;
}

export { QueryResult } from 'pg';

/**
 * Enhances the given pg.Client with a few convenience methods for managing
 * transactions and a tagged template helper for executing a query with
 * interpolations as parameters.
 */
export function enhance(client: pg.Client|pg.ClientConfig): Connection {
  if ('ts-pg-dao' in client) return client as Connection;
  if (!('connect' in client)) client = new pg.Client(client);
  const res = client as any;
  res['ts-pg-dao'] = true;

  res.inTransaction = false;

  res.begin = begin;
  res.savepoint = savepoint;
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
  (this as any).__savepoint = 0;
}

async function savepoint(this: Connection): Promise<SavePoint> {
  if (!this.inTransaction) throw new Error(`Can't set a savepoint when not in transaction`);
  const point = `step${(this as any).__savepoint++}`;
  await this.query(`savepoint ${point}`);
  return { point };
}

async function rollback(this: Connection, point?: SavePoint) {
  if (!this.inTransaction) throw new Error(`Can't rollback when not in transaction`);

  if (point) {
    try {
      await this.query(`rollback to ${point.point}`);
      return;
    } catch {}
  }
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
