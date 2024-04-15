import * as pg from 'pg';

const cfgprop = 'ts-pg-dao';

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
  rollback(savepoint?: SavePoint, err?: Error): Promise<void>;
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
  sql<T = any>(string: TemplateStringsArray, ...parts: any[]): Promise<pg.QueryResult<T>>;
  /**
   * Wrap a string such that it can be included directly in a tagged `sql` template
   * without being turned into a parameter.
   */
  lit(sql: string): SQL;
  /**
   * Execute the given callback immediately before the current transaction
   * commits. If there is no current transaction, the callback will be run
   * immediately. If the callback throws, it will abort the committing transaction.
   * */
  onBeforeCommit<T extends void|Promise<void>>(run: TransactionCallback<T>): T;
  /**
   * Execute the given callback immediately after the current transaction
   * commits. If there is no current transaction, the callback will be run
   * immediately.
   * */
  onCommit(run: ConnectionCallback): void;
  /**
   * Execute the given callback immediately after the current transaction
   * is rolled back. If there is no current transaction, the callback will
   * be discarded.
   */
  onRollback(run: RollbackCallback): void;

  /**
   * Execute the given callback immediately after any query returns a result.
   * Result callbacks are automatically removed when a connection is released
   * back to a pool.
   */
  onResult(run: ResultCallback): HookHandle;

  /**
   * Execute the given callback immediately after the connection is done. For
   * pooled connections, this is after release, and it is after end for
   * non-pooled connections.
   * End callbacks are automatically removed when a connection is released
   * back to a pool.
   */
  onEnd(run: EndCallback): HookHandle;

  /**
   * Enhanced connection settings and information
   */
  readonly [cfgprop]: EnhanceInfo;

  /**
   * On-demand data storage for a transaction.
   */
  readonly txData?: { [key: string]: any };
}

export interface TransactingConnection extends Connection {
  inTransaction: true;
  readonly txData: { [key: string]: any };
}

export interface EnhanceInfo {
  clientStack?: boolean;
  attachQueryToError?: boolean;
  attachParametersToError?: boolean;
  readonly id: number;
}

interface InternalInfo extends EnhanceInfo {
  __onbeforecommit?: TransactionCallback<void|Promise<void>>[];
  __oncommit?: ConnectionCallback[];
  __onrollback?: RollbackCallback[];
  __onend?: EndCallback[];
  __onresult?: ResultCallback[];
  __savepoint?: number;
  __txdata?: { [key: string]: any };
}

/**
 * A handle to allow cancelling a listener hook.
 */
export interface HookHandle {
  cancel(): void;
}

export type ConnectionCallback = (con: Connection, err?: Error) => void;
export type TransactionCallback<T extends void|Promise<void>> = (con: Connection) => T;
export type RollbackCallback = (con: Connection, err?: Error, savepoint?: SavePoint) => void;
export type ResultCallback = (con: Connection, query: string, params: any[], ok: boolean, result: Error|pg.QueryResult, time: number) => void;
export type EndCallback = (con: Connection, err?: Error, end?: boolean) => void;

/**
 * A savepoint genereated from a transaction.
 */
export interface SavePoint {
  point: string;
}

export { QueryResult } from 'pg';

const __onenhance: ConnectionCallback[] = [];
export function onEnhance(run: ConnectionCallback): HookHandle {
  __onenhance.push(run);
  return {
    cancel() {
      const idx = __onenhance.indexOf(run);
      if (!idx) __onenhance.splice(idx, 1);
    }
  };
}

const __onend: EndCallback[] = [];
export function onEnd(run: EndCallback): HookHandle {
  __onend.push(run);
  return {
    cancel() {
      const idx = __onend.indexOf(run);
      if (!idx) __onend.splice(idx, 1);
    }
  };
}

const __onresult: ResultCallback[] = [];
export function onResult(run: ResultCallback): HookHandle {
  __onresult.push(run);
  return {
    cancel() {
      const idx = __onresult.indexOf(run);
      if (!idx) __onresult.splice(idx, 1);
    }
  };
}

let count = 0;

/**
 * Enhances the given pg.Client with a few convenience methods for managing
 * transactions and a tagged template helper for executing a query with
 * interpolations as parameters.
 */
export function enhance(client: pg.Client|pg.ClientConfig): Connection {
  if (cfgprop in client) return client as Connection;
  if (!('connect' in client)) client = new pg.Client(client);
  const db = client as any;
  const config: InternalInfo = db[cfgprop] = { id: count++, attachQueryToError: true, clientStack: true };

  db.inTransaction = false;

  db.begin = begin;
  db.savepoint = savepoint;
  db.rollback = rollback;
  db.commit = commit;
  db.transact = transact;
  db.sql = sql;
  db.lit = lit;
  db.onBeforeCommit = onBeforeCommit;
  db.onCommit = onCommit;
  db.onRollback = onRollback;
  db.onResult = _onResult;
  db.onEnd = _onEnd;

  Object.defineProperty(db, 'txData', {
    get() {
      if (!db.__txdata && db.inTransaction) db.__txdata = {};
      return db.__txdata;
    }
  });

  const query = db.query;
  db.query = function(...args: any[]):any {
    let lastq: string;
    let lastp: any[];
    if (typeof args[0] === 'string') {
      lastq = args[0];
      if (Array.isArray(args[1])) lastp = args[1];
    } else if (typeof args[0] === 'object' && args[0] && typeof args[0].text === 'string') {
      lastq = args[0].text;
      if (Array.isArray(args[0].values)) lastp = args[0].values;
    }

    const stack = config.clientStack ? new Error() : null;
    const start = Date.now();

    const cb = typeof args[args.length - 1] === 'function' ? args.pop() : null;

    const res = new Promise((ok, fail) => {
      query.apply(db, args.concat([(err: any, res: any) => {
        if (err && lastq && typeof err === 'object') {
          if (config.attachQueryToError) err.query = lastq;
          if (config.attachParametersToError) err.parameters = lastq;
        }
        if (err && stack && typeof err === 'object') err.stack = `${stack.stack}\n--- Driver exception:\n${err.stack}`
        if (err) {
          if (typeof err === 'object' && (config.__onresult || __onresult.length)) setTimeout(fireResult, 0, db, lastq || 'unknown', lastp || [], false, err, Date.now() - start);
          fail(err);
        } else {
          if (config.__onresult || __onresult.length) setTimeout(fireResult, 0, db, lastq || 'unknown', lastp || [], true, res, Date.now() - start);
          ok(res);
        }
        if (cb) cb(err, res);
      }]));
    });

    if (!cb) return res;
  }

  const end = db.end;
  if (end) {
    db.end = function(cb?: any) {
      const stack = config.clientStack ? new Error() : null;
      const res = new Promise<void>((ok, fail) => {
        end.call(db, (err: Error) => {
          if (err && stack && typeof err === 'object') err.stack = `${stack.stack}\n--- Driver exception:\n${err.stack}`

          if (err) fail(err);
          else ok();

          if (cb) cb(err);

          if (config.__onend) for (const cb of config.__onend) try { cb(db, err, true); } catch {}
          if (__onend.length) for (const cb of __onend) try { cb(db, err, true); } catch {}
          config.__onend = config.__onresult = config.__oncommit = config.__onrollback = undefined;
        });
      });
      if (!cb) return res;
    }
  }

  const release = db.release;
  if (release) {
    db.release = function(err?: Error|boolean) {
      release.call(db, err);
      if (config.__onend || __onend.length) {
        const stack = config.clientStack ? new Error() : null;
        if (err && stack && typeof err === 'object' && 'stack' in err) stack.stack = `${err.stack}\n--- Release called from:\n${stack.stack}`;
        if (config.__onend) for (const cb of config.__onend) try { cb(db, typeof err === 'boolean' ? (err ? stack : undefined) : err); } catch {}
        for (const cb of __onend) try { cb(db); } catch {}
        config.__onend = config.__onresult = config.__oncommit = config.__onrollback = undefined;
      }
    }
  }

  if (__onenhance) for (const cb of __onenhance) try { cb(db); } catch {}

  return db as Connection;
}

async function begin(this: Connection) {
  if (this.inTransaction) return;
  await this.query('begin');
  this.inTransaction = true;
  (this[cfgprop] as InternalInfo).__savepoint = 0;
}

async function savepoint(this: Connection): Promise<SavePoint> {
  if (!this.inTransaction) throw new Error(`Can't set a savepoint when not in transaction`);
  const point = `step${(this[cfgprop] as InternalInfo).__savepoint++}`;
  await this.query(`savepoint ${point}`);
  return { point };
}

async function rollback(this: Connection, point?: SavePoint, err?: Error) {
  if (!this.inTransaction) throw new Error(`Can't rollback when not in transaction`);
  const t = this[cfgprop] as InternalInfo;

  if (point) {
    try {
      await this.query(`rollback to ${point.point}`);
      // process rollback callbacks
      if (Array.isArray(t.__onrollback)) for (const r of t.__onrollback) try { r(this, err, point); } catch {}
      return;
    } catch (e) {
      await this.rollback(undefined, e);
    }
  }

  await this.query('rollback');
  // process rollback callbacks
  if (Array.isArray(t.__onrollback)) for (const r of t.__onrollback) try { r(this, err); } catch {}
  // discard callbacks
  t.__txdata = t.__onbeforecommit = t.__oncommit = t.__onrollback = undefined;
  this.inTransaction = false;
}

async function commit(this: Connection) {
  if (!this.inTransaction) throw new Error(`Can't commit when not in transaction`);
  const t = this[cfgprop] as InternalInfo;
  if (Array.isArray(t.__onbeforecommit)) for (const r of t.__onbeforecommit) await r(this);
  await this.query('commit');
  // process commit callbacks
  if (Array.isArray(t.__oncommit)) for (const r of t.__oncommit) try { r(this); } catch {}
  // discard callbacks
  t.__txdata = t.__onbeforecommit = t.__oncommit = t.__onrollback = undefined;
  this.inTransaction = false;
}

async function transact<T>(this: Connection, cb: (con: TransactingConnection) => Promise<T>) {
  const init = !this.inTransaction;
  if (init) {
    await this.begin();
    try {
      const res = await cb(this as any);
      await this.commit();
      return res;
    } catch (e) {
      await this.rollback(undefined, e);
      throw e;
    }
  } else {
    return cb(this as any);
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

function onBeforeCommit<T extends void|Promise<void>>(this: Connection, run: TransactionCallback<T>): T {
  if (!this.inTransaction) return run(this);
  else {
    const t = this[cfgprop] as InternalInfo;
    (t.__onbeforecommit || (t.__onbeforecommit = [])).push(run);
  }
}

function onCommit(this: Connection, run: ConnectionCallback): void {
  if (!this.inTransaction) return run(this);
  else {
    const t = this[cfgprop] as InternalInfo;
    (t.__oncommit || (t.__oncommit = [])).push(run);
  }
}

function onRollback(this: Connection, run: ConnectionCallback): void {
  if (!this.inTransaction) return;
  else {
    const t = this[cfgprop] as InternalInfo;
    (t.__onrollback || (t.__onrollback = [])).push(run);
  }
}

function _onResult(this: Connection, run: ResultCallback): HookHandle {
  const t = this[cfgprop] as InternalInfo;
  (t.__onresult || (t.__onresult = [])).push(run);
  return {
    cancel() {
      if (!t.__onresult) return;
      const idx = t.__onresult.indexOf(run);
      if (!idx) t.__onresult.splice(idx, 1);
      if (t.__onresult.length === 0) t.__onresult = undefined;
    }
  }
}

function fireResult(con: Connection, query: string, params: any[], ok: boolean, result: Error|pg.QueryResult, time?: number) {
  const t = con[cfgprop] as InternalInfo;
  if (t && t.__onresult) for (const c of t.__onresult) try { c(con, query, params, ok, result, time); } catch {}
  if (__onresult.length) for (const c of __onresult) try { c(con, query, params, ok, result, time); } catch {}
}

function _onEnd(this: Connection, run: EndCallback): HookHandle {
  const t = this[cfgprop] as InternalInfo;
  (t.__onend || (t.__onend = [])).push(run);
  return {
    cancel() {
      if (!t.__onend) return;
      const idx = t.__onend.indexOf(run);
      if (!idx) t.__onend.splice(idx, 1);
      if (t.__onend.length === 0) t.__onend = undefined;
    }
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

export interface Model<T> {
  new (props?: Partial<T>): T;
  readonly table: string;
  readonly keys: string[];
  onBeforeSave(hook: (model: T, con: Connection) => (Promise<void>|void)): void;
  onBeforeDelete(hook: (model: T, con: Connection) => (Promise<void>|void)): void;
  save(con: Connection, model: T): Promise<T>;
  delete(con: Connection, model: T): Promise<void>;
  findCurrent(con: Connection, model: T): Promise<T>;
  findOne(con: Connection, where?: string, params?: any[], optional?: boolean): Promise<T>;
  findAll(con: Connection, where?: string, params?: any[]): Promise<T[]>;
  truncateStrings(model: T): void;
  stripDates(model: T): void;
}
