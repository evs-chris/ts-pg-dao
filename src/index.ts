import * as pg from 'pg';

export interface Connection extends pg.Client {
  begin(): Promise<void>;
  rollback(): Promise<void>;
  commit(): Promise<void>;
  inTransaction: boolean;
  transact<T>(cb: (con: Connection) => Promise<T>): Promise<T>;
  sql<T = any>(string: TemplateStringsArray, ...parts: any[]): Promise<pg.QueryResult & { rows: T[] }>;
  lit(sql: string): SQL;
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
  this.inTransaction = false;
}

async function commit(this: Connection) {
  if (!this.inTransaction) throw new Error(`Can't commit when not in transaction`);
  await this.query('commit');
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
