import * as pg from 'pg';

export interface Connection extends pg.Client {
  begin(): Promise<void>;
  rollback(): Promise<void>;
  commit(): Promise<void>;
  inTransaction: boolean;
}

export function enhance(client: pg.Client): Connection {
  if ('ts-pg-dao' in client) return client as Connection;
  const res = client as any;
  res['ts-pg-dao'] = true;

  res.inTransaction = false;

  res.begin = async function() {
    if (this.inTransaction) return;
    await this.query('begin');
    this.inTransaction = true;
  }

  res.rollback = async function() {
    if (!this.inTransaction) throw new Error(`Can't rollback when not in transaction`);
    await this.query('rollback');
    this.inTransaction = false;
  }

  res.commit = async function() {
    if (!this.inTransaction) throw new Error(`Can't commit when not in transaction`);
    await this.query('commit');
    this.inTransaction = false;
  }

  return res as Connection;
}

export interface Cache {
  [key: string]: any;
}