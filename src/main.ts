import * as pg from 'pg';

export interface Config {
  output: Output;
  models: Model[];
  index?: boolean;
}

export interface BuildConfig extends Config {
  pgconfig: pg.ClientConfig;
}

export interface MergedOutput {
  path: string;
}
export interface SplitOutput {
  client: string;
  server: string;
}
export type Output = MergedOutput | SplitOutput;

export interface ModelOpts {
  name?: string;
}

export type Hook = (any) => void;
export interface Hooks {
  beforesave?: Hook
}

export class Model {
  constructor(table: string, fields: Column[], opts: ModelOpts = {}) {
    this.name = opts.name || table;
    this.table = table;
    this.fields = fields;
  }

  /** The name of the model */
  name: string;
  /** The name of the table */
  table: string;
  /** The relevant fields to use in the model */
  fields: Column[];

  hooks: Hooks = {};

  get pkeys(): Column[] {
    return this.fields.filter(f => f.pkey);
  }

  get cols(): Column[] {
    return this.fields.filter(f => !f.exclude);
  }

  /** Pre-configured queries */
  queries: Query[] = [];

  field(name: string): Field {
    const field = this.fields.find(f => f.name === name);
    if (!field) throw new Error(`Table ${this.table} has no column ${name}.`);
    return new Field(field);
  }

  query(options: QueryOptions): Query {
    const q = new Query(this, options);
    if (this.queries.find(q => q.name === options.name)) throw new Error(`Duplicate query ${options.name} in model ${this.name}.`);
    this.queries.push(q);
    return q;
  }

  select(alias: string = ''): string {
    return this.cols.map(c => {
      if (alias) return `"${alias}"."${c.name}" "${alias}__${c.name}"`;
      else return `"${c.name}"`;
    }).join(', ');
  }

  hook(name: keyof Hooks, fn: Hook) {
    this.hooks[name] = fn;
  }

  static from(table: Table, opts: ModelOpts = {}) {
    return new Model(table.name, table.columns, opts);
  }
}

export class Field {
  private field: Column;
  constructor(column: Column) {
    this.field = column;
  }

  alias(name: string): Field {
    this.field.alias = name;
    return this;
  }

  cast(type: string): Field {
    this.field.cast = type;
    return this;
  }

  default(value: string): Field {
    this.field.default = value;
    return this;
  }

  elide(b: boolean = true): Field {
    this.field.elide = b;
    return this;
  }

  exclude(b: boolean = true): Field {
    this.field.exclude = b;
    return this;
  }

  concurrent(b: boolean = true): Field {
    this.field.optlock = b;
    return this;
  }

  pkey(b: boolean = true): Field {
    this.field.pkey = b;
    return this;
  }
}

export interface QueryOptions {
  name: string;
  params?: Param[];
  sql: string;
  include?: IncludeMap;
  singular?: boolean;
  result?: string;
}

export class Query {
  owner: Model;
  sql: string;
  name: string;
  params?: Param[];
  include?: IncludeMap;
  singular?: boolean;
  result?: string;

  constructor(owner: Model, options: QueryOptions) {
    this.owner = owner;
    this.sql = options.sql;
    this.name = options.name;
    this.params = options.params;
    this.include = options.include;
    this.singular = options.singular;
    this.result = options.result;
  }
}

export type ParamType = 'string' | 'number' | 'string[]' | 'number[]' | 'boolean' | string;
export class Param {
  name: string;
  type: ParamType;
  default: string;

  constructor(name: string, type: ParamType = 'string', def?: string) {
    this.name = name;
    this.type = type;
    this.default = def;
  }
}
export function param(name: string, type: ParamType = 'string', def?: string) {
  return new Param(name, type, def);
}

export interface IncludeExtraDef {
  type: string;
  name?: string;
}
export interface IncludeMapDef {
  // TODO: when object literal checking on union types can handle dropping the string[], do so
  [key: string]: 1 | IncludeExtraDef | IncludeMap | Array<IncludeMap> | string[];
}
export type IncludeMap = IncludeMapDef & { '*'?: string[] };

export function config(config: pg.ClientConfig, fn: (builder: Builder) => Promise<Config>): Promise<BuildConfig> {
  const builder = new PrivateBuilder(config);

  return (async () => {
    try {
      const res = await fn(builder) as BuildConfig;
      res.pgconfig = config;
      return res;
    } finally {
      await builder.end();
    }
  })();
}

export interface Table {
  /** The name of the table */
  name: string;

  /** The columns in the table */
  columns: Column[];
}

export interface Column {
  name: string;
  nullable: boolean;
  elide?: boolean;
  elidable?: boolean;
  exclude?: boolean;
  pgdefault?: string;
  default?: string;
  alias?: string;
  pkey: boolean;
  pgtype: string;
  type: TSType;
  array?: boolean;
  cast?: string;
  json?: true;
  optlock?: boolean;
}

export type TSType = 'Date' | 'number' | 'string' | 'any' | 'boolean' | 'Date[]' | 'number[]' | 'string[]' | 'any[]' | 'boolean[]' | 'any' | 'any[]';
export const Types: { [key: string]: TSType } = {
  int2: 'number',
  int4: 'number',
  int8: 'string',
  float4: 'number',
  float8: 'string',
  bool: 'boolean',
  date: 'Date',
  timestamp: 'Date',
  timestamptz: 'Date',
  json: 'any',
  jsonb: 'any',
  varchar: 'string',
  char: 'string',
  bpchar: 'string',
  text: 'string',
  bit: 'boolean',
  numeric: 'string',
  _int2: 'number[]',
  _int4: 'number[]',
  _int8: 'string[]',
  _float4: 'number[]',
  _float8: 'string[]',
  _bool: 'boolean[]',
  _date: 'Date[]',
  _timestamp: 'Date[]',
  _timestamptz: 'Date[]',
  _json: 'any[]',
  _jsonb: 'any[]',
  _varchar: 'string[]',
  _char: 'string[]',
  _bpchar: 'string[]',
  _text: 'string[]',
  _bit: 'boolean[]',
  _numeric: 'string[]',
}

export class Builder {
  protected _pool: pg.Pool;
  protected _config: pg.ClientConfig;

  constructor(config: pg.ClientConfig) {
    this._config = config;
  }

  private async connect(): Promise<pg.Client> {
    if (!this._pool) {
      this._pool = new pg.Pool(this._config);
    }
    return await this._pool.connect();
  }

  async table(name: string, schema: string = 'public'): Promise<Table> {
    const client = await this.connect();
    try {
      const columns: Column[] = (await client.query(`select a.attname as name, not a.attnotnull as nullable,
        (select conkey from pg_catalog.pg_constraint where conrelid = a.attrelid and contype = $1) @> ARRAY[a.attnum] as pkey,
        (select t.typname from pg_catalog.pg_type t where t.oid = a.atttypid) as "type", d.adsrc as default
        from pg_catalog.pg_attribute a join pg_catalog.pg_class c on c.oid = a.attrelid
        left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        left join pg_catalog.pg_attrdef d on (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        where c.relname = $3 and a.attnum >= 0
        and (n.nspname = $2)
        and a.attisdropped = false;`, ['p', schema, name])).rows.map(r => {
          const col: Column = {
            name: r.name, nullable: r.nullable, pkey: r.pkey, pgtype: r.type, type: r.type[0] === '_' ? 'any[]' : 'any', elidable: false
          }
          if (~col.type.toLowerCase().indexOf('json')) col.json = true;
          if (col.type[0] === '_') col.array = true;
          if (r.default != null) col.pgdefault = r.default;
          if (col.nullable || col.pgdefault) col.elidable = true;
          col.type = Types[col.pgtype] || col.type;

          if (col.pgdefault != null) {
            switch (col.type) {
              case 'string':
                const str = /^E?'((?:\\'|[^'])*)'/.exec(col.pgdefault);
                if (str) col.default = `'${str[1]}'`;
                else {
                  const num = /^\(?([-0-9\.]+)\)?/.exec(col.pgdefault);
                  if (num) col.default = `'${num[1]}'`;
                }
                break;
              
              case 'Date':
                if (!col.pgdefault.indexOf('now()::')) col.default = `new Date()`;
                break;

              case 'number':
                const num = /^\(?([-0-9\.]+)\)?/.exec(col.pgdefault);
                if (num) col.default = num[1];
                break;

              case 'any':
                const obj = /^'([^']+)'::json/.exec(col.pgdefault);
                if (obj) col.default = obj[1];
                break;

              case 'boolean':
                const bool = /^(true|false)$/.exec(col.pgdefault);
                if (bool) col.default = bool[1];
                break;
            }
          }

          return col;
        });

        if (!columns.find(c => c.pkey)) throw new Error(`Cannot create model for ${schema}.${name} with no primary key`);

        return { name, columns };
    } finally {
      await client.release();
    }
  }
}

class PrivateBuilder extends Builder {
  constructor(cfg: pg.ClientConfig) {
    super(cfg);
  }

  end(): Promise<void> {
    if (this._pool) return this._pool.end();
  }
}