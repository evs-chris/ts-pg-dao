import * as pg from 'pg';
import * as fs from 'fs-extra';
export { PatchOptions, PatchResult, patchConfig, calcDropRestore } from './patch';
import { Connection } from './index';

export interface Config {
  output: Output;
  models: Model[];
  index?: boolean|string;
  name?: string;
  stringyDates?: boolean;
  tzTimestamps?: boolean;
}

export type BuiltConfig = Config & { pgconfig?: BuilderConfig };

export interface BuildConfig {
  config: BuilderConfig;
  read(): Promise<BuiltConfig>;
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
  file?: string;
}

export type Hook = (model: any, con: Connection) => void;
export interface Hooks {
  beforesave?: Hook;
  beforedelete?: Hook;
}

export interface StatusFlags {
  load?: string|false;
  change?: string|false;
  remove?: string|false;
}

export interface OptionalExtras {
  [field: string]: string|{ type: string; comment?: string };
}

export interface CodeMap {
  bothInner?: string;
  bothOuter?: string;
  clientInner?: string;
  clientOuter?: string;
  serverInner?: string;
  serverOuter?: string;
}

export type CodeLocation = 'inner'|'outer';
export type CodeType = 'client'|'server'|'both';

export type ExportMap = {
  client?: string[];
  server?: string[];
  both?: string[]
}

export class Model {
  constructor(table: string, fields: Column[], opts: ModelOpts = {}) {
    this.name = opts.name || table;
    this.table = table;
    this.fields = fields;
    this.file = opts.file;
  }

  /** The name of the model */
  name: string;
  /** The name of the table */
  table: string;
  /** The relevant fields to use in the model */
  fields: Column[];
  /** The name of the target file
   * @default name
   */
  file?: string;

  hooks: Hooks = {};

  flags: StatusFlags = {};

  _imports: string[] = [];

  _extras: OptionalExtras = {};

  _exports: ExportMap = {};

  codeMap: CodeMap = {};

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

  select(config: Config, alias: string = ''): string {
    return this.cols.map(c => {
      if (alias) return `"${alias}"."${c.name}"${this.cast(config, c)} "${alias}__${c.name}"`;
      else return `"${c.name}"${this.cast(config, c)}`;
    }).join(', ');
  }

  cast(config: Config, col: Column): string {
    if (col.cast) return `::${col.cast}`;
    if (config.tzTimestamps && col.pgtype === 'timestamp') return `::timestamptz`;
    return '';
  }

  hook(name: keyof Hooks, fn: Hook): Model {
    this.hooks[name] = fn;
    return this;
  }

  imports(...descriptors: string[]): Model {
    this._imports.push.apply(this._imports, descriptors);
    return this;
  }

  extra(field: string, type: string): Model {
    this._extras[field] = type;
    return this;
  }

  extras(fields: OptionalExtras): Model {
    Object.assign(this._extras, fields);
    return this;
  }

  code(code: string, exports?: string[], type?: CodeType, location?: CodeLocation): Model;
  code(code: string, exports?: string[], location?: CodeLocation, type?: CodeType): Model;
  code(code: string, type?: CodeType, location?: CodeLocation, exports?: string[]): Model;
  code(code: string, type?: CodeType, exports?: string[], location?: CodeLocation): Model;
  code(code: string, location?: CodeLocation, exports?: string[], type?: CodeType): Model;
  code(code: string, location?: CodeLocation, type?: CodeType, exports?: string[]): Model;
  code(code: string, arg1?: CodeType|CodeLocation|string[], arg2?: CodeType|CodeLocation|string[], arg3?: CodeType|CodeLocation|string[]): Model {
    let type: CodeType = 'both';
    let location: CodeLocation = 'inner';
    let exports: string[];

    [arg1, arg2, arg3].forEach(arg => {
      if (Array.isArray(arg)) exports = arg;
      else if (arg === 'inner' || arg === 'outer') location = arg;
      else if (typeof arg === 'string') type = arg;
    });

    this.codeMap[`${type}${location[0].toUpperCase()}${location.substr(1)}`] = code;
    if (exports) exports.forEach(e => this.exports(e, type));
    return this;
  }

  exports(name: string, type: CodeType = 'both'): Model {
    if (!this._exports[type]) this._exports[type] = [];
    this._exports[type].push(name);
    return this;
  }

  static from(table: Table, opts: ModelOpts = {}) {
    return new Model(table.name, table.columns, opts);
  }

  static async build(builder: Builder, table: string, opts: ModelOpts = {}, schema?: string): Promise<Model> {
    const t = await builder.table(table, schema);
    return Model.from(t, opts);
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

  retype(type: string): Field {
    this.field.retype = type;
    return this;
  }

  trim(b: boolean): Field {
    this.field.trim = b;
    return this;
  }
}

export interface QueryOptions {
  /** The method name for the query */
  name: string;
  /** A list of parameters to be accepted as keys in a params object */
  params?: Param[];
  /** The statement to run */
  sql: string;
  /** The result mapping configuration for the query. Additional models and arrays of models can be included as joins, and individual fields can be appended to the various result types. */
  include?: IncludeMap;
  /** Should this query only return a single model? */
  singular?: boolean;
  /** If this query is singular, should it be allowed to return no result? */
  optional?: boolean;
  /** Export a type for the result of the query */
  result?: string;
  /** Modify the result type of the query */
  retype?: string;
  /** Should this query just return a field from the base table? */
  scalar?: string;
  extras?: OptionalExtras;
  /** Additional models that need to be imported for this query */
  imports?: string[];
  /** Conditional query parts that will be appended to the base sql based on parameters. The conditional keys are included as if statements that append the sql if they are truthy. */
  parts?: { [condition: string]: string };
}

export class Query {
  owner: Model;
  sql: string;
  name: string;
  params?: Param[];
  include?: IncludeMap;
  singular?: boolean;
  optional?: boolean;
  result?: string;
  retype?: string;
  scalar?: string;
  extras?: OptionalExtras;
  imports?: string[];
  parts?: { [condition: string]: string };

  constructor(owner: Model, options: QueryOptions) {
    this.owner = owner;
    this.sql = options.sql;
    this.name = options.name;
    this.params = options.params;
    this.include = options.include;
    this.singular = options.singular;
    this.optional = options.optional;
    this.result = options.result;
    this.retype = options.retype;
    this.scalar = options.scalar;
    this.extras = options.extras;
    this.imports = options.imports;
    this.parts = options.parts;
  }
}

export type ParamType = 'string' | 'number' | 'string[]' | 'number[]' | 'boolean' | string;
export class Param {
  name: string;
  type: ParamType;
  default?: string;
  optional: boolean = false;

  constructor(name: string, type: ParamType = 'string', def?: string, optional?: boolean) {
    this.name = name;
    this.type = type;
    this.default = def;
    if (optional !== undefined) this.optional = optional;
  }
}
export function param(name: string, type: ParamType = 'string', def?: string) {
  return new Param(name, type, def);
}
export function opt(name: string, type: ParamType = 'string', def?: string) {
  return new Param(name, type, def, true);
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

export const tableQuery = `select table_name as name, table_schema as schema from information_schema.tables where table_type = 'BASE TABLE' and table_schema not like 'pg_%' and table_schema <> 'information_schema' order by table_schema asc, table_name asc;`;
export const columnQuery = `select ts.table_schema as schema, ts.table_name as table, cs.column_name as name, cs.is_nullable = 'YES' as nullable, (select keys.constraint_name from information_schema.key_column_usage keys join information_schema.table_constraints tc on keys.constraint_name = tc.constraint_name and keys.constraint_schema = tc.constraint_schema and tc.table_name = keys.table_name where keys.table_schema = cs.table_schema and keys.table_name = cs.table_name and keys.column_name = cs.column_name and tc.constraint_type = 'PRIMARY KEY') is not null as pkey, cs.udt_name as type, cs.column_default as default, cs.character_maximum_length as length, case when cs.udt_name = 'numeric' and cs.numeric_precision > 0 and cs.numeric_scale > 0 then array[cs.numeric_precision::integer, cs.numeric_scale::integer] else null end as precision from information_schema.columns cs join information_schema.tables ts on ts.table_name = cs.table_name and ts.table_schema = cs.table_schema where ts.table_schema <> 'pg_catalog' and ts.table_schema <> 'information_schema' order by cs.column_name asc;`;
export const commentQuery =  `select shobj_description((select oid from pg_database where datname = $1), 'pg_database') as comment;`;
export const enumQuery = (type: string) => `select enum_range(null::${type})::varchar[] as values;`;
export const functionQuery = `SELECT n.nspname as "schema",
  p.proname as "name",
  pg_catalog.pg_get_function_result(p.oid) as "result",
  pg_catalog.pg_get_function_arguments(p.oid) as "args",
  pg_catalog.pg_get_functiondef(p.oid) as "def"
FROM pg_catalog.pg_proc p
  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE pg_catalog.pg_function_is_visible(p.oid)
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
ORDER BY 1, 2, 4;`;
export const indexQuery = `select schemaname as "schema", indexname as "name", tablename as "table", indexdef as def from pg_indexes where schemaname <> 'pg_catalog' and schemaname <> 'information_schema';`;
export const viewQuery = `select schemaname as "schema", viewname as "name", definition as "def", false as "materialized" from pg_views where schemaname <> 'pg_catalog' and schemaname <> 'information_schema' union all select schemaname as "schema", matviewname as "name", definition as "def", true as "materialized" from pg_matviews where schemaname <> 'pg_catalog' and schemaname <> 'information_schema';`;

export function config(config: BuilderConfig, fn: (builder: Builder) => Promise<Config>): BuildConfig {
  const builder = new PrivateBuilder(config);

  return {
    config,
    read: async () => {
      try {
        const res: BuiltConfig = await fn(builder);
        res.pgconfig = config;
        if (!res.name) res.name = config.name;
        return res;
      } finally {
        await builder.end();
      }
    }
  };
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
  retype?: string;
  array?: boolean;
  cast?: string;
  json?: true;
  optlock?: boolean;
  enum?: string[];
  trim?: boolean;
  length?: number;
  precision?: [number, number];
}

export type TSType = 'Date' | 'number' | 'string' | 'any' | 'boolean' | 'Date[]' | 'number[]' | 'string[]' | 'any[]' | 'boolean[]' | 'any' | 'any[]';
export const Types: { [key: string]: TSType } = {
  int2: 'number',
  int4: 'number',
  int8: 'string',
  float4: 'number',
  float8: 'string',
  bool: 'boolean',
  date: 'string',
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
  _date: 'string[]',
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

export type BuilderConfig = pg.ClientConfig & SchemaConfig & { name?: string; _cache?: any };
export interface SchemaConfig {
  schemaCacheFile?: string;
  schemaInclude?: string[];
  schemaExclude?: string[];
  schemaFull?: boolean;
}
export interface TableSchema {
  name: string;
  schema: string;
  columns: ColumnSchema[];
}
export interface ColumnSchema {
  name: string;
  nullable: boolean;
  pkey: boolean;
  type: string;
  default: string;
  enum?: string[];
  length?: number;
  precision?: [number, number];
  schema?: string;
  table?: string;
}
export interface FunctionSchema {
  schema: string;
  name: string;
  result: string;
  args: string;
  def: string;
}
export interface ViewSchema {
  schema: string;
  name: string;
  materialized: boolean;
  def: string;
}
export interface IndexSchema {
  schema: string;
  name: string;
  table: string;
  def: string;
}
export interface SchemaCache {
  tables: TableSchema[];
  functions?: FunctionSchema[];
  indexes?: IndexSchema[];
  views?: ViewSchema[];
}

export const BuilderOptions = {
  forceCache: false,
};

export class Builder {
  protected _pool: pg.Pool;
  protected _config: BuilderConfig;
  protected _schemaCache?: SchemaCache;

  constructor(config: BuilderConfig) {
    this._config = config;
  }

  private async connect(): Promise<pg.PoolClient> {
    if (!this._pool) {
      this._pool = new pg.Pool(Object.assign({ connectionTimeoutMillis: 2000 }, this._config));
    }
    return await this._pool.connect();
  }

  async table(name: string, schema: string = 'public'): Promise<Table> {
    let cols: ColumnSchema[];

    if (this._config.database && !BuilderOptions.forceCache) {
      if (!this._config._cache) {
        const cache: any = this._config._cache = {};
        if (!cache.cols) {
          try {
            const client = await this.connect();
            try {
              cols = cache.cols = (await client.query(columnQuery)).rows;
              for (const col of cols) {
                if (!Types[col.type]) { // check for enums
                  try {
                    col.enum = (await client.query(enumQuery(col.type))).rows[0].values;
                  } catch {}
                }
              }
            } finally {
              client.release();
            }
          } catch (e) {
            console.error(`Failed to read "${schema}.${name}" schema`);
          }
        }
      }
      cols = this._config._cache.cols.filter((c: ColumnSchema) => c.schema === schema && c.table === name).map(c => Object.assign({}, c, { table: undefined, schema: undefined }));
    }

    if (!cols && this._config.schemaCacheFile) {
      if (!this._schemaCache) {
        console.log(`Reading schema from cache ${this._config.schemaCacheFile}`);
        this._schemaCache = JSON.parse(await fs.readFile(this._config.schemaCacheFile, { encoding: 'utf8' }));
      }
      let table = this._schemaCache.tables.find(t => t.schema === schema && t.name === name);
      if (!table) throw new Error(`"${schema}.${name}" not found in schema cache ${this._config.schemaCacheFile}`);
      cols = table.columns;
    }

    if (!cols) throw new Error(`Could not load schema for "${schema}.${name}`);

    const columns = cols.map((r: ColumnSchema) => {
      const col: Column = {
        name: r.name, nullable: r.nullable, pkey: r.pkey, pgtype: r.type, type: r.type[0] === '_' ? 'any[]' : 'any', elidable: false, length: r.length, precision: r.precision
      }
      if (~col.type.toLowerCase().indexOf('json')) col.json = true;
      if (col.type[0] === '_') col.array = true;
      if (r.default != null) col.pgdefault = r.default;
      if (col.nullable || col.pgdefault) col.elidable = true;
      col.type = Types[col.cast || col.pgtype] || col.type;

      if (r.enum) {
        col.enum = r.enum;
        col.type = 'string';
      }

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

    return { name, columns };
  }
}

class PrivateBuilder extends Builder {
  constructor(cfg: BuilderConfig) {
    super(cfg);
  }

  end(): Promise<void> {
    if (this._pool) return this._pool.end();
  }
}
