import * as fs from 'fs-extra';
import * as pg from 'pg';
import { SchemaConfig, tableQuery, columnQuery, SchemaCache, ColumnSchema, FunctionSchema, functionQuery } from './main';

export interface PatchOptions {
  details?: boolean;
  commit?: boolean;
  tables?: string[];
  functions?: string[];
  connect?: pg.ClientConfig & { schemaCacheFile?: string };
  log?: (msg: string) => void;
}

export interface PatchConfig extends pg.ClientConfig, SchemaConfig {
  name?: string;
}

export interface PatchResult {
  tables: { [name: string]: string[] };
  functions: { [name: string]: string };
  statements: string[];
}

function createColumn(c: ColumnSchema): string {
  let sql = `"${c.name}" `;
  if (c.pkey && c.default && ~c.default.indexOf(`_${c.name}_seq`) && ~c.default.indexOf('nextval(')) {
    if (c.type === 'int8') return `${sql} bigserial primary key`;
    else if (c.type === 'int4') return `${sql} serial primary key`;
  }
  return `${sql} ${c.type}${c.precision ? `(${c.precision.join(', ')})` : ''}${c.length ? `(${c.length})` : ''}${c.nullable ? '' : ' not null'}${c.pkey ? ' primary key' : ''}${c.default ? ` default ${c.default}` : ''}`;
}

function colType(c: ColumnSchema) {
  return `${c.type}${c.precision ? `(${c.precision.join(', ')})` : ''}${c.length ? `(${c.length})` : ''}`;
}

export async function patchConfig(config: PatchConfig, opts: PatchOptions = {}) {
  const connect = opts.connect || config;
  const log = opts.log || console.error;
  const res: PatchResult = {
    tables: {},
    functions: {},
    statements: [],
  };

  if (config && config.schemaCacheFile && connect && connect.database) {
    const qs: string[] = res.statements;
    const client = new pg.Client(connect);
    await client.connect();
    const cache: SchemaCache = JSON.parse(await fs.readFile(connect.schemaCacheFile, { encoding: 'utf8' }));
    const schema: SchemaCache = { tables: [] };
    if (!cache.functions) cache.functions = [];

    const name = config.name ? `${config.name} (${connect.user || process.env.USER}@${connect.host || 'localhost'}:${connect.port || 5432}/${connect.database || process.env.USER})` : `${connect.user || process.env.USER}@${connect.host || 'localhost'}:${connect.port || 5432}/${connect.database || process.env.USER})`;

    log(`Patching ${name}...`);

    try {
      const allCols = (await client.query(columnQuery)).rows;

      for (const tbl of (await client.query(tableQuery)).rows) {
        if (config.schemaInclude && !config.schemaInclude.includes(tbl.name)) continue;
        else if (config.schemaExclude && config.schemaExclude.includes(tbl.name)) continue;
        else schema.tables.push({ name: tbl.name, schema: tbl.schema, columns: allCols.filter(c => c.schema === tbl.schema && c.table === tbl.name).map(c => Object.assign({}, c, { table: undefined, schema: undefined, length: c.length || undefined, precision: c.precision || undefined })) });
      }

      schema.functions = (await client.query(functionQuery)).rows;

      for (const ct of cache.tables) {
        if (opts.tables && !opts.tables.includes(ct.name)) continue;
        const t = schema.tables.find(e => e.name === ct.name && e.schema === ct.schema);
        if (!t) {
          const q = `create table "${ct.name}" (${ct.columns.map(createColumn).join(', ')});`;
          qs.push(q);
          res.tables[ct.name] = [q];
        } else {
          for (const col of ct.columns) {
            const c = t.columns.find(e => e.name === col.name);
            if (!c) {
              const q = `alter table "${ct.name}" add column "${col.name}" ${col.type}${col.nullable ? '' : ' not null'}${col.default ? ` default ${col.default}` : ''};`;
              qs.push(q);
              (res.tables[ct.name] || (res.tables[ct.name] = [])).push(q);
            } else if (opts.details && (c.default !== col.default || c.nullable !== col.nullable || c.type !== col.type || c.length !== col.length || JSON.stringify(c.precision || []) !== JSON.stringify(col.precision || []))) {
              const t = res.tables[ct.name] || (res.tables[ct.name] = []);
              if (c.type !== col.type || c.length !== col.length || JSON.stringify(c.precision || []) !== JSON.stringify(col.precision || [])) {
                const q = `alter table "${ct.name}" alter column "${col.name}" type ${colType(col)};`;
                qs.push(q);
                t.push(q);
              }
              if (c.default !== col.default) {
                const q = `alter table "${ct.name}" alter column "${col.name}" ${col.default ? 'set' : 'drop'} default${col.default ? ` ${col.default}` : ''};`;
                qs.push(q);
                t.push(q);
              }
              if (c.nullable !== col.nullable) {
                const q = `alter table "${ct.name}" alter column "${col.name}" ${col.nullable ? 'drop' : 'set'} not null;`;
                qs.push(q);
                t.push(q);
              }
            }
          }
        }
      }

      for (const cf of cache.functions) {
        if (opts.functions && !opts.functions.includes(cf.name)) continue;
        const f = schema.functions.find(e => e.schema === cf.schema && e.name === cf.name && e.args === cf.args && e.result === cf.result);
        if (!f || f.def !== cf.def) {
          qs.push(cf.def);
          res.functions[`${cf.name}(${cf.args}): ${cf.result}`] = cf.def;
        }
      }

      if (qs.length) {
        log(`\nPatches for ${name}`);
        log(qs.join('\n'));
        if (opts.commit) {
          await client.query(['begin;'].concat(qs).concat('commit;').join('\n'));
        }
      } else {
        log(`\nNo patches needed for ${name}`);
      }
    } finally {
      await client.end();
    }
  }

  return res;
}

