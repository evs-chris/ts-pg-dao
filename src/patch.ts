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

export const depsQuery = `select obj_schema "schema", obj_name "name", obj_type "type" from
  (
    with recursive recursive_deps(obj_schema, obj_name, obj_type, depth) as
    (
      select $1::varchar collate "C", $2::varchar collate "C", null::varchar collate "C", 0
      union
      select dep_schema::varchar collate "C", dep_name::varchar collate "C", dep_type::varchar collate "C",
        recursive_deps.depth + 1 from
      (
        select ref_nsp.nspname ref_schema, ref_cl.relname ref_name,
          rwr_cl.relkind dep_type, rwr_nsp.nspname dep_schema,
          rwr_cl.relname dep_name
        from pg_depend dep
        join pg_class ref_cl on dep.refobjid = ref_cl.oid
        join pg_namespace ref_nsp on ref_cl.relnamespace = ref_nsp.oid
        join pg_rewrite rwr on dep.objid = rwr.oid
        join pg_class rwr_cl on rwr.ev_class = rwr_cl.oid
        join pg_namespace rwr_nsp on rwr_cl.relnamespace = rwr_nsp.oid
        where dep.deptype = 'n'
        and dep.classid = 'pg_rewrite'::regclass
      ) deps
      join recursive_deps on deps.ref_schema = recursive_deps.obj_schema
        and deps.ref_name = recursive_deps.obj_name
      where (deps.ref_schema != deps.dep_schema or deps.ref_name != deps.dep_name)
    )
    select obj_schema, obj_name, obj_type, depth
    from recursive_deps
    where depth > 0
  ) t
group by obj_schema, obj_name, obj_type
order by max(depth) desc`;

export async function calcDropRestore(connect: pg.Client, schema: string, table: string): Promise<{ drop: string[]; restore: string[] }> {
  const deps: Array<{ schema: string; name: string; type: string }> = (await connect.query(depsQuery, [schema, table])).rows;

  const drop: string[] = [];
  const restore: string[] = [];

  for (const d of deps) {
    const comment = (await connect.query(`select obj_description((select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and c.relname = $2 and relkind in ('v', 'm'))) as comment`, [d.schema, d.name])).rows[0].comment;
    if (comment != null) restore.push(`COMMENT ON ${d.type === 'm' ? 'MATERIALIZED ' : ''}VIEW "${d.schema}"."${d.name}" IS $comment$${comment}$comment$;`);
    restore.push(`CREATE ${d.type === 'm' ? 'MATERIALIZED ' : ''}VIEW "${d.schema}"."${d.name}" AS ${(await connect.query(`select pg_get_viewdef((select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and c.relname = $2 and relkind in ('v', 'm'))) as def`, [d.schema, d.name])).rows[0].def}`);
    drop.push(`DROP ${d.type === 'm' ? 'MATERIALIZED ' : ''}VIEW "${d.schema}"."${d.name}";`);
  }

  return { drop, restore: restore.reverse() };
}

