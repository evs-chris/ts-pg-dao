import * as fs from 'fs-extra';
import * as ts from 'typescript';
import * as path from 'path';
import { BuildConfig, BuiltConfig, BuilderOptions, Config, Model, Column, Query, Param, Output, MergedOutput, IncludeMap, IncludeExtraDef, SchemaCache, tableQuery, columnQuery } from './main';
import * as requireCwd from 'import-cwd';
import * as pg from 'pg';

export interface ConfigOpts {
  forceCache?: boolean;
}

export async function config(file, opts: ConfigOpts = {}): Promise<BuildConfig|BuildConfig[]> {
  const input = await fs.readFile(file, { encoding: 'utf8' });
  const output = ts.transpileModule(input, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      moduleResolution: ts.ModuleResolutionKind.NodeJs,
      lib: [ 'node', 'es2017' ],
      target: ts.ScriptTarget.ES2017
    }
  });

  interface ConfigModule {
    default?: any
  }
  const config: ConfigModule = {};
  if (opts.forceCache) BuilderOptions.forceCache = true;
  (new Function('exports', 'require', '__filename', '__dirname', output.outputText))(config, requireCwd, path.resolve(file), path.dirname(path.resolve(file)));

  if (typeof config.default !== 'object' || typeof config.default.then !== 'function') {
    throw new Error('Config file should export a default Promise<BuildConfig|BuildConfig[]>.');
  }

  return config.default as Promise<BuildConfig|BuildConfig[]>
}

function isPathy(output: Output): output is MergedOutput {
  return 'path' in output;
}

type ProcessModel = Model & { extraTypes?: Array<[string, string]>, extraImports?: string[], serverOuter?: string, serverBody?: string };

const modelImport = /^\s*import\b/;

export async function write(config: BuiltConfig): Promise<void> {
  const { models } = config;

  let pathy: boolean = false;
  let serverPath: string;
  let clientPath: string;
  if (isPathy(config.output)) {
    serverPath = config.output.path;
    await fs.mkdirp(serverPath);
    pathy = true;
  } else {
    serverPath = config.output.server;
    clientPath = config.output.client;
    await fs.mkdirp(clientPath);
    await fs.mkdirp(serverPath);
  }

  let model: ProcessModel;
  // if there is a database in the client config, use the database, otherwise it may just be cached schema
  const client: false | pg.Client = !!config.pgconfig.database && new pg.Client(config.pgconfig);
  try {
    client && await client.connect();

    for (let j = 0; j < models.length; j++) {
      model = models[j];

      model.extraTypes = [];
      model.extraImports = [];
      model.serverOuter = '';
      model.serverBody = '';

      let i = model._imports.length;
      while (i--) {
        if (!modelImport.test(model._imports[i])) model.extraImports.push(model._imports.splice(i, 1)[0]);
      }
    
      for (i = 0; i < model.queries.length; i++) {
        const q = model.queries[i];
        const res = processQuery(config, q, model);
        if (res.interface) {
          const prev = model.extraTypes.find(([t]) => t === res.interface[0]);
          if (!prev) model.extraTypes.push(res.interface);
          else if (prev[1] !== res.interface[1]) throw new Error(`Query ${q.name} interface ${prev[0]} has multiple conflicting types`);
        }
        if (res.outer) model.serverOuter += res.outer + '\n';
        model.serverBody += '\n\n' + res.method;
        if (res.others.length) {
          for (const o of res.others) {
            if (!model.extraImports.includes(o)) model.extraImports.push(o);
          }
        };
        
        if (client) {
          const sql = combineParts(res.sql, res.params, res.parts);
          let part: string;
          try {
            for (const s of sql) {
              part = s;
              await client.query(`PREPARE __pg_dao_check_stmt AS ${s}; DEALLOCATE __pg_dao_check_stmt;`);
            }
          } catch (e) {
            console.error(`Error processing query ${q.name} for ${model.table} - ${q.sql}\n---part--\n${part}\n\n${e.message}`);
            throw e;
          }
        }
      }

      const server = path.join(serverPath, (model.file || model.name) + '.ts');
      console.log(`\twriting server ${model.name} to ${server}...`);
      await fs.writeFile(server, serverModel(config, model));
      if (!pathy) {
        const client = path.join(clientPath, (model.file || model.name) + '.ts');
        console.log(`\twriting client ${model.name} to ${client}...`);
        await fs.writeFile(client, clientModel(config, model));
      }
    }

    if (client && config.pgconfig.schemaCacheFile) {
      const cache: SchemaCache = { tables: [] };
      const cfg = config.pgconfig;
      console.log(`\t - generating schema cache`);

      try {
        for (const tbl of (await client.query(tableQuery)).rows) {
          if (cfg.schemaInclude && !cfg.schemaInclude.includes(tbl.name)) continue;
          else if (cfg.schemaExclude && cfg.schemaExclude.includes(tbl.name)) continue;
          else if (!models.find(m => m.table === tbl.name) && !cfg.schemaFull) continue;
          else cache.tables.push({ name: tbl.name, schema: tbl.schema, columns: (await client.query(columnQuery, [tbl.schema, tbl.name])).rows });
        }
      } catch (e) {
        console.error(`Error generating schema cache:`, e);
      }

      console.log(`\t - writing schema cache ${cfg.schemaCacheFile}`);
      await fs.writeFile(cfg.schemaCacheFile, JSON.stringify(cache, null, ' '), { encoding: 'utf8' });
    }
  } finally {
    client && await client.end();
  }

  if (config.index) {
    const tpl = models.map((m: ProcessModel) => {
      const exports: string[] = [];
      if (m.extraTypes && m.extraTypes.length) exports.push(...m.extraTypes.map(t => t[0]));
      if (m._exports.server) exports.push(...m._exports.server);
      if (m._exports.both) exports.push(...m._exports.both);
      return `export { default as ${m.name}${exports.length ? `, ${exports.join(', ')}` : ''} } from './${m.file || m.name}';`;
    }).join('\n');
    const server = path.join(serverPath, typeof config.index === 'string' ? config.index : 'index.ts');
    console.log(`\twriting server index to ${server}...`);
    await fs.writeFile(server, tpl);
    if (!pathy) {
      const tpl = models.map((m: ProcessModel) => {
        const exports: string[] = [];
        if (m.extraTypes && m.extraTypes.length) exports.push(...m.extraTypes.map(t => t[0]));
        if (m._exports.client) exports.push(...m._exports.client);
        if (m._exports.both) exports.push(...m._exports.both);
        return `export { default as ${m.name}${exports.length ? `, ${exports.join(', ')}` : ''} } from './${m.file || m.name}';`;
      }).join('\n');
      const client = path.join(clientPath, typeof config.index === 'string' ? config.index : 'index.ts');
      console.log(`\twriting client index to ${client}...`);
      await fs.writeFile(client, tpl);
    }
  }
}

const dates = ['timestamp', 'date'];
function serverModel(config: BuiltConfig, model: ProcessModel): string {
  let tpl = `import * as dao from '@evs-chris/ts-pg-dao/runtime';${model.extraImports && model.extraImports.length ? '\n' + model.extraImports.map(o => `import ${o} from './${o}';`).join('\n') + '\n' : ''}${model._imports.length ? '\n' + model._imports.join(';\n') + '\n' : ''}${model.serverOuter ? `\n${model.serverOuter}` : ''}
export default class ${model.name} {
  static get table() { return ${JSON.stringify(model.table)}; }
`;

  const hasPkey = model.pkeys.length;

  const loadFlag = ((!model.flags.load && model.flags.load !== false) || model.flags.load) ? model.flags.load || '__loaded' : '';
  const changeFlag = hasPkey && ((!model.flags.change && model.flags.change !== false) || model.flags.change) ? model.flags.change || '__changed' : '';
  const removeFlag = hasPkey && ((!model.flags.remove && model.flags.remove !== false) || model.flags.remove) ? model.flags.remove || '__removed' : '';

  if (loadFlag) tpl += `  ${loadFlag}?: boolean;
`;
  if (changeFlag) tpl += `  ${changeFlag}?: boolean;
`;
  if (removeFlag) tpl += `  ${removeFlag}?: boolean;
`;

  tpl += modelProps(config, model);

  for (const [name, hook] of Object.entries(model.hooks)) {
    if (typeof hook === 'function') {
      tpl += `  protected static ${name}: (item: ${model.name}) => void = ${reindent(hook.toString(), '  ')}\n`;
    }
  }

  if (hasPkey) {
    tpl += `
  static async save(con: dao.Connection, model: ${model.name}): Promise<void> {
    if (!model) throw new Error('Model is required');${model.hooks.beforesave ? `
    ${model.name}.beforesave(model);` : ''}
    ${model.fields.filter(f => ~dates.indexOf(f.pgtype)).map(f => `if (typeof model.${f.name} === 'string') model.${f.name} = new Date(model.${f.name});
    `).join('')}

    if (${model.fields.filter(f => f.pkey || f.optlock).map(f => `model.${f.name} !== undefined`).join(' && ')}) {${updateMembers(model, '      ')}

      const transact = !con.inTransaction;
      if (transact) await con.begin();
      try {
        const res = await con.query(sql, params);
        if (res.rowCount < 1) throw new Error('No matching row to update for ${model.name}');
        if (res.rowCount > 1) throw new Error('Too many matching rows updated for ${model.name}');
        if (transact) await con.commit();${changeFlag ? `
        model.${changeFlag} = false;` : ''}
      } catch (e) {
        if (transact) await con.rollback();
        throw e;
      }
      ${model.fields.find(f => f.optlock) ? `${model.fields.filter(f => f.optlock).map(f => `\n      model.${f.alias || f.name} = lock;`).join('')}` : ''}
    } else {${insertMembers(model, '      ')}${loadFlag ? `
      model.${loadFlag} = false;` : ''}${changeFlag ? `
      model.${changeFlag} = false;` : ''}
    }
  }

  static async delete(con: dao.Connection, model: ${model.name}): Promise<void> {
    if (!model) throw new Error('Model is required');${model.hooks.beforedelete ? `
    ${model.name}.beforeDelete(model);` : ''}
    ${model.fields.filter(f => ~dates.indexOf(f.pgtype)).map(f => `if (typeof model.${f.name} === 'string') model.${f.name} = new Date(model.${f.name});
    `).join('')}
    const transact = !con.inTransaction;
    if (transact) await con.begin();
    try {
      const params = [${model.fields.filter(f => f.pkey || f.optlock).map(f => `model.${f.name}`).join(', ')}];
      const res = await con.query(\`delete from "${model.table}" where ${model.fields.filter(f => f.pkey || f.optlock).map((f, i) => `${f.optlock ? `date_trunc('millisecond', "${f.name}")` : `"${f.name}"`} = $${i + 1}`).join(' AND ')}\`, params);
      if (res.rowCount < 1) throw new Error('No matching row to delete for ${model.name}');
      if (res.rowCount > 1) throw new Error('Too many matching rows deleted for ${model.name}');
      if (transact) await con.commit();
    } catch (e) {
      if (transact) await con.rollback();
      throw e;
    }
  }
  
  ${model.queries.find(q => q.name === 'findById') ? '' : `static async findById(con: dao.Connection, ${model.pkeys.map(k => `${k.alias || k.name}: ${k.retype || k.type}`).join(', ')}, optional: boolean = false): Promise<${model.name}> {
    return await ${model.name}.findOne(con, '${model.pkeys.map((k, i) => `${k.name} = $${i + 1}`).join(' AND ')}', [${model.pkeys.map(k => k.alias || k.name).join(', ')}], optional)
  }

  `}static keyString(row: any, prefix: string = ''): string {
    return '${model.table}' + ${model.pkeys.map(k => `'_' + (prefix ? row[prefix + '${k.name}'] : row.${k.name})`).join(' + ')}
  }`;
  } else {
    tpl += `
  static async insert(con: dao.Connection, model: ${model.name}): Promise<void> {
    if (!model) throw new Error('Model is required');${model.hooks.beforesave ? `
    ${model.name}.beforesave(model);
    ` : ''}${model.fields.filter(f => ~dates.indexOf(f.pgtype)).map(f => `if (typeof model.${f.name} === 'string') model.${f.name} = new Date(model.${f.name});
    `).join('')}
    ${insertMembers(model, '    ')}${loadFlag ? `
    model.${loadFlag} = false;` : ''}
  }
  `
  }

  tpl += `
  static async findOne(con: dao.Connection, where: string = '', params: any[] = [], optional: boolean = false): Promise<${model.name}> {
    const res = await ${model.name}.findAll(con, where, params);
    if (res.length < 1 && !optional) throw new Error('${model.name} not found')
    if (res.length > 1) throw new Error('Too many results found');
    return res[0];
  }

  static async findAll(con: dao.Connection, where: string = '', params: any[] = []): Promise<${model.name}[]> {
    const res = await con.query('select ${model.select()} from "${model.table}"' + (where ? ' WHERE ' + where : ''), params);
    return res.rows.map(r => ${model.name}.load(r, new ${model.name}()));
  }

  static load(row: any, model: any, prefix: string = '', cache: dao.Cache = null): any {
    ${hasPkey ? `if (${model.pkeys.map(k => `row[prefix + ${JSON.stringify(k.name)}] == null`).join(' && ')}) return;
    if (cache && (model = cache[${model.name}.keyString(row, prefix)])) return model;
    ` : ''}if (!model) model = {};${hasPkey ? `
    if (cache) cache[${model.name}.keyString(row, prefix)] = model;` : ''}${loadFlag ? `
    model.${loadFlag} = true;` : ''}

`

  for (const f of model.cols) {
    tpl += `    if ((prefix + '${f.name}') in row) model.${f.alias || f.name} = prefix ? row[prefix + '${f.name}'] : row.${f.name};\n`
  }
    
  tpl += `    
    return model;
  }`;

  tpl += `${model.serverBody}\n`;

  if (model.codeMap.serverInner) tpl += `\n\n${reindent(model.codeMap.serverInner, '  ')}\n`;
  if (model.codeMap.bothInner) tpl += `\n\n${reindent(model.codeMap.bothInner, '  ')}\n`;

  tpl += '}';

  if (model.codeMap.serverOuter) tpl += `\n\n${model.codeMap.serverOuter}\n`;
  if (model.codeMap.bothOuter) tpl += `\n\n${model.codeMap.bothOuter}\n`;

  return tpl;  
}

function clientModel(config: Config, model: ProcessModel): string {
  let tpl = `${
    model.extraImports && model.extraImports.length ? model.extraImports.map(o => `import ${o} from './${o}';`).join('\n') + '\n' : ''
}${model._imports.length ? '\n' + model._imports.join(';\n') + '\n' : ''}${model.extraTypes && model.extraTypes.length ? '\n' + model.extraTypes.map(([n, t]) => `export type ${n} = ${t};`).join('\n') + '\n' : ''
}export default class ${model.name} {\n`;

  const hasPkey = model.pkeys.length > 0;

  const loadFlag = ((!model.flags.load && model.flags.load !== false) || model.flags.load) ? model.flags.load || '__loaded' : '';
  const changeFlag = hasPkey && ((!model.flags.change && model.flags.change !== false) || model.flags.change) ? model.flags.change || '__changed' : '';
  const removeFlag = hasPkey && ((!model.flags.remove && model.flags.remove !== false) || model.flags.remove) ? model.flags.remove || '__removed' : '';

  if (loadFlag) tpl += `  ${loadFlag}?: boolean;
`;
  if (changeFlag) tpl += `  ${changeFlag}?: boolean;
`;
  if (removeFlag) tpl += `  ${removeFlag}?: boolean;
`;

  tpl += modelProps(config, model, true);

  if (model.codeMap.clientInner) tpl += `\n\n${reindent(model.codeMap.clientInner, '  ')}\n`;
  if (model.codeMap.bothInner) tpl += `\n\n${reindent(model.codeMap.bothInner, '  ')}\n`;

  tpl += `}`;

  if (model.codeMap.clientOuter) tpl += `\n\n${model.codeMap.clientOuter}\n`;
  if (model.codeMap.bothOuter) tpl += `\n\n${model.codeMap.bothOuter}\n`;

  return tpl;
}

function modelProps(config: Config, model: Model, client: boolean = false): string {
  let tpl = '';

  let col: Column;
  for (let c = 0; c < model.cols.length; c++) {
    col = model.cols[c];
    tpl += `  ${col.alias || col.name}${col.nullable ? '?' : ''}: ${col.enum ? col.enum.map(v => `'${v}'`).join('|') : (col.retype || col.type)}${col.default ? ` = ${col.default}` : ''};\n`;
  }

  if (Object.keys(model._extras).length > 0) {
    tpl += '\n';
    for (const [field, type] of Object.entries(model._extras)) {
      tpl += `  ${field}?: ${type};\n`;
    }
  }

  return tpl;
}

function colToParam(f) {
  return `${f.optlock ? 'new Date()' : f.type === 'any' ? `Array.isArray(model.${f.alias || f.name}) ? JSON.stringify(model.${f.alias || f.name}) : model.${f.alias || f.name}` : `model.${f.alias || f.name}`}`;
}

function updateMembers(model: Model, prefix: string): string {
  let res = `\n${prefix}const params = [];\n${prefix}const sets = [];\n${prefix}let sql = 'UPDATE ${model.table} SET ';`;
  if (model.fields.find(f => f.optlock)) res += `\n${prefix}const lock = new Date();`;
  model.fields.forEach(f => {
    if (!f.pkey && !f.optlock) {
      res += setParam(f, prefix, `sets.push('"${f.name}" = $' + params.length)`);
    }
  });

  const locks = model.fields.filter(f => f.optlock);

  if (locks.length) {
    res += `\n${locks.map(l => `${prefix}params.push(lock);\n${prefix}sets.push('"${l.name}" = $' + params.length);`).join('\n')}`;
  }
  res += `\n\n${prefix}sql += sets.join(', ');\n`;
  res += `\n${prefix}const count = params.length;`;
  const where = model.fields.filter(f => f.pkey || f.optlock);
  res += `\n${prefix}params.push(${where.map(f => `model.${f.alias || f.name}`).join(', ')});`;
  res += `\n${prefix}sql += \` WHERE ${where.map((f, i) => `${f.optlock ? `date_trunc('millisecond', "${f.name}")` : `"${f.name}"`} = $\${count + ${i + 1}}`).join(' AND ')}\`;`

  return res;
}

function insertMembers(model: Model, prefix: string): string {
  let res = `\n${prefix}const params = [];\n${prefix}const sets = [];\n${prefix}let sql = 'INSERT INTO ${model.table} ';`;
  for (const f of model.fields) {
    if (!f.optlock) {
      res += setParam(f, prefix, `sets.push('${f.name}')`);
      if (!f.elidable) res += `\n${prefix}else throw new Error('Missing non-elidable field ${f.alias || f.name}');`;
    }
  }

  const ret = model.fields.filter(f => f.pkey || f.optlock || (f.elidable && f.pgdefault));
  const locks = model.fields.filter(f => f.optlock);
  res += `\n${prefix}sql += \`(\${sets.map(s => \`"\${s}"\`).join(', ')}\${${locks.length} && sets.length ? ', ' : ''}${locks.map(l => l.name).join(', ')}) VALUES (\${sets.map((n, i) => \`$\${i + 1}\`)}\${${locks.length} && sets.length ? ', ' : ''}${locks.map(l => 'now()').join(', ')})${ret.length ? ` RETURNING ${ret.map(f => `"${f.name}"`).join(', ')}` : ''};\`;`;
  res += `\n\n${prefix}const res = (await con.query(sql, params)).rows[0];`
  res += ret.map(f => `\n${prefix}model.${f.alias || f.name} = res.${f.name};`).join('');

  return res;
}

function setParam(col: Column, prefix: string, set: string, model: string = 'model', params: string = 'params'): string {
  let cond = `\n${prefix}if (${model}.hasOwnProperty('${col.alias || col.name}')) { `
  if (col.pgtype !== 'char' && col.pgtype !== 'varchar' && col.pgtype !== 'bpchar' && col.pgtype !== 'text') {
    cond += `\n${prefix}  if ((${model}['${col.alias || col.name}'] as any) === '') { ${params}.push(null); ${set}; }\n${prefix}  else { ${params}.push(${colToParam(col)}); ${set}; }\n${prefix}`;
  } else {
    cond += `${params}.push(${colToParam(col)}); ${set}; `;
  }
  cond += '}';
  return cond;
}

const tableAliases = /@"?([a-zA-Z_]+[a-zA-Z0-9_]*)"?(?!\.)\s(?:(?!\s*(?:left|right|cross|inner|outer|on|where)\s)\s*(?:[aA][sS]\s)?\s*"?([a-zA-Z_]+[a-zA-Z0-9_]*)?"?)?/gi;
const fieldAliases = /@:?"?([a-zA-Z_]+[a-zA-Z0-9_]*)"?\."?([a-zA-Z_]+[a-zA-Z0-9_]+|\*)"?/gi;
const params = /\$([a-zA-Z_]+[a-zA-Z0-9_]*)/g;

interface Alias {
  model: Model;
  alias: string;
  prefix: string;
  root: boolean;
  cols?: Column[];
  extra?: IncludeExtraDef[];
  include?: IncludeAlias[];
  type?: string;
}
interface AliasMap { [key: string]: Alias }
interface IncludeAlias {
  name: string;
  type: 'one' | 'many';
}

type ProcessQuery = Query & { aliases: AliasMap, root: Alias };
interface ProcessQueryResult {
  method: string;
  outer?: string;
  others: string[];
  interface?: [string, string];
  sql: string;
  params: number;
  parts: QueryPartCheck[];
}

interface QueryPart {
  code: string;
  params: Param[];
  check: QueryPartCheck;
}

type QueryPartCheck = (params: number) => [string, number];

function processQuery(config: Config, start: Query, model: ProcessModel): ProcessQueryResult {
  const query = start as ProcessQuery;
  const parms: Param[] = [];
  const aliases: AliasMap = query.aliases = {};
  const parts: QueryPart[] = [];
  let root: Alias;

  if (start.scalar && !start.owner.cols.find(c => c.name === start.scalar)) throw new Error(`Scalar query ${query.owner.name}.${query.name} must return a field from ${query.owner.table}.`);

  // process params
  function mapParams(sql: string, ps: Param[], offset?: string): [string, QueryPartCheck] {
    let fn: QueryPartCheck = null;

    const str = sql.replace(params, (str, name) => {
      let p, i;
      if (!query.params || !(p = query.params.find(p => p.name === name))) throw new Error(`Query ${query.owner.name}.${query.name} references parameter ${name} that is not defined.`);
      if (~(i = parms.indexOf(p))) return `$${i + 1}`;
      else if (~(i = ps.indexOf(p))) return offset ? `$\${${offset} + ${i + 1}}` : `$${i + 1}`;
      else {
        ps.push(p);
        return offset ? `$\${${offset} + ${ps.length}}` : `$${ps.length}`;
      }
    });

    if (offset) {
      fn = num => {
        let len = num;
        const s = sql.replace(params, (str, name) => {
          let i: number;
          const p = query.params.find(p => p.name === name);
          if (~(i = parms.indexOf(p))) return `$${i + 1}`;
          else if (~(i = ps.indexOf(p))) return `$${len + i + 1}`;
          else return `$${++len}`;
        });
        return [s, len];
      };
    }

    return [str, fn];
  }

  let sql = mapParams(query.sql, parms)[0];

  // map tables to models and record relevant aliases
  sql = sql.replace(tableAliases, (m ,tbl, alias) => {
    const mdl = findModel(tbl, config);
    if (!mdl) throw new Error(`Could not find model for table ${tbl} referenced in query ${query.name}.`);

    const entry = { model: mdl, prefix: `${alias || tbl}__`, alias: alias || tbl, root: !alias && mdl === query.owner };
    if (entry.root) query.root = root = entry;
    aliases[alias || tbl] = entry;
    return `${tbl}${alias ? ` AS ${alias}` : ''} `;
  });

  if (!root) {
    query.root = root = { model: query.owner, prefix: '', alias: query.owner.table, root: true };
    aliases[root.alias] = root;
  }

  // compute includes and select lists if available
  if (query.include && root) {
    buildIncludes(query, root, query.include);
  }

  buildTypes(query, root);

  // map and expand column aliases as necessary
  function mapFields(sql: string): string {
    return sql.replace(fieldAliases, (m, alias, col) => {
      let entry = aliases[alias];
      let mdl: Model;
      if (entry) mdl = entry.model;
      else {
        throw new Error(`Query ${query.owner.name}.${query.name} requires an entry for ${alias} before its use in column ref ${alias}.${col}.`);
      }

      if (col === '*') {
        return (entry.cols || entry.model.cols).map(c => `${alias}.${c.name} AS ${entry.prefix}${c.name}`).join(', ');
      } else {
        const c = mdl.fields.find(f => col === f.name);
        if (!c) throw new Error(`Could not find field for ${alias}.${col} referenced in query ${query.name}.`);

        if (~m.indexOf(':')) {
          return `${entry.prefix}${c.name}`;
        } else {
          return `${alias}.${c.name} AS ${entry.prefix}${c.name}`;
        }
      }
    });
  }

  sql = mapFields(sql);

  const defaulted = (query.params || []).reduce((a, c) => a && (c.optional || !!c.default), true);

  query.parts && Object.entries(query.parts).forEach(([condition, sql]) => {
    const ps: Param[] = [];
    const [str, check] = mapParams(mapFields(sql), ps, 'ps.length');
    const cond = condition.replace(params, (str, name) => {
      if (!start.params.find(p => p.name === name)) throw new Error(`Query ${query.owner.name}.${query.name} has an invalid part condition referencing unknown parameter $${name}.`);
      return `params.${name}`;
    });
    const code = `if (${defaulted ? '' : 'params && '}${cond}) {
      sql += \`${JSON.stringify(str).slice(1, -1)}\`;${ps.length > 0 ? `
      ` : ''}${ps.map(p => `ps.push(${!defaulted ? 'params && ' : ''}'${p.name}' in params ? params.${p.name} : ${p.default});`).join('\n      ')}
    }`
    parts.push({
      code, params: ps, check,
    });
  });

  const loader = buildLoader(query);

  let outer = `const __${query.name}_sql = ${JSON.stringify(sql)};`;
  if (query.result && !model.extraTypes.find(([t]) => t === query.result)) outer += `\nexport type ${query.result} = ${loader.interface};`

  const others = [];

  for (let k in aliases) {
    if (!aliases[k].root && !~others.indexOf(aliases[k].model.name)) others.push(aliases[k].model.name);
  }

  // manual other imports
  for (const i of query.imports || []) {
    if (!others.includes(i)) others.push(i);
  }

  function getParamsValues(parms: Param[]) {
    return parms.map(p => p.default ?
      `${!defaulted ? 'params && ' : ''}'${p.name}' in params ? params.${p.name} : ${p.default}` :
      `params[${JSON.stringify(p.name)}]`).join(', ')
  }

  const res: ProcessQueryResult = {
    method: `  static async ${query.name}(con: dao.Connection, params: { ${(query.params || []).map(p => `${p.name}${p.optional || p.default ? '?' : ''}: ${p.type || 'string'}`).join(', ')} }${defaulted ? ' = {}' : ''}): Promise<${query.result ? query.result : query.scalar ? loader.interface : query.owner.name}${query.singular ? '' : '[]'}> {
    let sql = __${query.name}_sql;
    const ps: any[] = [${getParamsValues(parms)}];
    ${parts.map((p, i) => p.code).join('\n    ')}
    const query = await con.query(sql, ps);
    ${loader.loader}
  }`,
    outer,
    others,
    sql,
    parts: parts.map(p => p.check),
    params: parms.length,
  };
  if (query.result) res.interface = [query.result, loader.interface];

  return res;
}

function findModel(table: string, config: Config): Model {
  for (let i = 0; i < config.models.length; i++) {
    if (config.models[i].table === table) return config.models[i];
  }
}

function buildIncludes(query: ProcessQuery, alias: Alias, map: IncludeMap): void {
  if (map['*']) {
    alias.cols = alias.model.cols.filter(c => ~map['*'].indexOf(c.name));
    if (map['*'].length !== alias.cols.length) throw new Error(`Specified columns ${map['*'].filter(n => !alias.cols.find(c => c.name === n)).join(', ')} are not in table ${alias.model.table}.`);
  }

  for (const k in map) {
    if (k === '*') continue;

    if (k in query.aliases) {
      (alias.include || (alias.include = [])).push({ name: k, type: Array.isArray(map[k]) ? 'many' : 'one' });
      const include = map[k];
      if (Array.isArray(include)) {
        if (include.length > 1 || (include.length === 1 && typeof include[0] !== 'object')) throw new Error(`Array include ${alias.alias || alias.model.name}.${k} may only specify 0 or 1 nested mappings.`);
        if (include.length === 1) buildIncludes(query, query.aliases[k], include[0] as IncludeMap);
      } else if (typeof map[k] === 'object') {
        buildIncludes(query, query.aliases[k], include as IncludeMap);
      }
    } else if (~query.sql.indexOf(k) && typeof map[k] === 'object' && 'type' in (map[k] as object)) {
      const include = map[k] as IncludeExtraDef;
      (alias.extra || (alias.extra = [])).push(include);
      if (!include.name) include.name = k;
    } else throw new Error(`Unknown include reference ${alias.alias || alias.model.name}.${k}.`);
  }
}

function buildTypes(query: ProcessQuery, alias: Alias, level: number = 0): void {
  if (alias.type) return;
  let t = '', b = '';
  if (!alias.cols) t = alias.model.name;
  else b = `${alias.cols.map(c => `${c.alias || c.name}: ${c.retype || c.type}`).join(', ')}`;

  if (alias.extra) b += `${b.length ? ', ' : ''}${alias.extra.map(e => `${e.name}: ${e.type}`).join(', ')}`;

  if (alias.include) {
    for (const a of alias.include) {
      buildTypes(query, query.aliases[a.name], level + 1);
    }
    b += `${b.length ? ', ' : ''}${alias.include.map(i => `${i.name}${i.type === 'many' ? '' : '?'}: ${query.aliases[i.name].type}${i.type === 'many' ? '[]' : ''}`).join(', ')}`;
  }

  if (!level && query.extras && Object.keys(query.extras).length) b += `${b.length ? ', ' : ''}${Object.entries(query.extras).map(([field, type]) => `${field}?: ${type}`).join(', ')}`;

  if (t && b) alias.type = `${t} & { ${b} }`;
  else if (t) alias.type = t;
  else if (b) alias.type = `{ ${b} }`;
}

function buildLoader(query: ProcessQuery): { loader: string, interface: string } {
  
  let tpl = `
    const res = [];
    const cache: dao.Cache = {};
    query.rows.forEach(r => {${query.scalar ? `
      res.push(r[${JSON.stringify((query.root.prefix || '') + query.scalar)}]);` : `${processLoader(query, query.root)}if (!~res.indexOf(o)) res.push(o);`}
    });
  `;

  if (query.singular) {
    if (!query.optional) {
      tpl += `
    if (res.length < 1) throw new Error('${query.owner.name} not found');`;
    }
    tpl += `
    if (res.length > 1) throw new Error('Too many ${query.owner.name} results found');
    return res[0];`;
  } else {
    tpl += `
    return res;`;
  }

  const scalar = query.scalar && query.owner.cols.find(c => c.name === query.scalar);
  return { loader: tpl, interface: query.scalar ? (scalar.retype || scalar.type) : query.root.type };
}

interface Depth { n: number };
function processLoader(query: ProcessQuery, alias: Alias, depth: Depth = { n: 0 }): string {
  const r = depth.n;
  let tpl = `
      const o${r || ''}: object = ${alias.model.name}.load(r, null, ${alias.prefix ? JSON.stringify(alias.prefix) : '\'\''}, cache)`
  // if key cols aren't included, allow the object to be created every time
  if (alias.cols && !alias.model.cols.filter(c => c.pkey).reduce((a, c) => a && !!alias.cols.find(a => a.name === c.name), true)) tpl += ' || {}';
  tpl += `;${!r ? '\n\n      ' : ''}`;
  if (alias.extra && alias.extra.length) {
    for (const e of alias.extra) {
     tpl += `o${r || ''}[${JSON.stringify(e.name)}] = r[${JSON.stringify(e.name)}];
      `
    }
  }
  if (alias.include && alias.include.length) {
    for (const i of alias.include) {
      const n = ++depth.n;
      const deep = processLoader(query, query.aliases[i.name], depth);

      tpl += `if (!o${r || ''}[${JSON.stringify(i.name)}]) o${r || ''}[${JSON.stringify(i.name)}] = ${i.type === 'many' ? '[]' : 'null'};${deep}
      `;
      if (i.type === 'one') {
        tpl += `if (o${n}) o${r || ''}[${JSON.stringify(i.name)}] = o${n};

      `
      } else {
        tpl += `if (o${n} && !~o${r || ''}[${JSON.stringify(i.name)}].indexOf(o${n})) o${r || ''}[${JSON.stringify(i.name)}].push(o${n});

      `
      }
    }
  }
  return tpl;
}

function reindent(fn: string, prefix: string): string {
  const lines = fn.split('\n');
  const indent = lines[lines.length - 1].replace(/^(\s*).*/, '$1');
  return fn.replace(new RegExp(`^${indent}`, 'gm'), prefix);
}

function combineParts(sql: string, params: number, parts: QueryPartCheck[]): string[] {
  const res: string[] = [sql];
  for (let i = 0; i < parts.length; i++) {
    let num = params;
    res.push(sql + parts.filter((p, j) => (i + 1) & (j + 1)).map(p => {
      const [sql, count] = p(num);
      num += count;
      return sql;
    }).join(''));
  }
  return res;
}
