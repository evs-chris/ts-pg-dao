import * as fs from 'fs-extra';
import * as ts from 'typescript';
import * as path from 'path';
import { BuildConfig, Config, Model, Column, Query, Output, MergedOutput, SplitOutput, IncludeMap, IncludeExtraDef, IncludeMapDef } from './main';
import * as requireCwd from 'import-cwd';
import * as pg from 'pg';

export async function config(file): Promise<BuildConfig|BuildConfig[]> {
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

export async function write(config: BuildConfig): Promise<void> {
  const { models, output } = config;

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
  const client = await new pg.Client(config.pgconfig);
  try {
    await client.connect();

    for (let j = 0; j < models.length; j++) {
      model = models[j];

      model.extraTypes = [];
      model.extraImports = [];
      model.serverOuter = '';
      model.serverBody = '';
    
      for (let i = 0; i < model.queries.length; i++) {
        const q = model.queries[i];
        const res = processQuery(config, q);
        if (res.interface) model.extraTypes.push(res.interface);
        if (res.outer) model.serverOuter += res.outer + '\n';
        model.serverBody += '\n\n' + res.method;
        if (res.others.length) {
          res.others.forEach(o => {
            if (!~model.extraImports.indexOf(o)) model.extraImports.push(o);
          });
        };
        
        await client.query(`PREPARE __pg_dao_check_stmt AS ${res.sql}; DEALLOCATE __pg_dao_check_stmt;`);
      }

      const server = path.join(serverPath, model.name + '.ts');
      console.log(`\twriting server ${model.name} to ${server}...`);
      await fs.writeFile(server, serverModel(config, model));
      if (!pathy) {
        const client = path.join(clientPath, model.name + '.ts');
        console.log(`\twriting client ${model.name} to ${client}...`);
        await fs.writeFile(client, clientModel(config, model));
      }
    }
  } finally {
    await client.end();
  }

  if (config.index) {
    const tpl = models.map((m: ProcessModel) => `export { default as ${m.name}${m.extraTypes.length ? `, ${m.extraTypes.map(t => t[0]).join(', ')}` : ''} } from './${m.name}';`).join('\n');
    const server = path.join(serverPath, typeof config.index === 'string' ? config.index : 'index.ts');
    console.log(`\twriting server index to ${server}...`);
    await fs.writeFile(server, tpl);
    if (!pathy) {
      const client = path.join(clientPath, typeof config.index === 'string' ? config.index : 'index.ts');
      console.log(`\twriting client index to ${client}...`);
      await fs.writeFile(client, tpl);
    }
  }
}

const dates = ['timestamp', 'date'];
function serverModel(config: BuildConfig, model: ProcessModel): string {
  let tpl = `import * as dao from '@evs-chris/ts-pg-dao/runtime';${model.extraImports && model.extraImports.length ? '\n' + model.extraImports.map(o => `import ${o} from './${o}';`).join('\n') + '\n' : ''}${model.serverOuter ? `\n${model.serverOuter}` : ''}
export default class ${model.name} {
  static get table() { return ${JSON.stringify(model.table)}; }
`;

  const loadFlag = ((!model.flags.load && model.flags.load !== false) || model.flags.load) ? model.flags.load || '__loaded' : '';
  const changeFlag = ((!model.flags.change && model.flags.change !== false) || model.flags.change) ? model.flags.change || '__changed' : '';
  const removeFlag = ((!model.flags.remove && model.flags.remove !== false) || model.flags.remove) ? model.flags.remove || '__removed' : '';

  if (loadFlag) tpl += `  ${loadFlag}?: boolean;
`;
  if (changeFlag) tpl += `  ${changeFlag}?: boolean;
`;
  if (removeFlag) tpl += `  ${removeFlag}?: boolean;
`;

  tpl += modelProps(config, model);

  Object.keys(model.hooks).forEach(h => {
    if (typeof model.hooks[h] === 'function') {
      tpl += `  protected static ${h}: (item: ${model.name}) => void = ${reindent(model.hooks[h].toString(), '  ')}\n`;
    }
  });

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

    const transact = !con.inTransaction;
    if (transact) await con.begin();
    try {
      const params = [${model.fields.filter(f => f.pkey || f.optlock).map(f => `model.${f.name}`).join(', ')}];
      const res = await con.query('delete from "${model.table}" where ${model.fields.filter(f => f.pkey || f.optlock).map((f, i) => `"${f.name}" = $${i + 1}`).join(' AND ')}', params);
      if (res.rowCount < 1) throw new Error('No matching row to delete for ${model.name}');
      if (res.rowCount > 1) throw new Error('Too many matching rows deleted for ${model.name}');
    } catch (e) {
      if (transact) await con.rollback();
      throw e;
    }
  }
  
  ${model.queries.find(q => q.name === 'findById') ? '' : `static async findById(con: dao.Connection, ${model.pkeys.map(k => `${k.alias || k.name}: ${k.type}`).join(', ')}): Promise<${model.name}> {
    return await ${model.name}.findOne(con, '${model.pkeys.map((k, i) => `${k.name} = $${i + 1}`).join(' AND ')}', [${model.pkeys.map(k => k.alias || k.name).join(', ')}])
  }

  `}static async findOne(con: dao.Connection, where: string = '', params: any[] = []): Promise<${model.name}> {
    const res = await ${model.name}.findAll(con, where, params);
    if (res.length < 1) throw new Error('${model.name} not found')
    if (res.length > 1) throw new Error('Too many results found');
    return res[0];
  }

  static async findAll(con: dao.Connection, where: string = '', params: any[] = []): Promise<${model.name}[]> {
    const res = await con.query('select ${model.select()} from "${model.table}"' + (where ? ' WHERE ' + where : ''), params);
    return res.rows.map(r => ${model.name}.load(r, new ${model.name}()));
  }

  static keyString(row: any, prefix: string = ''): string {
    return '${model.table}' + ${model.pkeys.map(k => `'_' + (prefix ? row[prefix + '${k.name}'] : row.${k.name})`).join(' + ')}
  }

  static load(row: any, model: any, prefix: string = '', cache: dao.Cache = null): any {
    if (${model.pkeys.map(k => `row[prefix + ${JSON.stringify(k.name)}] == null`).join(' && ')}) return;
    if (cache && (model = cache[${model.name}.keyString(row, prefix)])) return model;
    if (!model) model = {};
    if (cache) cache[${model.name}.keyString(row, prefix)] = model;${loadFlag ? `
    model.${loadFlag} = true;` : ''}

`

  for (const f of model.cols) {
    tpl += `    if ((prefix + '${f.name}') in row) model.${f.alias || f.name} = prefix ? row[prefix + '${f.name}'] : row.${f.name};\n`
  }
    
  tpl += `    
    return model;
  }`;

  tpl += `${model.serverBody}\n}`;

  return tpl;  
}

function clientModel(config: Config, model: ProcessModel): string {
  let tpl = `${
    model.extraImports && model.extraImports.length ? model.extraImports.map(o => `import ${o} from './${o}';`).join('\n') + '\n' : ''
}${model.extraTypes && model.extraTypes.length ? '\n' + model.extraTypes.map(([n, t]) => `export type ${n} = ${t};`).join('\n') + '\n' : ''
}export default class ${model.name} {\n`;

  const loadFlag = ((!model.flags.load && model.flags.load !== false) || model.flags.load) ? model.flags.load || '__loaded' : '';
  const changeFlag = ((!model.flags.change && model.flags.change !== false) || model.flags.change) ? model.flags.change || '__changed' : '';
  const removeFlag = ((!model.flags.remove && model.flags.remove !== false) || model.flags.remove) ? model.flags.remove || '__removed' : '';

  if (loadFlag) tpl += `  ${loadFlag}?: boolean;
`;
  if (changeFlag) tpl += `  ${changeFlag}?: boolean;
`;
  if (removeFlag) tpl += `  ${removeFlag}?: boolean;
`;

  tpl += modelProps(config, model, true);

  tpl += `}`;

  return tpl;
}

function modelProps(config: Config, model: Model, client: boolean = false): string {
  let tpl = '';

  let col: Column;
  for (let c = 0; c < model.cols.length; c++) {
    col = model.cols[c];
    tpl += `  ${col.alias || col.name}${col.nullable ? '?' : ''}: ${col.type}${col.default ? ` = ${col.default}` : ''};\n`;
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
      res += `\n${prefix}if (model.hasOwnProperty('${f.alias || f.name}')) { params.push(${colToParam(f)}); sets.push('${f.name} = $' + params.length); }`;
    }
  });

  const locks = model.fields.filter(f => f.optlock);

  res += `\n${prefix}sql += sets.join(', ');\n${prefix}sql += \`\${${locks.length} && sets.length ? ', ' : ''}${locks.map(l => `${l.name} = now()`).join(', ')}\`;\n\n${prefix}const count = params.length;`;
  const where = model.fields.filter(f => f.pkey || f.optlock);
  res += `\n${prefix}params.push(${where.map(f => `model.${f.alias || f.name}`).join(', ')});`;
  res += `\n${prefix}sql += \` WHERE ${where.map((f, i) => `${f.optlock ? `date_trunc('millisecond', ${f.name})` : f.name} = $\${count + ${i + 1}}`).join(' AND ')}\`;`

  return res;
}

function insertMembers(model: Model, prefix: string): string {
  let res = `\n${prefix}const params = [];\n${prefix}const sets = [];\n${prefix}let sql = 'INSERT INTO ${model.table} ';`;
  model.fields.forEach(f => {
    if (!f.optlock) {
      res += `\n${prefix}if (model.hasOwnProperty('${f.alias || f.name}')) { params.push(${colToParam(f)}); sets.push('${f.name}'); }`;
      if (!f.elidable) res += `\n${prefix}else throw new Error('Missing non-elidable field ${f.alias || f.name}');`;
    }
  });

  const ret = model.fields.filter(f => f.pkey || f.optlock || (f.elidable && f.pgdefault));
  const locks = model.fields.filter(f => f.optlock);
  res += `\n${prefix}sql += \`(\${sets.join(', ')}\${${locks.length} && sets.length ? ', ' : ''}${locks.map(l => l.name).join(', ')}) VALUES (\${sets.map((n, i) => \`$\${i + 1}\`)}\${${locks.length} && sets.length ? ', ' : ''}${locks.map(l => 'now()').join(', ')}) RETURNING ${ret.map(f => f.name).join(', ')};\`;`;
  res += `\n\n${prefix}const res = (await con.query(sql, params)).rows[0];`
  res += ret.map(f => `\n${prefix}model.${f.alias || f.name} = res.${f.name};`).join('');

  return res;
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
}
function processQuery(config: Config, start: Query): ProcessQueryResult {
  const query = start as ProcessQuery;
  const parms = [];
  const aliases: AliasMap = query.aliases = {};
  let root: Alias;

  if (start.scalar && !start.owner.cols.find(c => c.name === start.scalar)) throw new Error(`Scalar query ${query.owner.name}.${query.name} must return a field from ${query.owner.table}.`);

  let sql = query.sql.replace(params, (str, name) => {
    let p, i;
    if (!query.params || !(p = query.params.find(p => p.name === name))) throw new Error(`Query ${query.owner.name}.${query.name} references parameter ${name} that is not defined.`);
    if (~(i = parms.indexOf(p))) return `$${i + 1}`;
    else {
      const rep = `$${parms.length + 1}`;
      parms.push(p);
      return rep;
    }
  });

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
  sql = sql.replace(fieldAliases, (m, alias, col) => {
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

  const defaulted = parms.reduce((a, c) => a && c.default, true);

  const loader = buildLoader(query);

  let outer = `const __${query.name}_sql = ${JSON.stringify(sql)};`;
  if (query.result) outer += `\nexport type ${query.result} = ${loader.interface};`

  const others = [];

  for (let k in aliases) {
    if (!aliases[k].root && !~others.indexOf(aliases[k].model.name)) others.push(aliases[k].model.name);
  }

  const res: ProcessQueryResult = {
    method: `  static async ${query.name}(con: dao.Connection, params: { ${parms.map(p => `${p.name}${p.default ? '?' : ''}: ${p.type || 'string'}`).join(', ')} }${defaulted ? ' = {}' : ''}): Promise<${query.result ? query.result : query.scalar ? loader.interface : query.owner.name}${query.singular ? '' : '[]'}> {
    const query = await con.query(__${query.name}_sql, [${parms.map(p => p.default ?
      `${defaulted ? 'params && ' : ''}${JSON.stringify(p.name)} in params ? params[${JSON.stringify(p.name)}] : ${p.default}` :
      `params[${JSON.stringify(p.name)}]`).join(', ')}]);
    ${loader.loader}
  }`,
    outer,
    others,
    sql
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

function buildTypes(query: ProcessQuery, alias: Alias): void {
  if (alias.type) return;
  let t = '', b = '';
  if (!alias.cols) t = alias.model.name;
  else b = `${alias.cols.map(c => `${c.alias || c.name}: ${c.type}`).join(', ')}`;

  if (alias.extra) b += `${b.length ? ', ' : ''}${alias.extra.map(e => `${e.name}: ${e.type}`).join(', ')}`;

  if (alias.include) {
    alias.include.forEach(a => buildTypes(query, query.aliases[a.name]));
    b += `${b.length ? ', ' : ''}${alias.include.map(i => `${i.name}: ${query.aliases[i.name].type}${i.type === 'many' ? '[]' : ''}`).join(', ')}`;
  }

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
    tpl += `
    if (res.length < 1) throw new Error('${query.owner.name} not found');
    if (res.length > 1) throw new Error('Too many ${query.owner.name} results found');
    return res[0];`;
  } else {
    tpl += `
    return res;`;
  }

  return { loader: tpl, interface: query.scalar ? query.owner.cols.find(c => c.name === query.scalar).type : query.root.type };
}

interface Depth { n: number };
function processLoader(query: ProcessQuery, alias: Alias, depth: Depth = { n: 0 }): string {
  const r = depth.n;
  let tpl = `
      const o${r || ''}: object = ${alias.model.name}.load(r, null, ${alias.prefix ? JSON.stringify(alias.prefix) : '\'\''}, cache);${!r ? '\n\n      ' : ''}`;
  if (alias.extra && alias.extra.length) {
    alias.extra.forEach(e => tpl += `o${r || ''}[${JSON.stringify(e.name)}] = r[${JSON.stringify(e.name)}];
      `);
  }
  if (alias.include && alias.include.length) {
    alias.include.forEach(i => {
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
    });
  }
  return tpl;
}

function reindent(fn: string, prefix: string): string {
  const lines = fn.split('\n');
  const indent = lines[lines.length - 1].replace(/^(\s*).*/, '$1');
  return fn.replace(new RegExp(`^${indent}`, 'gm'), prefix);
} 