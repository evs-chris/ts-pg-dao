import * as fs from 'fs-extra';
import * as ts from 'typescript';
import * as path from 'path';
import { BuildConfig, BuiltConfig, BuilderOptions, Config, Model, Column, Query, Param, Output, MergedOutput, IncludeMap, IncludeExtraDef, SchemaCache, tableQuery, columnQuery } from './main';
import * as requireCwd from 'import-cwd';
import * as pg from 'pg';

export interface ConfigOpts {
  forceCache?: boolean;
}

export async function config(file: string, opts: ConfigOpts = {}): Promise<BuildConfig|BuildConfig[]> {
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
      console.log(`    writing server ${model.name} to ${server}...`);
      await fs.writeFile(server, serverModel(config, model));
      if (!pathy) {
        const client = path.join(clientPath, (model.file || model.name) + '.ts');
        console.log(`    writing client ${model.name} to ${client}...`);
        await fs.writeFile(client, clientModel(config, model));
      }
    }

    if (client && config.pgconfig.schemaCacheFile) {
      const cache: SchemaCache = { tables: [] };
      const cfg = config.pgconfig;
      console.log(`     - generating schema cache`);

      const allCols = (await client.query(columnQuery)).rows;

      try {
        for (const tbl of (await client.query(tableQuery)).rows) {
          if (cfg.schemaInclude && !cfg.schemaInclude.includes(tbl.name)) continue;
          else if (cfg.schemaExclude && cfg.schemaExclude.includes(tbl.name)) continue;
          else if (!models.find(m => m.table === tbl.name) && !cfg.schemaFull) continue;
          else cache.tables.push({ name: tbl.name, schema: tbl.schema, columns: allCols.filter(c => c.schema === tbl.schema && c.table === tbl.name).map(c => Object.assign({}, c, { table: undefined, schema: undefined, length: c.length || undefined, precision: c.precision || undefined })) });
        }
      } catch (e) {
        console.error(`Error generating schema cache:`, e);
      }

      console.log(`     - writing schema cache ${cfg.schemaCacheFile}`);
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
    console.log(`    writing server index to ${server}...`);
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
      console.log(`    writing client index to ${client}...`);
      await fs.writeFile(client, tpl);
    }
  }
}

const dates = ['timestamp'];
function serverModel(config: BuiltConfig, model: ProcessModel): string {
  let tpl = `import * as dao from '@evs-chris/ts-pg-dao/runtime';${model.extraImports && model.extraImports.length ? '\n' + model.extraImports.map(o => `import ${o} from './${o}';`).join('\n') + '\n' : ''}${model._imports.length ? '\n' + model._imports.join(';\n') + '\n' : ''}${model.serverOuter ? `\n${model.serverOuter}` : ''}

const _saveHooks: Array<(model: ${model.name}, con: dao.Connection) => (Promise<void>|void)> = [];
const _deleteHooks: Array<(model: ${model.name}, con: dao.Connection) => (Promise<void>|void)> = [];

/** DAO model representing table ${model.table} */
export default class ${model.name} {
  constructor(props?: Partial<${model.name}>) {
    if (props) Object.assign(this, props);
  }
  static get table() { return ${JSON.stringify(model.table)}; }
  static get keys() { return ${JSON.stringify(model.pkeys.map(f => f.alias || f.name))}; }
  static get locks() { return ${JSON.stringify(model.fields.filter(f => f.optlock).map(f => f.alias || f.name))}; }
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

  tpl += `
  /** Add a function to be called before any ${model.name} is written to the database. */
  static onBeforeSave(hook: (model: ${model.name}, con: dao.Connection) => (Promise<void>|void)): void {
    _saveHooks.push(hook);
  }

  /** Add a function to be called before any ${model.name} is deleted from the database. */
  static onBeforeDelete(hook: (model: ${model.name}, con: dao.Connection) => (Promise<void>|void)): void {
    _deleteHooks.push(hook);
  }
`;

  if (hasPkey) {
    tpl += `
  /** Persist a ${model.name} to table ${model.table}. If the model has primary key and/or optimistic lock fields in place, this will perform an update. Otherwise, it will perform an insert. */
  static async save(con: dao.Connection, model: ${model.name}): Promise<${model.name}> {
    if (!model) throw new Error('Model is required');
    if (_saveHooks.length) for (const h of _saveHooks) await h(model, con);

    if (${model.fields.filter(f => f.pkey || f.optlock).map(f => `model.${f.name} !== undefined`).join(' && ')}) {${updateMembers(config, model, '      ')}

      let res: dao.QueryResult;
      const transact = !con.inTransaction;
      if (transact) await con.begin();
      try {
        res = await con.query(sql, params);
        if (res.rowCount < 1) ${(!model.fields.find(f => f.optlock) || !model.fields.find(f => f.pkey)) ? `throw new Error('No matching row to update for ${model.name}');` : `{
          const err = new Error('No matching row to update for ${model.name}');
          try {
            const rec = await ${model.name}.findOne(con, '${model.pkeys.map((k, i) => `${k.name} = $${i + 1}`).join(' AND ')}', [${model.pkeys.map(k => `model.${k.alias || k.name}`).join(', ')}], true);
            if (rec) {
              (err as any).daoFields = {};${model.fields.filter(f => f.optlock || f.pkey).map(f => `
              (err as any).daoFields['${f.alias || f.name}'] = rec['${f.alias || f.name}'];`).join('')}
            }
          } catch {}
          throw err;
        }`}
        if (res.rowCount > 1) throw new Error(\`Too many matching rows updated for ${model.name} (\${res.rowCount})\`);
        if (transact) await con.commit();${changeFlag ? `
        con.onCommit(() => model.${changeFlag} = false);` : ''}
      } catch (e) {
        if (transact) await con.rollback();
        throw e;
      }
      ${model.fields.find(f => f.optlock) ? `${model.fields.filter(f => f.optlock).map(f => `\n      model.${f.alias || f.name} = res.rows[0].${f.alias || f.name};`).join('')}` : ''}
    } else {${insertMembers(config, model, '      ')}${loadFlag ? `
      model.${loadFlag} = false;` : ''}${changeFlag ? `
      model.${changeFlag} = false;` : ''}
    }

    return model;
  }

  /** Delete a ${model.name} from table ${model.table}. The model must have primary key and optimisitic lock fields in place. */
  static async delete(con: dao.Connection, model: ${model.name}): Promise<void> {
    if (!model) throw new Error('Model is required');
    if (_deleteHooks.length) for (const h of _deleteHooks) await h(model, con);
    ${model.fields.filter(f => ~dates.indexOf(f.cast || f.pgtype)).map(f => `if (typeof model.${f.name} === 'string') model.${f.name} = new Date(model.${f.name});
    `).join('')}
    const transact = !con.inTransaction;
    if (transact) await con.begin();
    try {
      const params = [${model.fields.filter(f => f.pkey || f.optlock).map(f => `model.${f.name}`).join(', ')}];
      const res = await con.query(\`delete from "${model.table}" where ${model.fields.filter(f => f.pkey || f.optlock).map((f, i) => `${f.optlock ? `$\{params[${i}] == null ? \`"${f.name}" is null\` : \`date_trunc('millisecond', "${f.name}"${model.cast(config, f)})\`}` : `"${f.name}"`} = $${i + 1}`).join(' AND ')}\`, params);
      if (res.rowCount < 1) throw new Error('No matching row to delete for ${model.name}');
      if (res.rowCount > 1) throw new Error(\`Too many matching rows deleted for ${model.name} (\${res.rowCount})\`);
      if (transact) await con.commit();
    } catch (e) {
      if (transact) await con.rollback();
      throw e;
    }
  }

  /** Delete a ${model.name} from table ${model.table} using only its primary key${model.pkeys.length > 1 ? 's' : ''}. This ignores any optimistic locking. */
  static async deleteById(con: dao.Connection, ${model.pkeys.map(k => `${k.alias || k.name}: ${k.retype || k.type}`).join(', ')}): Promise<boolean> {
    const params = [${model.pkeys.map(k => k.alias || k.name).join(', ')}];
    if (_deleteHooks.length) {
      const model = await ${model.name}.findOne(con, '${model.pkeys.map((k, i) => `${k.name} = $${i + 1}`).join(' AND ')}', params);
      for (const h of _deleteHooks) await h(model, con);
    }
    const res = await con.query(\`delete from "${model.table}" where ${model.pkeys.map((k, i) => `"${k.name}" = $${i + 1}`).join(' AND ')}\`, params);
    if (res.rowCount !== 1) return false;
    return true;
  }
  
  ${model.queries.find(q => q.name === 'findById') ? '' : `static async findById(con: dao.Connection, ${model.pkeys.map(k => `${k.alias || k.name}: ${k.retype || k.type}`).join(', ')}, optional: boolean = false): Promise<${model.name}> {
    return await ${model.name}.findOne(con, '${model.pkeys.map((k, i) => `${k.name} = $${i + 1}`).join(' AND ')}', [${model.pkeys.map(k => k.alias || k.name).join(', ')}], optional)
  }
  `}
  ${model.pkeys.length ? `/** Load a new copy of a model as it exists currently in the database. */
  static async findCurrent(con: dao.Connection, model: ${model.name}): Promise<${model.name}> {
    return await ${model.name}.findOne(con, '${model.pkeys.map((k, i) => `${k.name} = $${i + 1}`).join(' AND ')}', [${model.pkeys.map(k => `model.${k.alias || k.name}`).join(', ')}]);
  }
  ` : ''}
  /** Generate a string that can represent this model in a map using a given prefix. */
  static keyString(row: any, prefix: string = ''): string {
    return '${model.table}' + ${model.pkeys.map(k => `'_' + (prefix ? row[prefix + '${k.name}'] : row.${k.name})`).join(' + ')}
  }`;
  } else {
    tpl += `
  /** Insert a ${model.name} into table ${model.table}. */
  static async insert(con: dao.Connection, model: ${model.name}): Promise<${model.name}> {
    if (!model) throw new Error('Model is required');
    if (_saveHooks.length) for (const h of _saveHooks) await h(model, con);
    ${model.fields.filter(f => ~dates.indexOf(f.cast || f.pgtype)).map(f => `if (typeof model.${f.name} === 'string') model.${f.name} = new Date(model.${f.name});
    `).join('')}
    ${insertMembers(config, model, '    ')}${loadFlag ? `
    model.${loadFlag} = false;` : ''}
    return model;
  }
  `
  }

  tpl += `
  /** Retrieve exactly one ${model.name} from the table ${model.table}. This will reject if more than one result is returned.
   * @param where - sql clause *excluding* \`where\` to filter results
   * @params - an array of values to pass with the where clause where the first element corresponds to \$1 in clause and the second corresponds to \$2, etc
   * @param optional - if true will not reject if no rows are returned
   */
  static async findOne(con: dao.Connection, where: string = '', params: any[] = [], optional: boolean = false): Promise<${model.name}> {
    const res = await ${model.name}.findAll(con, where, params);
    if (res.length < 1 && !optional) throw new Error('${model.name} not found')
    if (res.length > 1) throw new Error(\`Too many results found for ${model.name} (\${res.length})\`);
    return res[0];
  }

  /** Retrieve zero or more ${model.name} from the table ${model.table}.
   * @param where - sql clause *excluding* \`where\` to filter results
   * @params - an array of values to pass with the where clause where the first element corresponds to \$1 in clause and the second corresponds to \$2, etc
   */
  static async findAll(con: dao.Connection, where: string = '', params: any[] = []): Promise<${model.name}[]> {
    const res = await con.query('select ${model.select(config)} from "${model.table}"' + (where ? ' WHERE ' + where : ''), params);
    return res.rows.map(r => ${model.name}.load(r, new ${model.name}()));
  }

  /** Populates ${model.name} from a result row, optionally using a cache to ensure that the same model is returned if the underlying row exists multiple times in the result set.
   * @param model - the model to populate. If not provided, this will start with a new empty object.
   * @param prefix - field prefix for model columns in the given row
   * @param cache - a model cache, that if given, will be checked for an existing model matching the given row, and if none exists, will be used to store the result of this call
   */
  static load(row: any, model?: any, prefix: string = '', cache: dao.Cache = null): any {
    ${hasPkey ? `if (${model.pkeys.map(k => `row[prefix + ${JSON.stringify(k.name)}] == null`).join(' && ')}) return;
    if (cache && (model = cache[${model.name}.keyString(row, prefix)])) return model;
    ` : ''}if (!model) model = {};${hasPkey ? `
    if (cache) cache[${model.name}.keyString(row, prefix)] = model;` : ''}${loadFlag ? `
    model.${loadFlag} = true;` : ''}

`

  for (const f of model.cols) {
    tpl += `    if (\`\${prefix}${f.name}\` in row) model.${f.alias || f.name} = row[\`\${prefix}${f.name}\`];\n`
    if (f.cast === 'date' || f.pgtype === 'date') tpl += `    if (model.${f.alias || f.name} instanceof Date) { const d = model.${f.alias || f.name}; model.${f.alias || f.name} = \`\${('' + d.getFullYear()).padStart(4, '0')}-\${d.getMonth() < 9 ? '0' : ''}\${(d.getMonth() + 1 + '')}-\${d.getDate() < 10 ? '0' : ''}\${(d.getDate())}T00:00\`; }\n`;
    if (f.trim) tpl += `    if (typeof model.${f.alias || f.name} === 'string') model.${f.alias || f.name} = model.${f.alias || f.name}.trim();\n`
  }
    
  tpl += `    
    return model;
  }`;

  tpl += `

  /** Ensures that all of the strings in the model fit within the bounds of the underlying column in the ${model.table} table. */
  static truncateStrings(model: ${model.name}) {`;
  const strs = ['char', 'varchar', 'bpchar'];
  for (const c of model.cols) {
    if (c.length && strs.includes(c.pgtype) && !c.retype) {
      tpl += `\n    if (model.${c.alias || c.name} && model.${c.alias || c.name}.length > ${c.length}) model.${c.alias || c.name} = model.${c.alias || c.name}.slice(0, ${c.length});`;
    }
  }
  tpl += `
  }`;

  tpl += stripDates(config, model);

  tpl += `${model.serverBody}\n`;

  if (model.codeMap.serverInner) tpl += `\n\n${reindent(model.codeMap.serverInner, '  ')}\n`;
  if (model.codeMap.bothInner) tpl += `\n\n${reindent(model.codeMap.bothInner, '  ')}\n`;

  tpl += '}';

  if (model.codeMap.serverOuter) tpl += `\n\n${model.codeMap.serverOuter}\n`;
  if (model.codeMap.bothOuter) tpl += `\n\n${model.codeMap.bothOuter}\n`;

  for (const [name, hook] of Object.entries(model.hooks)) {
    if (typeof hook === 'function') {
      tpl += `${model.name}.${name === 'beforesave' ? 'save' : 'delete'}Hooks.push(${reindent(hook.toString(), '  ')})\n`;
    }
  }

  return tpl;  
}

function clientModel(config: Config, model: ProcessModel): string {
  let tpl = `${
    model.extraImports && model.extraImports.length ? model.extraImports.map(o => `import ${o} from './${o}';`).join('\n') + '\n' : ''
}${model._imports.length ? '\n' + model._imports.join(';\n') + '\n' : ''}${model.extraTypes && model.extraTypes.length ? '\n' + model.extraTypes.map(([n, t]) => `export type ${n} = ${t};`).join('\n') + '\n' : ''
}export default class ${model.name} {\n
  constructor(props?: Partial<${model.name}>) {
    if (props) Object.assign(this, props);
  }\n`;

  const hasPkey = model.pkeys.length > 0;

  const loadFlag = ((!model.flags.load && model.flags.load !== false) || model.flags.load) ? model.flags.load || '__loaded' : '';
  const changeFlag = hasPkey && ((!model.flags.change && model.flags.change !== false) || model.flags.change) ? model.flags.change || '__changed' : '';
  const removeFlag = hasPkey && ((!model.flags.remove && model.flags.remove !== false) || model.flags.remove) ? model.flags.remove || '__removed' : '';

  if (loadFlag) tpl += `  /** This field is automatically set to true when the DAO populates an instance from the database. */\n  ${loadFlag}?: boolean;
`;
  if (changeFlag) tpl += `  /** This field can be used to indicate that this model has been modified and needs to be saved. It is automatically set to false when the model is successfully persisted to the database. */\n  ${changeFlag}?: boolean;
`;
  if (removeFlag) tpl += `  /** This field can be used to indicate that this model should be removed from the database. */\n  ${removeFlag}?: boolean;
`;

  tpl += modelProps(config, model, true);

  if (model.codeMap.clientInner) tpl += `\n\n${reindent(model.codeMap.clientInner, '  ')}\n`;
  if (model.codeMap.bothInner) tpl += `\n\n${reindent(model.codeMap.bothInner, '  ')}\n`;

  tpl += `
  /** A map of column name to length for string columns in table ${model.table} */
  static readonly lengths = { ${model.cols.filter(c => c.length).map(c => `${c.alias || c.name}: ${c.length}`).join(', ')} };
  /** A map of column name to [scale, precision] for numeric columsn in table ${model.table} */
  static readonly precisions = { ${model.cols.filter(c => c.precision).map(c => `${c.alias || c.name}: ${JSON.stringify(c.precision)}`).join(', ')} }
  /** A map of all columns in table ${model.table}. This is reconstituted from the DAO configuration. */
  static readonly columns: { [column: string]: {
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
    type: string;
    retype?: string;
    array?: boolean;
    cast?: string;
    json?: true;
    optlock?: boolean;
    enum?: string[];
    trim?: boolean;
    length?: number;
    precision?: [number, number];
  } } = { ${model.cols.map(c => `"${c.alias || c.name}": ${JSON.stringify(c)}`).join(', ')} };
`;

  tpl += stripDates(config, model);

  tpl += `}`;

  if (model.codeMap.clientOuter) tpl += `\n\n${model.codeMap.clientOuter}\n`;
  if (model.codeMap.bothOuter) tpl += `\n\n${model.codeMap.bothOuter}\n`;

  return tpl;
}

function stripDates(config: BuiltConfig, model: ProcessModel): string {
  let children: string[] = [];
  for (const k in model._extras) {
    let ck = `if ('${k}' in model && model['${k}']) `;
    const extra = model._extras[k];
    let e = typeof extra === 'object' ? extra.type : extra;
    const arr = e.endsWith('[]');
    e = arr ? e.slice(0, -2) : e;
    if (!config.models.find(m => m.name === e)) continue;
    if (arr) ck += `model['${k}'].forEach(c => ${e}.stripDates(c));`
    else ck += `${e}.stripDates(model['${k}']);`
    children.push(ck);
  }
  return `
  /** Stringify dates so that they persist as entered without possible skew by timezone or mangle during JSON stringification. */
  static stripDates(model: ${model.name}) {${model.cols.filter(c => (c.cast || c.pgtype) === 'date').map(c => `
    if ((model.${c.alias || c.name} as any) instanceof Date) { const d = model.${c.alias || c.name} as any as Date; model.${c.alias || c.name} = \`\${('' + d.getFullYear()).padStart(4, '0')}-\${d.getMonth() < 9 ? '0' : ''}\${d.getMonth() + 1}-\${d.getDate() < 10 ? '0' : ''}\${d.getDate()}\` as any; }`).join('')}${
    children.length ? `
    ${children.join('\n    ')}` : ''}
  }
  `;
}

function stringyDates(config: Config, type: string): string {
  if (!config.stringyDates) return type;
  if (type === 'Date') return 'Date|string'
  if (type === 'Date[]') return 'Array<Date|string>';
  return type;
}
function modelProps(config: Config, model: Model, _client: boolean = false): string {
  let tpl = '';

  let col: Column;
  for (let c = 0; c < model.cols.length; c++) {
    col = model.cols[c];
    tpl += `  /** ${col.pkey ? 'Primary key column' : col.optlock ? 'Optimistic lock column' : 'Field linked to column'} ${col.name} (${col.pgtype}${col.length != null ? `(${col.length})` : col.precision ? `(${col.precision.join(', ')})`: ''} ${col.nullable ? '' : 'not '}nullable${col.default ? ` default ${col.default}` : ''}) in table ${model.table} */\n  ${col.alias || col.name}${col.nullable ? '?' : ''}: ${col.enum ? col.enum.map(v => `'${v}'`).join('|') : stringyDates(config, col.retype || col.type)}${col.default ? ` = ${col.default}` : ''};\n`;
  }

  if (Object.keys(model._extras).length > 0) {
    tpl += '\n';
    for (const [field, extra] of Object.entries(model._extras)) {
      tpl += `${typeof extra === 'object' && extra.comment ? `  /** ${extra.comment} */\n` : ''}  ${field}?: ${typeof extra === 'object' ? extra.type : extra};\n`;
    }
  }

  return tpl;
}

function colToParam(f: Column) {
  return `${f.optlock ? 'new Date()' : f.type === 'any' ? `Array.isArray(model.${f.alias || f.name}) ? JSON.stringify(model.${f.alias || f.name}) : model.${f.alias || f.name}` : `model.${f.alias || f.name}`}`;
}

function updateMembers(config: Config, model: Model, prefix: string): string {
  let res = `\n${prefix}const params = [];\n${prefix}const sets = [];\n${prefix}let sql = 'UPDATE ${model.table} SET ';`;
  model.fields.forEach(f => {
    if (!f.pkey && !f.optlock) {
      res += setParam(f, prefix, `sets.push('"${f.name}" = $' + params.length${f.pgtype === 'timestamp' && config.tzTimestamps ? ` + '::timestamptz'` : ''})`);
    }
  });

  const locks = model.fields.filter(f => f.optlock);

  if (locks.length) {
    res += `\n${locks.map(l => `${prefix}sets.push('"${l.name}" = now()');`).join('\n')}`;
  }
  res += `\n\n${prefix}sql += sets.join(', ');\n`;
  res += `\n${prefix}let count = params.length;`;
  const keys = model.fields.filter(f => f.pkey);
  const keystrs = keys.map(f => `"${f.name}" = $\${++count}`);
  const lockstrs = locks.map(f => `\${model.${f.name} === null ? \`"${f.name}" is null\` : \`date_trunc('millisecond', "${f.name}"${model.cast(config, f)}) = $\${++count}\`}`);
  res += `\n${prefix}params.push(${keys.map(f => `model.${f.alias || f.name}`).join(', ')});`;
  res += locks.map(f => `\n${prefix}if (model.${f.name} !== null) params.push(model.${f.name});`).join('');
  res += `\n${prefix}sql += \` WHERE ${keystrs.concat(lockstrs).join(' AND ')}${locks.length ? ` RETURNING ${locks.map(l => `"${l.name}"${model.cast(config, l)}`).join(', ')}`: ''}\`;`

  return res;
}

function insertMembers(config: Config, model: Model, prefix: string): string {
  let res = `\n${prefix}const params = [];\n${prefix}const sets = [];\n${prefix}let sql = 'INSERT INTO ${model.table} ';\n${prefix}let casts = {};`;
  for (const f of model.fields) {
    if (!f.optlock) {
      res += setParam(f, prefix, `sets.push('${f.name}')${f.pgtype === 'timestamp' && config.tzTimestamps ? `; casts['${f.name}'] = '::timestamptz'` : ''}`);
      if (!f.elidable) res += `\n${prefix}else throw new Error('Missing non-elidable field ${f.alias || f.name}');`;
    }
  }

  const ret = model.fields.filter(f => f.pkey || f.optlock || (f.elidable && f.pgdefault));
  const locks = model.fields.filter(f => f.optlock);
  res += `\n${prefix}sql += \`(\${sets.map(s => \`"\${s}"\`).join(', ')}\${${locks.length} && sets.length ? ', ' : ''}${locks.map(l => l.name).join(', ')}) VALUES (\${sets.map((n, i) => \`$\${i + 1}\${casts[n] || ''}\`)}\${${locks.length} && sets.length ? ', ' : ''}${locks.map(_l => 'now()').join(', ')})${ret.length ? ` RETURNING ${ret.map(f => `"${f.name}"${model.cast(config, f)}`).join(', ')}` : ''};\`;`;
  res += `\n\n${prefix}const res = (await con.query(sql, params)).rows[0];`
  res += ret.map(f => `\n${prefix}model.${f.alias || f.name} = res.${f.name};`).join('');

  return res;
}

function setParam(col: Column, prefix: string, set: string, model: string = 'model', params: string = 'params'): string {
  let cond = `\n${prefix}if ('${col.alias || col.name}' in ${model}) { `
  if (col.pgtype !== 'char' && col.pgtype !== 'varchar' && col.pgtype !== 'bpchar' && col.pgtype !== 'text') {
    if (col.nullable) {
      cond += `\n${prefix}  if ((${model}.${col.alias || col.name} as any) === '') { ${params}.push(null); ${set}; }\n${prefix}  else { ${params}.push(${colToParam(col)}); ${set}; }\n${prefix}`;
    } else if (col.default) {
      cond += `\n${prefix}  if ((${model}.${col.alias || col.name} as any) === '') { ${params}.push(${col.default}); ${set}; }\n${prefix}  else { ${params}.push(${colToParam(col)}); ${set}; }\n${prefix}`;
    } else if (col.pgtype.slice(0, 3) === 'int' || col.pgtype === 'numeric' || col.pgtype.slice(0, 5) === 'float') {
      cond += `\n${prefix}  if ((${model}.${col.alias || col.name} as any) === '') { ${params}.push(0); ${set}; }\n${prefix}  else { ${params}.push(${colToParam(col)}); ${set}; }\n${prefix}`;
    } else {
      cond += `${params}.push(${colToParam(col)}); ${set}; `;
    }
  } else {
    cond += `${params}.push(${colToParam(col)}); ${set}; `;
  }
  cond += '}';

  return cond;
}

const tableAliases = /@"?([a-zA-Z_]+[a-zA-Z0-9_]*)"?(?!\.)\b(?:(?!\s+(?:left|right|cross|inner|outer|on|where|join|union)\b)\s*(?:[aA][sS]\s)?\s*"?([a-zA-Z_]+[a-zA-Z0-9_]*)?"?)?/gi;
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

    const str = sql.replace(params, (_str, name) => {
      let p: Param, i: number;
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
        const s = sql.replace(params, (_str, name) => {
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
  const referencedTables: { [name: string]: number } = {};

  // map tables to models and record relevant aliases
  sql = sql.replace(tableAliases, (_m ,tbl, alias) => {
    const mdl = findModel(tbl, config);
    if (!mdl) throw new Error(`Could not find model for table ${tbl} referenced in query ${query.name}.`);

    if (!referencedTables[tbl]) referencedTables[tbl] = 0;
    referencedTables[tbl]++;

    const entry = { model: mdl, prefix: `${alias || tbl}__`, alias: alias || tbl, root: !alias && mdl === query.owner };
    if (entry.root) query.root = root = entry;
    aliases[alias || tbl] = entry;
    return `${tbl}${alias ? ` AS ${alias}` : ''} `;
  });

  const tableCount = Object.keys(referencedTables).length;

  if (!tableCount && (query.owner.cols.find(c => c.cast) || (config.tzTimestamps && query.owner.cols.find(c => c.pgtype === 'timestamp'))) ) {
    console.warn(`      >>>> query ${query.owner.name}.${query.name} may have columns that need to be cast\n        >> using something like 'select @${model.table[0]}.* from @${model.table} as ${model.table[0]};' will automtaically cast columns`);
  }

  if (!root) {
    const owners: AliasMap = {};
    for (const k in aliases) {
      if (aliases[k].model === query.owner) owners[k] = aliases[k];
    }
    const keys = Object.keys(owners);
    if (keys.length === 1) {
      owners[keys[0]].root = true;
      query.root = root = owners[keys[0]];
    } else {
      query.root = root = { model: query.owner, prefix: '', alias: query.owner.table, root: true };
      aliases[root.alias] = root;
    }
  }

  if (root && tableCount === 1) root.prefix = '';

  // compute includes and select lists if available
  if (query.include && root) {
    buildIncludes(query, root, query.include);
  }

  buildTypes(query, root);

  const referencedCols: { [table: string]: string[] } = {};

  // map and expand column aliases as necessary
  function mapFields(sql: string): string {
    return sql.replace(fieldAliases, (m, alias, col) => {
      let entry = aliases[alias];
      let mdl: Model;
      if (entry) mdl = entry.model;
      else {
        throw new Error(`Query ${query.owner.name}.${query.name} requires an entry for ${alias} before its use in column ref ${alias}.${col}.`);
      }

      if (!referencedCols[mdl.table]) referencedCols[mdl.table] = [];
      referencedCols[mdl.table].push(col);

      if (col === '*') {
        return (entry.cols || entry.model.cols).map(c => tableCount === 1 ? `${c.name}${mdl.cast(config, c)}` : `${alias}.${c.name}${mdl.cast(config, c)} AS ${entry.prefix}${c.name}`).join(', ');
      } else {
        const c = mdl.fields.find(f => col === f.name);
        if (!c) throw new Error(`Could not find field for ${alias}.${col} referenced in query ${query.name}.`);

        if (~m.indexOf(':')) {
          return `${entry.prefix}${c.name}`;
        } else {
          return `${alias}.${c.name}${mdl.cast(config, c)} AS ${entry.prefix}${c.name}`;
        }
      }
    });
  }

  sql = mapFields(sql);

  for (const k in aliases) {
    const alias = aliases[k];
    const table = alias.model.table;
    if (!referencedTables[table]) continue;
    if (!referencedCols[table]) console.warn(`      >>>> table ${table} in ${query.owner.name}.${query.name} may have columns that need to be cast\n        >> referencing the columns as '@${alias.alias}.columnname' or '@${alias.alias}.*' will automatically cast columns`);
  }

  const defaulted = (query.params || []).reduce((a, c) => a && (c.optional || !!c.default), true);

  query.parts && Object.entries(query.parts).forEach(([condition, sql]) => {
    const ps: Param[] = [];
    const [str, check] = mapParams(mapFields(sql), ps, 'ps.length');
    const cond = condition.replace(params, (_str, name) => {
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
    method: `  static async ${query.name}(con: dao.Connection, params: { ${(query.params || []).map(p => `${p.name}${p.optional || p.default ? '?' : ''}: ${p.type || 'string'}`).join(', ')} }${defaulted ? ' = {}' : ''}): Promise<${query.result ? query.result : query.scalar ? loader.interface : start.retype ? start.retype : query.owner.name}${query.singular ? '' : '[]'}> {
    let sql = __${query.name}_sql;
    const ps: any[] = [${getParamsValues(parms)}];
    ${parts.map((p, _i) => p.code).join('\n    ')}
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
    if (res.length > 1) throw new Error(\`Too many ${query.owner.name} results found (\${res.length})\`);
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
    res.push(sql + parts.filter((_p, j) => (i + 1) & (j + 1)).map(p => {
      const [sql, count] = p(num);
      num += count;
      return sql;
    }).join(''));
  }
  return res;
}
