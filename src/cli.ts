#!/usr/bin/env node
import * as cli from 'commander';
import * as path from 'path';
import * as rl from 'readline';
import * as fs from 'fs-extra';
import * as pg from 'pg';
import { config, write, ConfigOpts } from './run';
import { BuildConfig, tableQuery, columnQuery, ColumnSchema, commentQuery, enumQuery, SchemaCache, Types, functionQuery } from './main';
import { PatchOptions, patchConfig } from './patch';

const pkg = require(path.join(__dirname, '../package.json'));

async function findConfig(dir: string): Promise<string> {
  try {
    await fs.stat(path.join(dir, 'ts-pg-dao.config.ts'));
    return path.resolve(dir, 'ts-pg-dao.config.ts');
  } catch (e) {
    const next = path.join(dir, '..');
    if (next === dir) throw new Error('Could not find ts-pg-dao.config.ts');
    else return await findConfig(next);
  }
}

const commands: cli.Command[] = [];

cli
  .version(pkg.version)
  .option('-c, --config <file>', 'Config file to use, otherwise I\'ll search for a ts-pg-dao.config.ts');

commands.push(cli.command('build')
  .description('Build a DAO with the given config file')
  .option('-s, --skip-cache', 'Do not cache the target schema')
  .option('-n, --from-cache', 'Do not connect to an databases')
  .option('-o, --only <list>', 'Only run for the named configuration(s)', str => str.split(','))
  .action(async cmd => {
    const file = cli.config || await findConfig(path.resolve('.'));
    const opts: ConfigOpts = {};
    if (cmd.fromCache) opts.forceCache = true;
    const res = await config(file);
    const configs: BuildConfig[] = Array.isArray(res) ? res : [res];

    for (const config of configs) {
      if (cmd.only && !cmd.only.includes(config.config.name)) continue;
      if (cmd.fromCache) delete config.config.database;
      else if (cmd.skipCache) delete config.config.schemaCacheFile;
      await write(await config.read());
    }
  }));

export async function readSchema(connect: pg.ClientConfig) {
  const res: SchemaCache = { tables: [], functions: [] };
  try {
    const client = new pg.Client(connect);
    await client.connect();
    try {
      const allCols = (await client.query(columnQuery)).rows;
      const ts = (await client.query(tableQuery)).rows;
      for (const t of ts) {
        const cols: ColumnSchema[] = allCols.filter(c => c.schema === t.schema && c.table === t.name).map(c => Object.assign({}, c, { table: undefined, schema: undefined, length: c.length || undefined, precision: c.precision || undefined }));
        for (const col of cols) {
          if (!Types[col.type]) { // check for enums
            try {
              col.enum = (await client.query(enumQuery(col.type))).rows[0].values;
            } catch {}
          }
        }
        res.tables.push({ name: t.name, schema: t.schema, columns: cols });
      }
      res.functions = (await client.query(functionQuery)).rows;
    } finally {
      await client.end();
    }
  } catch (e) {
    console.error(`Failed to read schema`, e);
  }

  return res;
}

commands.push(cli.command('cache')
  .description('Manage cached schema')
  .option('-n, --name <name>', 'Use only the named config')
  .option('-t, --tables <list>', 'Only target the named table(s)', str => str.split(','))
  .option('-f, --functions <list>', 'Only target the named function(s)', str => str.split(','))
  .option('-u, --update', 'Update schema cache from the database')
  .option('-l, --list', 'List all the entries in the cache')
  .option('-q, --query', 'List all the entries in the database')
  .option('-m, --columns', 'List columns for entries in the cache')
  .option('-r, --remove', 'Remove the targeted entries from the cache')
  .option('-H, --host <host>', 'Override the target connection host for the named config.')
  .option('-U, --user <user>', 'Override the target connection user for the named config.')
  .option('-W, --password [password]', 'Read the target password from the stdin for the named config.')
  .option('-d, --database <database>', 'Override the target connection database for the named config.')
  .option('-p, --port <port>', 'Overide the target connection port for the named config.', parseInt)
  .option('-s, --ssl', 'Turn on (non-verified) ssl when connecting.')
  .action(async cmd => {
    if (typeof cmd.name === 'function') cmd.name = '';
    const file = cli.config || await findConfig(path.resolve('.'));
    const res = await config(file, { forceCache: cmd.forceCache });
    const configs: BuildConfig[] = Array.isArray(res) ? res : [res];

    for (const { config } of configs) {
      if (cmd.name && config.name !== cmd.name) continue;
      if (!config.schemaCacheFile) continue;

      if (cmd.list || cmd.query) console.log(`\n\n ${config.name} \n==============================`);

      const cache: SchemaCache = JSON.parse(await fs.readFile(config.schemaCacheFile, 'utf8'));
      const connect: pg.ClientConfig = Object.assign({}, config);
      if (cmd.host || cmd.user || cmd.password || cmd.database || cmd.port) {
        if (cmd.host) connect.host = cmd.host;
        if (cmd.database) connect.database = cmd.database;
        if (cmd.user) connect.user = cmd.user;
        if (cmd.port) connect.port = cmd.port;
        if (cmd.ssl) connect.ssl = { rejectUnauthorized: false };
        if (cmd.password === true) {
          const r = rl.createInterface({ input: process.stdin, output: process.stdout });
          connect.password = await new Promise(ok => r.question('Password: ', ok));
          r.close();
        } else if (cmd.password) {
          connect.password = cmd.password;
        }
      }

      if (cmd.list || cmd.query) {
        const schema = cmd.list ? { tables: cache.tables, functions: cache.functions } : await readSchema(connect);
        const tables = cmd.tables ? schema.tables.filter(t => cmd.tables.includes(t.name)) : schema.tables;
        if (tables.length) console.log(` Tables \n------------------------------`);
        for (const table of tables) {
          console.log(`"${table.schema}"."${table.name}"`);
          if (cmd.columns) console.log(`    ${table.columns.map(c => `${c.name}:${c.type}`).join(', ')}`);
        }
        const funcs = cmd.functions ? (schema.functions || []).filter(f => cmd.functions.includes(f.name)) : (schema.functions || []);
        if (funcs.length) console.log(`${tables.length ? '\n' : ''} Functions \n------------------------------`);
        for (const func of funcs) {
          console.log(`"${func.schema}"."${func.name}"(${func.args}): ${func.result}`);
        }
      }

      if (cmd.remove) {
        if (cmd.tables) {
          cache.tables = cache.tables.filter(t => {
            const found = cmd.tables.includes(t.name);
            if (found) console.log(`Removing "${t.schema}"."${t.name}"...`);
            return !found;
          });
        }

        if (cmd.functions) {
          cache.functions = (cache.functions || []).filter(f => {
            const found = cmd.functions.includes(f.name);
            if (found) console.log(`Removing "${f.schema}"."${f.name}"(${f.args}): ${f.result}...`);
              return !found;
          });
        }
      }

      if (cmd.update) {
        if (!cache.functions) cache.functions = [];
        const tables = cmd.tables ? cache.tables.filter(t => cmd.tables.includes(t.name)) : cache.tables;
        const functions = cmd.functions ? cache.functions.filter(f => cmd.functions.includes(f.name)) : cache.functions;
        const schema = await readSchema(connect);

        // update current tables
        for (const table of tables) {
          const t = schema.tables.find(t => t.name === table.name && t.schema === table.schema);
          if (t) {
            cache.tables[cache.tables.indexOf(table)] = t;
            console.log(`Updating "${table.schema}"."${table.name}"...`);
          } else console.log(`"${table.schema}"."${table.name}" not found in target database`);
        }

        // update current functions
        for (const func of functions) {
          const f = schema.functions.find(f => f.name === func.name && f.args === func.args && f.result === func.result);
          if (f) {
            cache.functions[cache.functions.indexOf(func)] = f;
            console.log(`Updating "${func.schema}"."${func.name}"(${func.args}): ${func.result}...`);
          } else console.log(`"${func.schema}"."${func.name}"(${func.args}): ${func.result} not found in target database`);
        }

        // look for new tables
        if (cmd.tables) {
          for (const t of cmd.tables) {
            const table = schema.tables.find(c => c.name === t);
            if (!cache.tables.find(c => c.name === t) && table) {
              console.log(`Adding "${table.schema}"."${table.name}"...`);
              cache.tables.push(table);
            }
          }
        }

        if (cmd.functions) {
          for (const n of cmd.functions) {
            const funcs = schema.functions.filter(f => f.name === n);
            for (const fn of funcs) {
              if (!cache.functions.find(f => f.schema === fn.schema && f.name === fn.name && f.args === fn.args && f.result === fn.result)) {
                console.log(`Adding "${fn.schema}"."${fn.name}"(${fn.args}): ${fn.result}...`);
                cache.functions.push(fn);
              }
            }
          }
        }

        // check for *
        if (cmd.tables && cmd.tables.length === 1 && cmd.tables[0] === '*') {
          for (const t of schema.tables) {
            if (!cache.tables.find(c => c.name === t.name)) {
              console.log(`Adding "${t.schema}"."${t.name}"...`);
              cache.tables.push(t);
            }
          }
        }

        if (cmd.functions && cmd.functions.length === 1 && cmd.functions[0] === '*') {
          for (const fn of schema.functions || []) {
            if (!cache.functions.find(f => f.schema === fn.schema && f.name === fn.name && f.args === fn.args && f.result === fn.result)) {
              console.log(`Adding "${fn.schema}"."${fn.name}"(${fn.args}): ${fn.result}...`);
              cache.functions.push(fn);
            }
          }
        }
      }

      if (cmd.update || cmd.remove) {
        cache.tables.sort((l, r) => l.name < r.name ? -1 : l.name > r.name ? 1 : 0);
        cache.tables.forEach(t => t.columns.sort((l, r) => l.name < r.name ? -1 : l.name > r.name ? 1 : 0));
        cache.functions.sort((l, r) => {
          const a = `"${l.schema}"."${l.name}"(${l.args}): ${l.result}`;
          const b = `"${r.schema}"."${r.name}"(${r.args}): ${r.result}`;
          return a < b ? -1 : a > b ? 1 : 0;
        });
        await fs.writeFile(config.schemaCacheFile, JSON.stringify(cache, null, ' '), 'utf8');
        console.log(`Wrote ${config.schemaCacheFile}`);
      }
    }
  }));

commands.push(cli.command('patch')
  .description('Generate SQL statements to patch the target database not fail with the cached schema')
  .option('--commit', 'Apply the changes to the target database')
  .option('-l, --details', 'Update columns to match type and default')
  .option('-n, --name <name>', 'Only use the named config')
  .option('-t, --tables <list>', 'Only target the named table(s)', str => str.split(','))
  .option('-f, --functions <list>', 'Only target the named function(s)', str => str.split(','))
  .option('-H, --host <host>', 'Override the target connection host for the named config.')
  .option('-U, --user <user>', 'Override the target connection user for the named config.')
  .option('-W, --password [password]', 'Read the target password from the stdin for the named config.')
  .option('-d, --database <database>', 'Override the target connection database for the named config.')
  .option('-p, --port <port>', 'Overide the target connection port for the named config.', parseInt)
  .option('-s, --ssl', 'Turn on (non-verified) ssl when connecting.')
  .option('-x, --force-cache', 'Force use of cached schema in configs.')
  .action(async cmd => {
    if (typeof cmd.name === 'function') cmd.name = '';
    const file = cli.config || await findConfig(path.resolve('.'));
    const res = await config(file, { forceCache: cmd.forceCache });
    const configs: BuildConfig[] = Array.isArray(res) ? res : [res];

    if (cmd.name) {
      const config = configs.find(c => c.config.name === cmd.name);
      const opts: PatchOptions = { details: cmd.details, commit: cmd.commit };
      if (config) {
        if (cmd.host || cmd.user || cmd.password || cmd.database || cmd.port) {
          opts.connect = Object.assign({}, config.config);
          if (cmd.host) opts.connect.host = cmd.host;
          if (cmd.database) opts.connect.database = cmd.database;
          if (cmd.user) opts.connect.user = cmd.user;
          if (cmd.port) opts.connect.port = cmd.port;
          if (cmd.ssl) opts.connect.ssl = { rejectUnauthorized: false };
          if (cmd.password === true) {
            const r = rl.createInterface({ input: process.stdin, output: process.stdout });
            opts.connect.password = await new Promise(ok => r.question('Password: ', ok));
            r.close();
          } else if (cmd.password) {
            opts.connect.password = cmd.password;
          }
          if (cmd.tables) opts.tables = cmd.tables;
          if (cmd.functions) opts.functions = cmd.functions;
        }
        await patchConfig(config.config, opts);
      } else {
        console.error(`No config named '${cmd.name}' found.`);
        process.exit(1);
      }
    } else {
      for (const config of configs) await patchConfig(config.config);
    }
  }));

const migration = /^([0-9]{4}\.[0-9]{2}\.[0-9]{2}\.[0-9]{4})([-\.].*sql)?/;
const migrations = /((?:[\r\n]|.)*)(# Migrations\n\=\=\=\n(?:[\r\n]|.)*)/m;
commands.push(cli.command('migrate <name> <path>')
  .description('Migrate the config or database identified by <name> using the migrations in the given <path>')
  .option('-i, --init [migration]', 'Initialize the migration tracker for the given database optionally setting all migrations up to [migration] in the format yyyy.mm.dd.hhmm as applied.')
  .option('-a, --applied <migration>', 'Set the given migration as applied.')
  .option('-r, --reapply <migration>', 'Execute the given migration again.')
  .option('--applied-to <migration>', 'Set any migrations that preceed the given migration as applied.')
  .option('-l, --list', 'List all migrations and their status')
  .action(async (name, p, cmd) => {
    const file = cli.config || await findConfig(path.resolve('.'));
    const res = await config(file);
    const configs: BuildConfig[] = Array.isArray(res) ? res : [res];

    let cfg = configs.find(c => c.config.name === name);
    if (!cfg) cfg = configs.find(c => c.config && c.config.database === name);

    if (!cfg) throw new Error(`No config matching '${name}' was found`);
    if (!cfg.config && cfg.config.database) throw new Error(`No valid connection is specified for '${name}'`);

    if (typeof cmd.init === 'string' && !migration.test(cmd.init)) throw new Error(`Init migration target not in proper format`);

    const files = (await fs.readdir(p)).filter(n => migration.test(n) && migration.exec(n)[2]);
    files.sort();

    const client = new pg.Client(cfg.config);

    try {
      await client.connect();
      let comment: string = ((await client.query(commentQuery, [cfg.config.database])).rows[0] || {}).comment;
      if (!comment) {
        if (cmd.init) {
          comment = `# Migrations\n===\n${files.filter(f => migration.exec(f)[1] <= cmd.init).map(f => migration.exec(f)[1]).join('\n')}`;
          await client.query(`comment on database "${cfg.config.database}" is '${comment.replace(/'/g, '\'\'')}';`);
        } else {
          throw new Error(`Database '${name}' migrations are not initialized`);
        }
      }

      if (!migrations.test(comment)) {
        if (cmd.init) {
          comment += `${comment ? '\n\n' : ''}# Migrations\n===\n${files.filter(f => migration.exec(f)[1] <= cmd.init).map(f => migration.exec(f)[1]).join('\n')}`;
          await client.query(`comment on database "${cfg.config.database}" is '${comment.replace(/'/g, '\'\'')}';`);
        } else {
          throw new Error(`Database '${name}' migrations are not initialized in comment '${comment}'`);
        }
      }

      const [, pre, section] = migrations.exec(comment);
      const ran = section.split('\n').filter(s => migration.test(s));
      ran.sort();

      if (cmd.applied) {
        if (files.find(f => f.startsWith(cmd.applied))) {
          ran.push(cmd.applied);
          ran.sort();
          await client.query(`comment on database "${cfg.config.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
          console.log(`Marked ${cmd.applied} as applied.`);
        } else {
          throw new Error(`No migration found matching '${cmd.applied}'`);
        }
      } else if (cmd.reapply) {
        if (!migration.test(cmd.reapply) || migration.exec(cmd.reapply)[2]) throw new Error(`Invalid migration target '${cmd.reapply}'`);
        const m = files.find(f => f.startsWith(cmd.reapply));
        if (m) {
          console.log(`${ran.includes(cmd.reapply) ? 're-' : ''}running migration`, m);
          await client.query('begin;');
          await client.query(await fs.readFile(path.join(p, m), { encoding: 'utf8' }));
          if (!ran.includes(cmd.reapply)) ran.push(`${migration.exec(m)[1]}`);
          ran.sort();
          await client.query(`comment on database "${cfg.config.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
          await client.query('commit;');
        } else {
          throw new Error(`No migration found matching '${cmd.reapply}'`);
        }
      } else if (cmd.appliedTo) {
        const start = ran.length;
        if (!migration.test(cmd.appliedTo) || migration.exec(cmd.appliedTo)[2]) throw new Error(`Invalid migration target '${cmd.appliedTo}'`);
        files.filter(f => migration.exec(f)[1] <= cmd.appliedTo).map(f => migration.exec(f)[1]).forEach(m => {
          ran.push(m);
        });
        if (start < ran.length) {
          ran.sort();
          await client.query(`comment on database "${cfg.config.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
          console.log(`Marked ${ran.length - start} migration${ran.length - start > 1 ? 's' : ''} as applied`);
        } else {
          console.log('No matching non-applied migrations found.');
        }
      } else if (cmd.list) {
        const fs = files.map(f => migration.exec(f)[1]);
        console.log(`Applied migrations:\n===\n${files.filter(f => ran.includes(migration.exec(f)[1])).join('\n')}\n\nNon-applied migrations:\n===\n${files.filter(f => !ran.includes(migration.exec(f)[1])).join('\n')}\n\nAdditional migrations in database:\n===\n${ran.filter(r => !fs.includes(r)).join('\n')}`);
      } else {
        const run = files.filter(f => !ran.includes(migration.exec(f)[1]));

        for (const m of run) {
          console.log(`running migration`, m);
          await client.query('begin;');
          await client.query(await fs.readFile(path.join(p, m), { encoding: 'utf8' }));
          ran.push(`${migration.exec(m)[1]}`);
          await client.query(`comment on database "${cfg.config.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
          await client.query('commit;');
        }
      }
    } finally {
      await client.end();
    }
  })
);

commands.push(cli.command('help [command]')
  .description('Print help for all commands')
  .action(cmd => {
    if (!cmd) {
      cli.outputHelp();
      commands.forEach(c => {
        console.log(`\n${c.name()}:`);
        c.outputHelp(s => s.split('\n').map(s => `\t${s}`).join('\n'));
      });
    } else {
      const tgt = commands.find(c => c.name() === cmd);
      if (tgt) {
        tgt.outputHelp();
      } else {
        console.log(`No command named '${cmd}' is available.`);
        cli.outputHelp();
      }
    }
  }));

cli.parse(process.argv);

process.on('unhandledRejection', err => {
  console.error(err);
  process.exit(1);
});
