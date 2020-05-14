#!/usr/bin/env node
import * as cli from 'commander';
import * as path from 'path';
import * as rl from 'readline';
import * as fs from 'fs-extra';
import * as pg from 'pg';
import { config, write, ConfigOpts } from './run';
import { BuildConfig, tableQuery, columnQuery, commentQuery, enumQuery, SchemaCache, Types } from './main';

const pkg = require(path.join(__dirname, '../package.json'));

async function findConfig(dir): Promise<string> {
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

commands.push(cli.command('patch')
  .description('Generate SQL statements to patch the target database not fail with the cached schema')
  .option('--commit', 'Apply the changes to the target database')
  .option('-l, --details', 'Update columns to match type and default')
  .option('-n, --name <name>', 'Only use the named config')
  .option('-t, --tables <list>', 'Only target the named table(s)', str => str.split(','))
  .option('-H, --host <host>', 'Override the target connection host for the named config.')
  .option('-U, --user <user>', 'Override the target connection user for the named config.')
  .option('-W, --password [password]', 'Read the target password from the stdin for the named config.')
  .option('-d, --database <database>', 'Override the target connection database for the named config.')
  .option('-p, --port <port>', 'Overide the target connection port for the named config.', parseInt)
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
          if (cmd.password === true) {
            const r = rl.createInterface({ input: process.stdin, output: process.stdout });
            opts.connect.password = await new Promise(ok => r.question('Password: ', ok));
            r.close();
          } else if (cmd.password) {
            opts.connect.password = cmd.password;
          }
          if (cmd.tables) opts.tables = cmd.tables;
        }
        await patchConfig(config, opts);
      } else {
        console.error(`No config named '${cmd.name}' found.`);
        process.exit(1);
      }
    } else {
      for (const config of configs) await patchConfig(config);
    }
  }));

interface PatchOptions {
  details?: boolean;
  commit?: boolean;
  tables?: string[];
  connect?: pg.ClientConfig & { schemaCacheFile?: string };
}
async function patchConfig(config: BuildConfig, opts: PatchOptions = {}) {
  const connect = opts.connect || config.config;
  if (connect && connect.schemaCacheFile && connect.database) {
    const qs: string[] = [];
    const client = new pg.Client(connect);
    await client.connect();
    const cache: SchemaCache = JSON.parse(await fs.readFile(connect.schemaCacheFile, { encoding: 'utf8' }));
    const schema: SchemaCache = { tables: [] };

    const name = config.config.name ? `${config.config.name} (${connect.user || process.env.USER}@${connect.host || 'localhost'}:${connect.port || 5432}/${connect.database || process.env.USER})` : `${connect.user || process.env.USER}@${connect.host || 'localhost'}:${connect.port || 5432}/${connect.database || process.env.USER})`;

    try {
      const cfg = config.config;
      const built = await config.read();
      for (const tbl of (await client.query(tableQuery)).rows) {
        if (cfg.schemaInclude && !cfg.schemaInclude.includes(tbl.name)) continue;
        else if (cfg.schemaExclude && cfg.schemaExclude.includes(tbl.name)) continue;
        else if (!built.models.find(m => m.table === tbl.name) && !cfg.schemaFull) continue;
        else schema.tables.push({ name: tbl.name, schema: tbl.schema, columns: (await client.query(columnQuery, [tbl.schema, tbl.name])).rows });
      }

      for (const ct of cache.tables) {
        if (opts.tables && !opts.tables.includes(ct.name)) continue;
        const t = schema.tables.find(e => e.name === ct.name && e.schema === ct.schema);
        if (!t) {
          qs.push(`create table "${ct.name}" (${ct.columns.map(c => `"${c.name}" ${c.type}${c.nullable ? '' : ' not null'}${c.pkey ? ' primary key' : ''}${c.default ? ` default ${c.default}` : ''}`).join(', ')});`);
        } else {
          for (const col of ct.columns) {
            const c = t.columns.find(e => e.name === col.name);
            if (!c) {
              qs.push(`alter table "${ct.name}" add column "${col.name}" ${col.type}${col.nullable ? '' : ' not null'}${col.default ? ` default ${col.default}` : ''};`);
            } else if (opts.details && (c.default !== col.default || c.nullable !== col.nullable || c.type !== col.type)) {
              if (c.type !== col.type) qs.push(`alter table "${ct.name}" alter column "${col.name}" type ${col.type};`);
              if (c.default !== col.default) qs.push(`alter table "${ct.name}" alter column "${col.name}" ${col.default ? 'set' : 'drop'} default${col.default ? ` ${col.default}` : ''};`);
              if (c.nullable !== col.nullable) qs.push(`alter table "${ct.name}" alter column "${col.name}" ${col.nullable ? 'drop' : 'set'} not null;`);
            }
          }
        }
      }

      if (qs.length) {
        console.log(`\nPatches for ${name}`);
        console.log(qs.join('\n'));
        if (opts.commit) {
          await client.query(['begin;'].concat(qs).concat('commit;').join('\n'));
        }
      } else {
        console.log(`\nNo patches needed for ${name}`);
      }
    } finally {
      await client.end();
    }
  }
}

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
