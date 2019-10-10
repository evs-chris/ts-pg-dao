#!/usr/bin/env node
import * as cli from 'commander';
import * as path from 'path';
import * as fs from 'fs-extra';
import * as pg from 'pg';
import { config, write } from './run';
import { BuildConfig, tableQuery, columnQuery, commentQuery, SchemaCache } from './main';

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
  .option('-n, --skip-cache', 'Do not cache the target schema')
  .action(async cmd => {
    const file = cli.config || await findConfig(path.resolve('.'));
    const res = await config(file);
    const configs: BuildConfig[] = Array.isArray(res) ? res : [res];

    for (const config of configs) {
      if (cmd.skipCache) delete config.pgconfig.schemaCacheFile;
      await write(config);
    }
  }));

commands.push(cli.command('patch')
  .description('Generate SQL statements to patch the target database not fail with the cached schema')
  .option('--commit', 'Apply the changes to the target database')
  .option('-d, --details', 'Update columns to match type and default')
  .action(async cmd => {
    const file = cli.config || await findConfig(path.resolve('.'));
    const res = await config(file);
    const configs: BuildConfig[] = Array.isArray(res) ? res : [res];

    for (const config of configs) {
      if (config.pgconfig && config.pgconfig.schemaCacheFile && config.pgconfig.database) {
        const qs: string[] = [];
        const client = new pg.Client(config.pgconfig);
        await client.connect();
        const cache: SchemaCache = JSON.parse(await fs.readFile(config.pgconfig.schemaCacheFile, { encoding: 'utf8' }));
        const schema: SchemaCache = { tables: [] };

        try {
          const cfg = config.pgconfig;
          for (const tbl of (await client.query(tableQuery)).rows) {
            if (cfg.schemaInclude && !cfg.schemaInclude.includes(tbl.name)) continue;
            else if (cfg.schemaExclude && cfg.schemaExclude.includes(tbl.name)) continue;
            else if (!config.models.find(m => m.table === tbl.name) && !cfg.schemaFull) continue;
            else schema.tables.push({ name: tbl.name, schema: tbl.schema, columns: (await client.query(columnQuery, [tbl.schema, tbl.name])).rows });
          }

          for (const ct of cache.tables) {
            const t = schema.tables.find(e => e.name === ct.name && e.schema === ct.schema);
            if (!t) {
              qs.push(`create table "${ct.name}" (${ct.columns.map(c => `"${c.name}" ${c.type}${c.nullable ? '' : ' not null'}${c.pkey ? ' primary key' : ''}${c.default ? ` default ${c.default}` : ''}`).join(', ')});`);
            } else {
              for (const col of ct.columns) {
                const c = t.columns.find(e => e.name === col.name);
                if (!c) {
                  qs.push(`alter table "${ct.name}" add column "${col.name}" ${col.type}${col.nullable ? '' : ' not null'}${col.default ? ` default ${col.default}` : ''};`);
                } else if (cmd.details && (c.default !== col.default || c.nullable !== col.nullable || c.type !== col.type)) {
                  if (c.type !== col.type) qs.push(`alter table "${ct.name}" alter column "${col.name}" type ${col.type};`);
                  if (c.default !== col.default) qs.push(`alter table "${ct.name}" alter column "${col.name}" ${col.default ? 'set' : 'drop'} default${col.default ? ` ${col.default}` : ''};`);
                  if (c.nullable !== col.nullable) qs.push(`alter table "${ct.name}" alter column "${col.name}" ${col.nullable ? 'drop' : 'set'} not null;`);
                }
              }
            }
          }

          if (qs.length) {
            console.log(`\nPatches for ${config.name ? config.name : `${config.pgconfig.database}@${config.pgconfig.host || 'localhost'}`}`);
            console.log(qs.join('\n'));
            if (cmd.commit) {
              await client.query(['begin;'].concat(qs).concat('commit;').join('\n'));
            }
          } else {
            console.log(`\nNo patches needed for ${config.pgconfig.database}@${config.pgconfig.host || 'localhost'}`);
          }
        } finally {
          await client.end();
        }
      }
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

    let cfg = configs.find(c => c.name === name);
    if (!cfg) cfg = configs.find(c => c.pgconfig && c.pgconfig.database === name);

    if (!cfg) throw new Error(`No config matching '${name}' was found`);
    if (!cfg.pgconfig && cfg.pgconfig.database) throw new Error(`No valid connection is specified for '${name}'`);

    if (typeof cmd.init === 'string' && !migration.test(cmd.init)) throw new Error(`Init migration target not in proper format`);

    const files = (await fs.readdir(p)).filter(n => migration.test(n) && migration.exec(n)[2]);
    files.sort();

    const client = new pg.Client(cfg.pgconfig);

    try {
      await client.connect();
      let comment: string = ((await client.query(commentQuery, [cfg.pgconfig.database])).rows[0] || {}).comment;
      if (!comment) {
        if (cmd.init) {
          comment = `# Migrations\n===\n${files.filter(f => migration.exec(f)[1] <= cmd.init).map(f => migration.exec(f)[1]).join('\n')}`;
          await client.query(`comment on database "${cfg.pgconfig.database}" is '${comment.replace(/'/g, '\'\'')}';`);
        } else {
          throw new Error(`Database '${name}' migrations are not initialized`);
        }
      }

      if (!migrations.test(comment)) {
        if (cmd.init) {
          comment += `${comment ? '\n\n' : ''}# Migrations\n===\n${files.filter(f => migration.exec(f)[1] <= cmd.init).map(f => migration.exec(f)[1]).join('\n')}`;
          await client.query(`comment on database "${cfg.pgconfig.database}" is '${comment.replace(/'/g, '\'\'')}';`);
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
          await client.query(`comment on database "${cfg.pgconfig.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
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
          await client.query(`comment on database "${cfg.pgconfig.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
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
          await client.query(`comment on database "${cfg.pgconfig.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
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
          await client.query(`comment on database "${cfg.pgconfig.database}" is '${`${pre}# Migrations\n===\n${ran.join('\n')}`.replace(/'/g, '\'\'')}';`);
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
  console.error(err.message);
  process.exit(1);
});
