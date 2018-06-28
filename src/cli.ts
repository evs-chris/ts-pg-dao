#!/usr/bin/env node
import * as cli from 'commander';
import * as path from 'path';
import * as fs from 'fs-extra';
import { config, write } from './run';
import { BuildConfig } from './main';

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

cli
  .version('0.0.1')
  .option('-c, --config <file>', 'Config file to use, otherwise I\'ll search for a ts-pg-dao.config.ts')
  .parse(process.argv);

(async function() {
  const file = cli.file || await findConfig(path.resolve('.'));
  const res = await config(file);
  let cfg: BuildConfig[];
  if (Array.isArray(res)) cfg = res;
  else cfg = [res];

  for (let i = 0; i < cfg.length; i++) {
    await write(cfg[i]);
  }
})().then(null, e => {
  console.error(e);
});