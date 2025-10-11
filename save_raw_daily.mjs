// save_raw_daily.mjs
// Downloads a TED daily package by issue id and writes ALL raw XML files to ./raw/issue-<ISSUE>/
// ⚠️ Does NOT touch the database.

import fetch from 'node-fetch';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import tar from 'tar-stream';

function arg(k, def=null) {
  const m = process.argv.find(a => a.startsWith(`--${k}=`));
  return m ? m.split('=').slice(1).join('=').trim() : def;
}

const issue = arg('issue');
if (!issue) {
  console.error('Usage: node save_raw_daily.mjs --issue=202500173');
  process.exit(1);
}

const pkgUrl = `https://ted.europa.eu/packages/daily/${issue}`;
const outRoot = path.resolve('./raw');
const outDir = path.join(outRoot, `issue-${issue}`);

async function ensureDir(p) {
  await fs.promises.mkdir(p, { recursive: true });
}

function sanitizeName(name) {
  // keep only file-like names; drop directories outside current
  return name.replace(/^\.+/,'').replace(/[^a-zA-Z0-9._/-]/g,'_');
}

async function downloadAndExtract(url, destDir) {
  console.log(`Downloading: ${url}`);
  const res = await fetch(url, { redirect: 'follow' });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} fetching ${url}`);
  }

  await ensureDir(destDir);

  const extract = tar.extract();
  const gunzip = zlib.createGunzip();

  let totalEntries = 0;
  let xmlCount = 0;

  extract.on('entry', async (header, stream, next) => {
    totalEntries++;
    const name = sanitizeName(header.name || '');
    const isXml = name.toLowerCase().endsWith('.xml');

    if (isXml) {
      // Flatten nested dirs into the issue folder
      const base = path.basename(name);
      const outPath = path.join(destDir, base);
      const ws = fs.createWriteStream(outPath);
      stream.pipe(ws);
      ws.on('finish', () => {
        xmlCount++;
        next();
      });
      ws.on('error', (e) => {
        console.error(`Write error for ${base}:`, e.message);
        next();
      });
    } else {
      // drain
      stream.on('end', next);
      stream.resume();
    }
  });

  const done = new Promise((resolve, reject) => {
    extract.on('finish', resolve);
    extract.on('error', reject);
  });

  // Pipe the HTTP body -> gunzip -> tar extractor
  res.body.pipe(gunzip).pipe(extract);
  await done;

  return { totalEntries, xmlCount };
}

(async () => {
  try {
    await ensureDir(outRoot);
    const { totalEntries, xmlCount } = await downloadAndExtract(pkgUrl, outDir);
    console.log(`Saved raw XMLs to ${outDir}`);
    console.log(`Tar entries: ${totalEntries}  |  XML files saved: ${xmlCount}`);
  } catch (err) {
    console.error('Fatal error:', err.message || err);
    process.exit(1);
  }
})();
