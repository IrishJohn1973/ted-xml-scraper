#!/usr/bin/env node
// upload_raw_xml.mjs
// Reads raw XML files saved under raw/issue-<ISSUEID>/ and stores them in tb.ted_raw_xml.
//
// Usage:
//   node upload_raw_xml.mjs --issue=202500179
//   node upload_raw_xml.mjs --date=YYYY-MM-DD   (will resolve to an issue id if you also have save_raw_daily.mjs)
//
// Table expected (Supabase):
//   tb.ted_raw_xml (issue_id text, native_id text, xml text, sha256 text,
//                   created_at timestamptz default now(), updated_at timestamptz default now(),
//                   primary key (issue_id, native_id))

import 'dotenv/config';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { Client } from 'pg';
import fetch from 'node-fetch';

function argMap() {
  return Object.fromEntries(
    process.argv.slice(2).map((a) => {
      const [k, v] = a.replace(/^--/, '').split('=');
      return [k, v ?? true];
    }),
  );
}

async function connectDb() {
  const client = new Client({
    host: process.env.SUPA_DB_HOST,
    database: process.env.SUPA_DB,
    user: process.env.SUPA_DB_USER,
    password: process.env.SUPA_DB_PASS,
    port: 5432,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  return client;
}

async function resolveIssueIdByDate(dateStr) {
  // Reuse HEAD scan like find_and_run.mjs (lightweight)
  const year = dateStr.slice(0, 4);
  // Scan last ~400 issues for the year
  const start = 100; // early in year usually 00001.. but keep margin
  const end = 600;
  for (let n = start; n <= end; n++) {
    const issueId = `${year}${String(n).padStart(5, '0')}`;
    const url = `https://ted.europa.eu/packages/daily/${issueId}`;
    try {
      const res = await fetch(url, { method: 'HEAD' });
      if (res.ok) {
        const cd = res.headers.get('content-disposition') || '';
        if (cd.includes(dateStr.replace(/-/g, ''))) return issueId;
      }
    } catch {
      // ignore network blips
    }
  }
  throw new Error(`Could not resolve issueId for ${dateStr}`);
}

function parseNativeIdFromFilename(filename) {
  // expected pattern like 00608908_2025.xml => native_id = "00608908-2025"
  const base = path.basename(filename, '.xml');
  const m = base.match(/^(\d{8})_(\d{4})$/);
  if (!m) return base; // fallback to raw base if pattern differs
  return `${m[1]}-${m[2]}`;
}

async function loadXmlFiles(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = entries.filter((e) => e.isFile() && e.name.endsWith('.xml')).map((e) => e.name);
  const out = [];
  for (const name of files) {
    const full = path.join(dir, name);
    const xml = await fs.readFile(full, 'utf8');
    out.push({ name, xml });
  }
  return out;
}

function sha256Str(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

async function ensureTable(client) {
  await client.query(`
    create table if not exists tb.ted_raw_xml (
      issue_id   text not null,
      native_id  text not null,
      xml        text not null,
      sha256     text not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      constraint ted_raw_xml_pk primary key (issue_id, native_id)
    );
  `);
  await client.query(`create index if not exists ted_raw_xml_updated_at_idx on tb.ted_raw_xml(updated_at desc);`);
}

async function main() {
  const args = argMap();
  const issueExplicit = args.issue && String(args.issue);
  const dateArg = args.date && String(args.date);

  let issueId = issueExplicit || null;
  if (!issueId) {
    if (!dateArg) {
      throw new Error(`Provide --issue=YYYYnnnnn or --date=YYYY-MM-DD`);
    }
    issueId = await resolveIssueIdByDate(dateArg);
  }

  const rawDir = path.resolve(`raw/issue-${issueId}`);
  try {
    await fs.access(rawDir);
  } catch {
    throw new Error(`Raw directory not found: ${rawDir}`);
  }

  const client = await connectDb();
  console.log(`DB connected. Uploading raw XML from ${rawDir} (issue ${issueId})…`);
  await ensureTable(client);

  const files = await loadXmlFiles(rawDir);
  if (files.length === 0) {
    console.log(`No XML files found in ${rawDir}`);
    await client.end();
    return;
  }

  // Batch insert with upsert
  const text = `
    insert into tb.ted_raw_xml (issue_id, native_id, xml, sha256)
    values ${files.map((_, i) => `($1, $${i * 3 + 2}, $${i * 3 + 3}, $${i * 3 + 4})`).join(', ')}
    on conflict (issue_id, native_id) do update
      set xml = excluded.xml,
          sha256 = excluded.sha256,
          updated_at = now();
  `;

  const values = [issueId];
  for (const f of files) {
    const nativeId = parseNativeIdFromFilename(f.name);
    const digest = sha256Str(f.xml);
    values.push(nativeId, f.xml, digest);
  }

  try {
    await client.query('begin');
    await client.query(text, values);
    await client.query('commit');
    console.log(`Uploaded ${files.length} XML files to tb.ted_raw_xml. Failed: 0`);
  } catch (e) {
    await client.query('rollback');
    console.error('Fatal:', e);
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});

