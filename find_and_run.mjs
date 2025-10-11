#!/usr/bin/env node
// find_and_run.mjs — resolves the correct OJS issue for a given date, then runs ingest_daily_package.mjs

import 'dotenv/config';
import fetch from 'node-fetch';
import { execFileSync } from 'node:child_process';

function getArg(name, def = null) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=')[1] : def;
}

const dateStr = getArg('date');
if (!dateStr || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
  console.error('Usage: node find_and_run.mjs --date=YYYY-MM-DD');
  process.exit(2);
}

// Scan /packages/daily/{yyyynnnnn} by HEAD until filename contains YYYYMMDD
async function resolveIssueId(yyyyMmDd) {
  const y = yyyyMmDd.slice(0,4);
  const needle = yyyyMmDd.replace(/-/g, '');
  for (let n = 1; n <= 400; n++) {
    const id = `${y}${String(n).padStart(5,'0')}`;
    const url = `https://ted.europa.eu/packages/daily/${id}`;
    try {
      const res = await fetch(url, { method: 'HEAD' });
      if (res.ok) {
        const cd = res.headers.get('content-disposition') || '';
        if (cd.includes(needle)) return id;
      }
    } catch { /* ignore and continue */ }
  }
  return null;
}

(async () => {
  console.log(`Resolving package for ${dateStr}…`);
  const issueId = await resolveIssueId(dateStr);
  if (!issueId) {
    console.error(`Failed to resolve a package id for ${dateStr}`);
    process.exit(1);
  }
  console.log(`Resolved issueId: ${issueId}`);

  // Run the ingestor
  const out = execFileSync(
    process.execPath,
    ['ingest_daily_package.mjs', `--date=${dateStr}`, `--issue=${issueId}`],
    { stdio: 'inherit' }
  );
})();
