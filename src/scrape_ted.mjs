import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';

const OUT_DIR = 'data/raw';
mkdirSync(OUT_DIR, { recursive: true });

const START_URL =
  process.env.TED_SEARCH_URL
  || process.argv[2]
  || 'https://ted.europa.eu/en/advanced-search?search-txt=&published=90&nature-of-contract=all&language=en';

const MAX_PAGES   = Number(process.env.TED_MAX_PAGES   || 50);
const MAX_NOTICES = Number(process.env.TED_MAX_NOTICES || 2000);

// polite throttling + retry
const BASE_DELAY_MS   = 900;     // base delay between downloads
const JITTER_MS       = 500;     // random jitter to avoid patterns
const RETRIES         = 6;       // retry attempts for 429/5xx
const PAGE_PAUSE_EVERY= 25;      // after this many files, long pause
const PAGE_PAUSE_MS   = 10000;   // long pause duration

const SLEEP = ms => new Promise(r => setTimeout(r, ms));
const rand  = (n) => Math.floor(Math.random() * n);
const backoff = (attempt) => Math.min(15000, (2 ** attempt) * 400 + rand(JITTER_MS));

async function acceptCookies(page) {
  const selectors = [
    'button:has-text("Accept all")',
    'button:has-text("Accept All")',
    'button:has-text("I accept")',
    'button[aria-label*="Accept"]',
    '#eu-cookie-compliance-accept-button',
  ];
  for (const sel of selectors) {
    const el = await page.$(sel).catch(()=>null);
    if (el) { await el.click().catch(()=>{}); await page.waitForTimeout(500); break; }
  }
}

async function ensureResultsLoaded(page) {
  await page.waitForLoadState('domcontentloaded', { timeout: 120000 });
  await page.waitForTimeout(1500);
  await acceptCookies(page);
  // nudge SPA to render
  for (let i=0;i<8;i++){ await page.mouse.wheel(0, 1400); await page.waitForTimeout(250); }
}

async function collectNoticeIds(page) {
  // wait until some links exist, then harvest with lazy scroll
  let ids = new Set();
  for (let i=0;i<20;i++){
    const hrefs = await page.$$eval('a[href*="/en/notice/-/detail/"]', as => as.map(a => a.href)).catch(()=>[]);
    const re = /\/en\/notice\/-\/detail\/(\d+-\d+)/i;
    for (const h of hrefs) { const m = h.match(re); if (m) ids.add(m[1]); }
    await page.mouse.wheel(0, 1600);
    await page.waitForTimeout(300);
    if (ids.size >= 200) break;
  }
  return Array.from(ids);
}

async function goToNextResults(page) {
  let next = await page.$('a[rel="next"],button[rel="next"]').catch(()=>null);
  if (next) { await next.click().catch(()=>{}); return true; }
  const candidates = await page.$$('a,button');
  for (const el of candidates) {
    const aria = await el.getAttribute('aria-label').catch(()=>null);
    const txt  = (await el.innerText().catch(()=>''))?.trim();
    if ((aria && /next/i.test(aria)) || /^next\b/i.test(txt)) { await el.click().catch(()=>{}); return true; }
  }
  return false;
}

async function fetchXmlWithRetry(page, xmlUrl) {
  let attempt = 0;
  while (attempt <= RETRIES) {
    if (attempt > 0) {
      const wait = backoff(attempt);
      console.warn(`    retry ${attempt}/${RETRIES} after ${wait}ms`);
      await SLEEP(wait);
    }
    try {
      const resp = await page.request.get(xmlUrl, { timeout: 120000 });
      if (resp.status() === 429 || resp.status() >= 500) {
        attempt++;
        continue;
      }
      if (!resp.ok()) return { ok:false, status: resp.status() };
      const body = await resp.body();
      return { ok:true, body };
    } catch {
      attempt++;
    }
  }
  return { ok:false, status:429 };
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36 TedXMLScraper/2.3',
    extraHTTPHeaders: {
      'Accept': 'application/xml,text/xml;q=0.9,*/*;q=0.8'
    }
  });
  const page = await context.newPage();

  let total = 0;
  let pageNo = 0;
  let url = START_URL;

  while (pageNo < MAX_PAGES && total < MAX_NOTICES) {
    pageNo++;
    console.log(`[results] ${url}`);
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await ensureResultsLoaded(page);

    const ids = await collectNoticeIds(page);
    if (!ids.length) console.warn('  - no IDs found on this page');

    for (const id of ids) {
      if (total >= MAX_NOTICES) break;

      const xmlUrl = `https://ted.europa.eu/en/notice/${id}/xml`;
      const t0 = Date.now();
      const res = await fetchXmlWithRetry(page, xmlUrl);
      if (!res.ok) {
        console.warn(`  - ${id} download failed: HTTP ${res.status ?? 'ERR'}`);
      } else {
        const filename = join(OUT_DIR, `ted_${id}.xml`);
        writeFileSync(filename, res.body);
        console.log(`  + saved ${filename}`);
        total++;
      }

      // polite per-request pause
      const elapsed = Date.now() - t0;
      const delay = Math.max(0, BASE_DELAY_MS + rand(JITTER_MS) - elapsed);
      await SLEEP(delay);

      // periodic longer pause
      if (total > 0 && total % PAGE_PAUSE_EVERY === 0) {
        console.log(`  ~ pause ${PAGE_PAUSE_MS}ms to respect rate limits`);
        await SLEEP(PAGE_PAUSE_MS);
      }

      if (total >= MAX_NOTICES) break;
    }

    if (total >= MAX_NOTICES) break;
    if (pageNo >= MAX_PAGES) break;

    const advanced = await goToNextResults(page);
    if (!advanced) break;
    await page.waitForTimeout(1200);
    url = page.url();
  }

  await browser.close();
  console.log(`Done. Downloaded ${total} XML file(s) to ${OUT_DIR}`);
}

main().catch(e => { console.error(e); process.exit(1); });
