import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';

const OUT_DIR = 'data/raw';
mkdirSync(OUT_DIR, { recursive: true });

const YEAR      = process.env.YEAR || '2025';
const START_ID  = Number(process.env.START_ID || 636200);
const END_ID    = Number(process.env.END_ID   || 635700); // inclusive, going downward
const MAX_MISS  = Number(process.env.MAX_MISS || 120);    // stop after this many misses in a row

const BASE_DELAY_MS = 700;
const JITTER_MS     = 400;
const SLEEP = ms => new Promise(r => setTimeout(r, ms));
const rand  = n => Math.floor(Math.random()*n);

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36 TedXMLScraper/ids/1.0',
    extraHTTPHeaders: { 'Accept': 'application/xml,text/xml;q=0.9,*/*;q=0.8' }
  });
  const page = await context.newPage();

  let misses = 0, hits = 0;
  for (let id = START_ID; id >= END_ID; id--) {
    const slug = `${id}-${YEAR}`;
    const url  = `https://ted.europa.eu/en/notice/${slug}/xml`;
    try {
      const resp = await page.request.get(url, { timeout: 120000 });
      if (resp.ok()) {
        const body = await resp.body();
        const file = join(OUT_DIR, `ted_${slug}.xml`);
        writeFileSync(file, body);
        console.log(`+ ${slug} -> ${file}`);
        hits++; misses = 0;
      } else if (resp.status() === 404) {
        console.log(`- ${slug} not found`);
        misses++;
      } else if (resp.status() === 429) {
        console.log(`! ${slug} rate limited; pausing`);
        await SLEEP(5000 + rand(2000));
        id++; // retry same id on next loop
        continue;
      } else {
        console.log(`? ${slug} HTTP ${resp.status()}`);
        misses++;
      }
    } catch (e) {
      console.log(`x ${slug} ${e.message}`);
      misses++;
    }
    await SLEEP(BASE_DELAY_MS + rand(JITTER_MS));
    if (misses >= MAX_MISS) { console.log(`Stopping after ${misses} consecutive misses.`); break; }
  }

  await browser.close();
  console.log(`Done. Hits: ${hits}, consecutive misses limit: ${MAX_MISS}`);
})();
