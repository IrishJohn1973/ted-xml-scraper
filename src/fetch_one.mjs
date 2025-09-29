import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';

const OUT_DIR = 'data/raw';
mkdirSync(OUT_DIR, { recursive: true });

const NOTICE_URL = process.argv[2] || process.env.TED_NOTICE_URL;
if (!NOTICE_URL) {
  console.error('Usage: node src/fetch_one.mjs <ted_notice_detail_url>');
  process.exit(1);
}

async function findXmlUrl(page) {
  // 1) direct .xml links
  let href = await page.$$eval('a[href]', as => {
    for (const a of as) {
      const h = a.getAttribute('href');
      if (h && /\.xml(\?|$)/i.test(h)) {
        try { return new URL(h, location.href).href; } catch { return h; }
      }
    }
    return null;
  });
  if (href) return href;

  // 2) button/link mentioning XML then rescan
  const xmlButton = await page.$('text=/\\bXML\\b/i').catch(() => null);
  if (xmlButton) {
    await xmlButton.click().catch(()=>{});
    await page.waitForTimeout(800);
    href = await page.$$eval('a[href]', as => {
      for (const a of as) {
        const h = a.getAttribute('href');
        if (h && /\.xml(\?|$)/i.test(h)) {
          try { return new URL(h, location.href).href; } catch { return h; }
        }
      }
      return null;
    });
    if (href) return href;
  }

  // 3) heuristic: download links with format param
  href = await page.$$eval('a[href]', as => {
    for (const a of as) {
      const h = a.getAttribute('href') || '';
      if (/download/i.test(h) && /xml/i.test(h)) {
        try { return new URL(h, location.href).href; } catch { return h; }
      }
    }
    return null;
  });
  return href || null;
}

function ts() {
  const d = new Date();
  const pad = n => String(n).padStart(2, '0');
  return d.getFullYear()+pad(d.getMonth()+1)+pad(d.getDate())+pad(d.getHours())+pad(d.getMinutes())+pad(d.getSeconds());
}

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  console.log(`[notice] ${NOTICE_URL}`);
  await page.goto(NOTICE_URL, { waitUntil: 'domcontentloaded', timeout: 120_000 });
  await page.waitForTimeout(1200);

  const xmlUrl = await findXmlUrl(page);
  if (!xmlUrl) {
    console.error('No XML link found on this notice page.');
    await browser.close();
    process.exit(2);
  }
  console.log(`  -> xml ${xmlUrl}`);

  const resp = await page.request.get(xmlUrl, { timeout: 120_000 });
  if (!resp.ok()) {
    console.error('Download failed: ' + resp.status());
    await browser.close();
    process.exit(3);
  }
  const body = await resp.body();
  const filename = join(OUT_DIR, `ted_${ts()}_single.xml`);
  writeFileSync(filename, body);
  console.log(`  + saved ${filename}`);
  await browser.close();
})();
