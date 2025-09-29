import { execFileSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { join, basename } from 'node:path';
import { XMLParser } from 'fast-xml-parser';
import { query } from './db.mjs';

const RAW_DIR = 'data/raw';
const PARSED_DIR = 'data/parsed';

const parser = new XMLParser({ ignoreAttributes: false, trimValues: true });

const t = s => (s && String(s).trim().length ? String(s).trim() : null);
const iso = s => (t(s) ? String(s).trim() : null);
const arr = v => {
  if (!v) return null;
  const a = Array.isArray(v) ? v : [v];
  const c = a.map(x => String(x).trim()).filter(Boolean);
  return c.length ? c : null;
};

async function storeRaw(source_id, xml) {
  const sql = `
    insert into tb.ted_raw_xml (source, source_id, xml_text)
    values ('ted', $1, $2)
    on conflict (source, source_id) do update
      set xml_text = excluded.xml_text,
          inserted_at = now();
  `;
  await query(sql, [source_id, xml]);
}

async function upsertStaging(p) {
  const sql = `
    insert into tb.ted_staging_std
      (source, source_id, title, description, buyer_name, buyer_country, cpv_codes,
       published_at, deadline, url_notice, url_detail, attachments, parsed_json,
       notice_form, notice_type, notice_subtype, procedure_code, notice_language, regulatory_domain)
    values
      ('ted', $1, $2, $3, $4, $5, $6,
       $7, $8, $9, $10, $11, $12,
       $13, $14, $15, $16, $17, $18)
    on conflict (source, source_id) do update set
      title             = excluded.title,
      description       = excluded.description,
      buyer_name        = excluded.buyer_name,
      buyer_country     = excluded.buyer_country,
      cpv_codes         = excluded.cpv_codes,
      published_at      = excluded.published_at,
      deadline          = excluded.deadline,
      url_notice        = excluded.url_notice,
      url_detail        = excluded.url_detail,
      attachments       = excluded.attachments,
      parsed_json       = excluded.parsed_json,
      notice_form       = excluded.notice_form,
      notice_type       = excluded.notice_type,
      notice_subtype    = excluded.notice_subtype,
      procedure_code    = excluded.procedure_code,
      notice_language   = excluded.notice_language,
      regulatory_domain = excluded.regulatory_domain,
      updated_at        = now();
  `;
  const params = [
    p.source_id,
    t(p.title),
    t(p.description),
    t(p.buyer_name),
    t(p.buyer_country),
    arr(p.cpv_codes),
    iso(p.published_at),
    iso(p.deadline),
    t(p.url_notice),
    t(p.url_detail),
    JSON.stringify(p.attachments || []),
    JSON.stringify(p.parsed_json || {}),
    t(p.notice_form),
    t(p.notice_type),
    t(p.notice_subtype),
    t(p.procedure_code),
    t(p.notice_language),
    t(p.regulatory_domain)
  ];
  await query(sql, params);
}

async function run() {
  const files = readdirSync(RAW_DIR).filter(f => f.endsWith('.xml'));
  for (const f of files) {
    const input = join(RAW_DIR, f);
    const out = join(PARSED_DIR, f.replace(/\.xml$/, '.parsed.xml'));

    // XSLT transform with Saxon
    execFileSync('./bin/xslt.sh', [input, out], { stdio: 'inherit' });

    // Read raw and parsed
    const rawText = readFileSync(input, 'utf8');
    const parsedXml = readFileSync(out, 'utf8');
    const obj = parser.parse(parsedXml); // -> { parsed: { ... } }

    const P = obj?.parsed || {};
    const source_id = t(P.source_id) || basename(f, '.xml');

    const record = {
      source_id,
      title: P.title,
      description: P.description,
      buyer_name: P.buyer_name,
      buyer_country: P.buyer_country,
      cpv_codes: P?.cpv_codes?.code ?? null,
      published_at: P.published_at,
      deadline: P.deadline,
      url_notice: P.url_notice,
      url_detail: P.url_detail,
      attachments: Array.isArray(P?.attachments?.a)
        ? P.attachments.a.map(a => ({ name: a?.name ?? null, href: a?.href ?? null }))
        : [],
      parsed_json: obj,
      notice_form: P.notice_form,
      notice_type: P.notice_type,
      notice_subtype: P.notice_subtype,
      procedure_code: P.procedure_code,
      notice_language: P.notice_language,
      regulatory_domain: P.regulatory_domain
    };

    await storeRaw(source_id, rawText);
    await upsertStaging(record);

    console.log(`OK ${f} -> ${source_id}`);
  }
  process.exit(0);
}

run().catch(err => { console.error(err); process.exit(1); });
