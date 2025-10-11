// TED daily ingester - downloads, parses, saves raw XML + cleaned data
import "dotenv/config";
import fetch from "node-fetch";
import tar from "tar-stream";
import zlib from "zlib";
import { XMLParser } from "fast-xml-parser";
import { DateTime } from "luxon";
import { randomUUID, createHash } from "node:crypto";

// ============================================================================
// SUPABASE API OR DATABASE CONNECTION
// ============================================================================

const USE_API = process.env.USE_SUPABASE_API === 'true';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

async function supabaseRequest(endpoint, method = 'GET', body = null, preferResolution = false) {
  const url = `${SUPABASE_URL}/rest/v1/${endpoint}`;
  const headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
    'Accept-Profile': 'tb',
    'Content-Profile': 'tb'
  };
  
  if (method === 'POST' && preferResolution) {
    headers['Prefer'] = 'resolution=merge-duplicates';
  } else {
    headers['Prefer'] = 'return=minimal';
  }
  
  const options = { method, headers };
  
  if (body) {
    options.body = JSON.stringify(body);
  }
  
  const res = await fetch(url, options);
  if (!res.ok) {
    const error = await res.text();
    throw new Error(`Supabase API error: ${res.status} - ${error}`);
  }
  
  if (method === 'GET' || method === 'POST' && !headers.Prefer.includes('minimal')) {
    return await res.json();
  }
  return null;
}

async function connectDb() {
  if (USE_API) {
    console.log('Using Supabase API mode');
    return null;
  }
  
  const { Client } = await import('pg');
  const client = new Client({
    host: process.env.SUPA_DB_HOST,
    database: process.env.SUPA_DB,
    user: process.env.SUPA_DB_USER,
    password: process.env.SUPA_DB_PASS,
    port: parseInt(process.env.SUPA_DB_PORT) || 5432,
    ssl: process.env.SUPA_DB_HOST?.includes('supabase.co') ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 10000,
    query_timeout: 30000,
    statement_timeout: 30000,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000,
  });
  
  console.log(`Connecting to ${process.env.SUPA_DB_HOST}:${process.env.SUPA_DB_PORT}...`);
  await client.connect();
  console.log('✅ Database connected successfully');
  return client;
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function normText(x) {
  if (x == null) return null;
  if (typeof x === "string") return x.trim() || null;
  if (typeof x === "number") return String(x);
  if (Array.isArray(x)) {
    for (const v of x) {
      const t = normText(v);
      if (t) return t;
    }
    return null;
  }
  if (typeof x === "object") {
    if ("#text" in x) return normText(x["#text"]);
    if ("content" in x) return normText(x.content);
    if ("_" in x) return normText(x._);
    const vals = Object.values(x);
    if (vals.length === 1 && typeof vals[0] === "string") return normText(vals[0]);
  }
  return null;
}

function firstNonNull(...vals) {
  for (const v of vals) {
    const t = normText(v);
    if (t) return t;
  }
  return null;
}

function dig(obj, path) {
  let cur = obj;
  for (const k of path) {
    if (cur == null) return null;
    if (Array.isArray(cur)) cur = cur[0];
    cur = cur?.[k];
  }
  return cur ?? null;
}

function sha256Hex(s) {
  return createHash("sha256").update(s || "", "utf8").digest("hex");
}

// ============================================================================
// ID PARSING & FORMATTING
// ============================================================================

function nativeIdFromPublicationId(pubId) {
  if (!pubId) return null;
  const s = String(pubId).trim();
  if (/^\d{8}-\d{4}$/.test(s)) {
    const [left, year] = s.split("-");
    const num = left.replace(/^0+/, "") || "0";
    return `${num}-${year}`;
  }
  return s;
}

function parseNativeIdFromXml(xml) {
  const m = xml.match(/<[^>]*NoticePublicationID[^>]*>([^<]+)<\/[^>]*NoticePublicationID>/i);
  if (m) return nativeIdFromPublicationId(m[1]);
  
  const m2 = xml.match(/<NOTICE_NUMBER_OJS[^>]*>(\d+)<\/NOTICE_NUMBER_OJS>.*?<NOTICE_YEAR[^>]*>(\d{4})<\/NOTICE_YEAR>/is);
  if (m2) {
    const num = String(m2[1]).replace(/^0+/, "") || "0";
    return `${num}-${m2[2]}`;
  }
  return null;
}

function parseBuyerCountryFromXml(xml) {
  const m = xml.match(/<[^>]*IdentificationCode[^>]*listName="country"[^>]*>([A-Z]{3})<\/[^>]*IdentificationCode>/i);
  return m ? m[1] : null;
}

// ============================================================================
// DATE/TIME HANDLING
// ============================================================================

function combineDeadline(rawDate, rawTime) {
  if (!rawDate && !rawTime) return null;
  
  const expand = (s) => {
    if (!s) return null;
    if (/T/.test(s)) return s;
    
    const m1 = /^(\d{4}-\d{2}-\d{2})\+(\d{2}:\d{2})$/.exec(s);
    if (m1) return `${m1[1]}T00:00:00+${m1[2]}`;
    
    const m2 = /^(\d{2}:\d{2}:\d{2})\+(\d{2}:\d{2})$/.exec(s);
    if (m2) return `0000-01-01T${m2[1]}+${m2[2]}`;
    
    return s;
  };
  
  const dIso = expand(rawDate);
  const tIso = expand(rawTime);

  let iso = null;
  if (dIso && tIso && /^(\d{4}-\d{2}-\d{2})T/.test(dIso) && /^0000-01-01T/.test(tIso)) {
    const tz = dIso.includes("+") ? dIso.slice(dIso.indexOf("+")) : "Z";
    const day = dIso.slice(0, 10);
    const time = tIso.slice(11, 19);
    iso = `${day}T${time}${tz}`;
  } else if (dIso && !tIso) {
    if (/^\d{4}-\d{2}-\d{2}T/.test(dIso)) {
      iso = dIso;
    } else if (/^\d{4}-\d{2}-\d{2}\+/.test(dIso)) {
      const tz = dIso.slice(dIso.indexOf("+"));
      const day = dIso.slice(0, 10);
      iso = `${day}T00:00:00${tz}`;
    } else {
      iso = dIso;
    }
  } else if (!dIso && tIso) {
    iso = tIso;
  }
  
  try {
    const dt = DateTime.fromISO(iso, { setZone: true });
    if (dt.isValid) return dt.toUTC().toISO();
  } catch {}
  
  return null;
}

// ============================================================================
// ISSUE RESOLVER
// ============================================================================

async function resolveIssueForDate(dateStr) {
  const yyyy = dateStr.slice(0, 4);
  const yyyymmdd = dateStr.replace(/-/g, "");
  const start = Number(`${yyyy}00001`);
  const end = Number(`${yyyy}00400`);

  for (let candidate = start; candidate <= end; candidate++) {
    const url = `https://ted.europa.eu/packages/daily/${candidate}`;
    try {
      const res = await fetch(url, { method: "HEAD", redirect: "follow" });
      if (!res.ok) continue;
      
      const cd = res.headers.get("content-disposition") || "";
      if (cd.includes(`${yyyymmdd}_`)) {
        return String(candidate);
      }
    } catch {}
    
    await new Promise((r) => setTimeout(r, 30));
  }
  
  throw new Error(`No package found matching ${dateStr}`);
}

// ============================================================================
// DOWNLOAD & EXTRACTION
// ============================================================================

async function downloadAndExtract(url, onXml) {
  console.log(`Downloading: ${url}`);
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "*/*" },
    redirect: "follow",
  });
  
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);

  return new Promise((resolve, reject) => {
    const extract = tar.extract();

    extract.on("entry", (header, stream, next) => {
      const name = header.name || "";
      if (name.toLowerCase().endsWith(".xml")) {
        let xml = "";
        stream.on("data", (c) => (xml += c.toString("utf8")));
        stream.on("end", () => {
          try {
            onXml(xml, name);
          } catch (e) {
            console.error("XML handle error:", e.message);
          }
          next();
        });
      } else {
        stream.resume();
        stream.on("end", next);
      }
    });

    extract.on("finish", resolve);
    extract.on("error", reject);

    const gunzip = zlib.createGunzip();
    res.body.on("error", reject).pipe(gunzip).pipe(extract);
  });
}

// ============================================================================
// XML PARSING
// ============================================================================

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  textNodeName: "#text",
  processEntities: true,
});

function extractRow(xml, fallbackDate, source_row_hash) {
  const doc = parser.parse(xml);

  const isAward = !!doc.ContractAwardNotice;
  const root = doc.ContractNotice || doc.ContractAwardNotice || doc.PriorInformationNotice || doc;

  // Extract publication date - try multiple paths
  const pubDate = firstNonNull(
    dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension", "Publication", "PublicationDate"]),
    dig(root, ["UBLExtensions", "UBLExtension", 0, "ExtensionContent", "EformsExtension", "Publication", "PublicationDate"]),
    dig(root, ["IssueDate"]),
    dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "Publication", "PublicationDate"])
  );

  let published_at = null;
  if (pubDate) {
    try {
      const iso = /\+/.test(pubDate) && !/T/.test(pubDate)
        ? `${pubDate.slice(0, 10)}T00:00:00${pubDate.slice(10)}`
        : pubDate;
      const dt = DateTime.fromISO(iso, { setZone: true });
      if (dt.isValid) published_at = dt.toUTC().toISO();
    } catch {}
  }
  if (!published_at && fallbackDate) {
    published_at = DateTime.fromISO(fallbackDate, { zone: "UTC" }).toUTC().toISO();
  }

  // Extract language
  const language = firstNonNull(
    dig(root, ["NoticeLanguageCode"]),
    dig(root, ["cbc:NoticeLanguageCode"])
  );

  // Extract buyer organization name and address
  let buyerName = null;
  let buyerCity = null;
  let buyerStreet = null;
  
  const orgs = dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension", "Organizations", "Organization"]);
  if (orgs) {
    const orgList = Array.isArray(orgs) ? orgs : [orgs];
    // Find the first organization that's not Publications Office
    for (const org of orgList) {
      const name = firstNonNull(
        dig(org, ["Company", "PartyName", "Name"]),
        dig(org, ["efac:Company", "cac:PartyName", "cbc:Name"])
      );
      if (name && !name.includes("Publications Office")) {
        buyerName = name;
        buyerCity = firstNonNull(
          dig(org, ["Company", "PostalAddress", "CityName"]),
          dig(org, ["efac:Company", "cac:PostalAddress", "cbc:CityName"])
        );
        buyerStreet = firstNonNull(
          dig(org, ["Company", "PostalAddress", "StreetName"]),
          dig(org, ["efac:Company", "cac:PostalAddress", "cbc:StreetName"])
        );
        break;
      }
    }
  }

  // Extract core fields
  const title = firstNonNull(
    dig(root, ["ProcurementProject", "Name"]),
    dig(root, ["ProcurementProject", "Name", "#text"])
  );

  const shortDesc = firstNonNull(
    dig(root, ["ProcurementProject", "Description"]),
    dig(root, ["ProcurementProject", "Description", "#text"])
  );

  let buyerCountry = firstNonNull(
    dig(root, ["ProcurementProject", "RealizedLocation", "Address", "Country", "IdentificationCode"]),
    dig(root, ["ContractingParty", "Party", "PostalAddress", "Country", "IdentificationCode"])
  );
  if (!buyerCountry) buyerCountry = parseBuyerCountryFromXml(xml);

  // If we didn't get city from org, try from RealizedLocation
  if (!buyerCity) {
    buyerCity = firstNonNull(
      dig(root, ["ProcurementProject", "RealizedLocation", "Address", "CityName"])
    );
  }

  const cpv = firstNonNull(
    dig(root, ["ProcurementProject", "MainCommodityClassification", "ItemClassificationCode"])
  );

  // Extract deadline - try multiple paths including LOT level
  let rawDeadlineDate = firstNonNull(
    dig(root, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndDate"])
  );
  let rawDeadlineTime = firstNonNull(
    dig(root, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndTime"])
  );
  
  // If not found at root level, check LOT level
  if (!rawDeadlineDate) {
    const lot = dig(root, ["ProcurementProjectLot"]);
    if (lot) {
      rawDeadlineDate = firstNonNull(
        dig(lot, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndDate"])
      );
      rawDeadlineTime = firstNonNull(
        dig(lot, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndTime"])
      );
    }
  }
  
  const deadline = combineDeadline(rawDeadlineDate, rawDeadlineTime);

  // Extract publication ID and construct native_id - try multiple paths
  const pubId = firstNonNull(
    dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension", "Publication", "NoticePublicationID"]),
    dig(root, ["UBLExtensions", "UBLExtension", 0, "ExtensionContent", "EformsExtension", "Publication", "NoticePublicationID"]),
    dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "Publication", "NoticePublicationID"])
  );

  let native_id = nativeIdFromPublicationId(pubId);
  if (!native_id) native_id = parseNativeIdFromXml(xml);
  
  const tb_id = native_id ? `TED|${native_id}` : null;

  const detail_url = native_id ? `https://ted.europa.eu/en/notice/${native_id}` : null;
  const competition_flag = !isAward;

  return {
    tb_id,
    native_id,
    title,
    short_description: shortDesc,
    buyer_name: buyerName,
    buyer_country: buyerCountry,
    buyer_city: buyerCity,
    buyer_street: buyerStreet,
    language,
    cpv_main: cpv,
    deadline,
    raw_deadline_date: rawDeadlineDate,
    raw_deadline_time: rawDeadlineTime,
    detail_url,
    is_award: isAward,
    competition_flag,
    published_at,
    source_row_hash,
  };
}

// ============================================================================
// DATABASE OPERATIONS
// ============================================================================

async function saveRawXml(client, native_id, xml) {
  if (!native_id) return;
  
  if (USE_API) {
    // Skip raw XML saves in API mode - focus on staging data only
    return;
  }
  
  const sql = `
    INSERT INTO tb.ted_raw_xml (source, source_id, xml_text)
    VALUES ($1, $2, $3)
    ON CONFLICT (source, source_id) DO UPDATE 
    SET xml_text = EXCLUDED.xml_text
  `;
  
  await client.query(sql, ['TED', native_id, xml]);
}

async function upsertRows(client, rows, runId) {
  console.log(`\n📊 Pre-filter analysis of ${rows.length} rows:`);
  const missingTbId = rows.filter(r => !r.tb_id).length;
  const missingNativeId = rows.filter(r => !r.native_id).length;
  const missingPublishedAt = rows.filter(r => !r.published_at).length;
  
  console.log(`  Missing tb_id: ${missingTbId}`);
  console.log(`  Missing native_id: ${missingNativeId}`);
  console.log(`  Missing published_at: ${missingPublishedAt}`);
  
  if (missingPublishedAt > 0) {
    console.log(`\n  Sample rows missing published_at:`);
    const samples = rows.filter(r => !r.published_at).slice(0, 3);
    samples.forEach(r => {
      console.log(`    - ${r.native_id}: title="${r.title?.substring(0, 50)}"`);
    });
  }
  
  const filtered = rows.filter((r) => r.tb_id && r.native_id && r.published_at);
  
  console.log(`  Rows passing filter: ${filtered.length}\n`);
  
  if (!filtered.length) return 0;

  if (USE_API) {
    console.log(`  Upserting via Supabase API in batches...`);
    
    const batchSize = 500;
    let inserted = 0;
    
    for (let i = 0; i < filtered.length; i += batchSize) {
      const batch = filtered.slice(i, i + batchSize).map(r => ({
        tb_id: r.tb_id,
        native_id: r.native_id,
        source: 'TED',
        title: r.title,
        short_description: r.short_description,
        buyer_country: r.buyer_country,
        cpv_main: r.cpv_main,
        deadline: r.deadline,
        raw_deadline_date: r.raw_deadline_date,
        raw_deadline_time: r.raw_deadline_time,
        detail_url: r.detail_url,
        is_award: r.is_award,
        competition_flag: r.competition_flag,
        published_at: r.published_at,
        run_id: runId,
        source_row_hash: r.source_row_hash
      }));
      
      try {
        const result = await supabaseRequest(
          'ted_staging_std',
          'POST',
          batch,
          true
        );
        inserted += batch.length;
        console.log(`  Inserted batch ${Math.floor(i / batchSize) + 1}: ${inserted}/${filtered.length}`);
      } catch (err) {
        console.error(`  ❌ Batch ${Math.floor(i / batchSize) + 1} error:`, err.message);
        console.error(`  Sample record:`, batch[0]);
      }
    }
    
    return inserted;
  }

  // Original PostgreSQL version
  const sql = `
    INSERT INTO tb.ted_staging_std
      (tb_id, native_id, source, title, short_description, buyer_name, buyer_country, buyer_city, buyer_street, 
       language, cpv_main, deadline, raw_deadline_date, raw_deadline_time, detail_url,
       is_award, competition_flag, published_at, run_id, source_row_hash)
    VALUES
      ${filtered
        .map((_, i) => {
          const base = i * 20;
          return "(" + [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20].map((n) => "$" + (base + n)).join(", ") + ")";
        })
        .join(",\n")}
    ON CONFLICT (tb_id) DO UPDATE SET
      native_id = EXCLUDED.native_id,
      source = EXCLUDED.source,
      title = EXCLUDED.title,
      short_description = EXCLUDED.short_description,
      buyer_name = EXCLUDED.buyer_name,
      buyer_country = EXCLUDED.buyer_country,
      buyer_city = EXCLUDED.buyer_city,
      buyer_street = EXCLUDED.buyer_street,
      language = EXCLUDED.language,
      cpv_main = EXCLUDED.cpv_main,
      deadline = EXCLUDED.deadline,
      raw_deadline_date = EXCLUDED.raw_deadline_date,
      raw_deadline_time = EXCLUDED.raw_deadline_time,
      detail_url = EXCLUDED.detail_url,
      is_award = EXCLUDED.is_award,
      competition_flag = EXCLUDED.competition_flag,
      published_at = COALESCE(EXCLUDED.published_at, tb.ted_staging_std.published_at),
      run_id = EXCLUDED.run_id,
      source_row_hash = EXCLUDED.source_row_hash
  `;

  const flat = filtered.flatMap((r) => [
    r.tb_id,
    r.native_id,
    'TED',
    r.title,
    r.short_description,
    r.buyer_name,
    r.buyer_country,
    r.buyer_city,
    r.buyer_street,
    r.language,
    r.cpv_main,
    r.deadline,
    r.raw_deadline_date,
    r.raw_deadline_time,
    r.detail_url,
    r.is_award,
    r.competition_flag,
    r.published_at,
    runId,
    r.source_row_hash,
  ]);

  console.log(`  Executing INSERT in batches...`);
  
  // PostgreSQL parameter limit workaround - batch insert
  const batchSize = 100; // 100 rows * 20 params = 2000 params (safe)
  let totalInserted = 0;
  
  for (let i = 0; i < filtered.length; i += batchSize) {
    const batch = filtered.slice(i, i + batchSize);
    
    const sql = `
      INSERT INTO tb.ted_staging_std
        (tb_id, native_id, source, title, short_description, buyer_name, buyer_country, buyer_city, buyer_street, 
         language, cpv_main, deadline, raw_deadline_date, raw_deadline_time, detail_url,
         is_award, competition_flag, published_at, run_id, source_row_hash)
      VALUES
        ${batch
          .map((_, idx) => {
            const base = idx * 20;
            return "(" + [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20].map((n) => "$" + (base + n)).join(", ") + ")";
          })
          .join(",\n")}
      ON CONFLICT (tb_id) DO UPDATE SET
        native_id = EXCLUDED.native_id,
        source = EXCLUDED.source,
        title = EXCLUDED.title,
        short_description = EXCLUDED.short_description,
        buyer_name = EXCLUDED.buyer_name,
        buyer_country = EXCLUDED.buyer_country,
        buyer_city = EXCLUDED.buyer_city,
        buyer_street = EXCLUDED.buyer_street,
        language = EXCLUDED.language,
        cpv_main = EXCLUDED.cpv_main,
        deadline = EXCLUDED.deadline,
        raw_deadline_date = EXCLUDED.raw_deadline_date,
        raw_deadline_time = EXCLUDED.raw_deadline_time,
        detail_url = EXCLUDED.detail_url,
        is_award = EXCLUDED.is_award,
        competition_flag = EXCLUDED.competition_flag,
        published_at = COALESCE(EXCLUDED.published_at, tb.ted_staging_std.published_at),
        run_id = EXCLUDED.run_id,
        source_row_hash = EXCLUDED.source_row_hash
    `;

    const flat = batch.flatMap((r) => [
      r.tb_id,
      r.native_id,
      'TED',
      r.title,
      r.short_description,
      r.buyer_name,
      r.buyer_country,
      r.buyer_city,
      r.buyer_street,
      r.language,
      r.cpv_main,
      r.deadline,
      r.raw_deadline_date,
      r.raw_deadline_time,
      r.detail_url,
      r.is_award,
      r.competition_flag,
      r.published_at,
      runId,
      r.source_row_hash,
    ]);

    try {
      const result = await client.query(sql, flat);
      totalInserted += result.rowCount;
      console.log(`  Batch ${Math.floor(i / batchSize) + 1}: ${totalInserted}/${filtered.length} rows`);
    } catch (err) {
      console.error(`  ❌ Batch error:`, err.message);
      throw err;
    }
  }
  
  console.log(`  SQL executed successfully. Total rows: ${totalInserted}`);
  
  // Verify the insert actually worked
  const verifyResult = await client.query(
    'SELECT COUNT(*) as count FROM tb.ted_staging_std WHERE run_id = $1',
    [runId]
  );
  console.log(`  Verification: Found ${verifyResult.rows[0].count} rows with this run_id in database`);
  
  return totalInserted;
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function main() {
  const dateArg = process.argv.find((a) => a.startsWith("--date="));
  const issueArg = process.argv.find((a) => a.startsWith("--issue="));
  
  if (!dateArg) {
    console.error("Usage: node ingest_daily_package.mjs --date=YYYY-MM-DD [--issue=YYYYNNNNN]");
    process.exit(1);
  }
  
  if (USE_API && (!SUPABASE_URL || !SUPABASE_KEY)) {
    console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env for API mode");
    process.exit(1);
  }
  
  const date = dateArg.split("=")[1];
  let issue = issueArg ? issueArg.split("=")[1] : null;

  const RUN_ID = `ted-run:${issue ?? "auto"}:${date}:${randomUUID()}`;

  const client = await connectDb();
  console.log(`${USE_API ? 'API' : 'DB'} connected. Ingesting ${date}…`);
  
  try {
    if (!issue) {
      issue = await resolveIssueForDate(date);
      console.log(`Resolved issue: ${issue}`);
    }
    
    const url = `https://ted.europa.eu/packages/daily/${issue}`;
    const rows = [];
    const allRows = [];
    let xmlCount = 0;
    const failedXmlSamples = [];

    // Collect XML files and rows (synchronous - no await!)
    const xmlFiles = [];
    await downloadAndExtract(url, (xml) => {
      xmlCount += 1;
      const source_row_hash = sha256Hex(xml);
      const row = extractRow(xml, date, source_row_hash);
      
      allRows.push(row);
      
      if (row.native_id) {
        xmlFiles.push({ native_id: row.native_id, xml });
        rows.push(row);
      } else {
        // Capture first 3 failed XMLs for debugging
        if (failedXmlSamples.length < 3) {
          failedXmlSamples.push({
            xmlNumber: xmlCount,
            hasNoticePublicationID: xml.includes('NoticePublicationID'),
            firstChars: xml.substring(0, 500),
            rootElement: xml.match(/<(\w+)\s/)?.[1] || 'unknown'
          });
        }
      }
    });
    
    // Now save raw XMLs in batch (after extraction completes)
    if (!USE_API && xmlFiles.length > 0) {
      console.log(`Saving ${xmlFiles.length} raw XML files...`);
      for (const { native_id, xml } of xmlFiles) {
        await saveRawXml(client, native_id, xml);
      }
    }

    const xmlCountSkipped = xmlCount - rows.length;
    
    console.log(`Parsed XML files: ${xmlCount}`);
    console.log(`Rows with native_id: ${rows.length}`);
    console.log(`Skipped XMLs without native_id: ${xmlCountSkipped}`);
    
    if (failedXmlSamples.length > 0) {
      console.log(`\n⚠️  Sample of failed XMLs:`);
      failedXmlSamples.forEach(sample => {
        console.log(`\nXML #${sample.xmlNumber}:`);
        console.log(`  Root element: ${sample.rootElement}`);
        console.log(`  Has NoticePublicationID: ${sample.hasNoticePublicationID}`);
        console.log(`  First 500 chars:\n${sample.firstChars}`);
      });
    }
    
    if (xmlCountSkipped > 0) {
      console.log(`⚠️  Warning: ${xmlCountSkipped} XMLs had no extractable native_id`);
    }
    
    if (rows.length) {
      const insertedCount = await upsertRows(client, rows, RUN_ID);
      console.log(`✅ Upserted ${insertedCount} rows (raw + staging)`);
      console.log(`   Run ID: ${RUN_ID}`);
    } else {
      console.log("No rows to insert.");
    }
  } catch (err) {
    console.error("❌ Fatal error:", err.message || err);
    process.exitCode = 1;
  } finally {
    if (client?.end) await client.end();
  }
}

main();