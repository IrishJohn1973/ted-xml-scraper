// TED daily ingester - downloads, parses, saves raw XML + cleaned data
import fetch from "node-fetch";
import dotenv from "dotenv";
import { Client } from "pg";
import tar from "tar-stream";
import zlib from "zlib";
import { XMLParser } from "fast-xml-parser";
import { DateTime } from "luxon";
import { randomUUID, createHash } from "node:crypto";

dotenv.config();

// Database connection with SSL disabled
async function connectDb() {
  const client = new Client({
    host: process.env.SUPA_DB_HOST,
    database: process.env.SUPA_DB,
    user: process.env.SUPA_DB_USER,
    password: process.env.SUPA_DB_PASS,
    port: 5432,
    ssl: false,  // Changed from {rejectUnauthorized: false}
  });
  await client.connect();
  return client;
}

// Helper functions
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
    if (/^\d{4}-\d{2}-\d{2}T/.test(dIso)) iso = dIso;
    else if (/^\d{4}-\d{2}-\d{2}\+/.test(dIso)) {
      const tz = dIso.slice(dIso.indexOf("+"));
      const day = dIso.slice(0, 10);
      iso = `${day}T00:00:00${tz}`;
    } else iso = dIso;
  } else if (!dIso && tIso) {
    iso = tIso;
  }
  try {
    const dt = DateTime.fromISO(iso, { setZone: true });
    if (dt.isValid) return dt.toUTC().toISO();
  } catch {}
  return null;
}

function sha256Hex(s) {
  return createHash("sha256").update(s || "", "utf8").digest("hex");
}

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  textNodeName: "#text",
  processEntities: true,
});

// Issue resolver
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

// Download and extract
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

// Extract row from XML
function extractRow(xml, fallbackDate, source_row_hash) {
  const doc = parser.parse(xml);

  const isAward = !!doc.ContractAwardNotice;
  const root = doc.ContractNotice || doc.ContractAwardNotice || doc;

  const pubDate = firstNonNull(
    dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension", "Publication", "PublicationDate"]),
    dig(root, ["UBLExtensions", "UBLExtension", 0, "ExtensionContent", "EformsExtension", "Publication", "PublicationDate"]),
    dig(root, ["IssueDate"])
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

  const cpv = firstNonNull(
    dig(root, ["ProcurementProject", "MainCommodityClassification", "ItemClassificationCode"])
  );

  const rawDeadlineDate = firstNonNull(
    dig(root, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndDate"])
  );
  const rawDeadlineTime = firstNonNull(
    dig(root, ["TenderingProcess", "TenderSubmissionDeadlinePeriod", "EndTime"])
  );
  const deadline = combineDeadline(rawDeadlineDate, rawDeadlineTime);

  const pubId = firstNonNull(
    dig(root, ["UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension", "Publication", "NoticePublicationID"]),
    dig(root, ["UBLExtensions", "UBLExtension", 0, "ExtensionContent", "EformsExtension", "Publication", "NoticePublicationID"])
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
    buyer_country: buyerCountry,
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

// Save raw XML to database
async function saveRawXml(client, native_id, xml) {
  if (!native_id) return;
  
  const sql = `
    INSERT INTO tb.ted_raw_xml (source, source_id, xml_text)
    VALUES ($1, $2, $3)
    ON CONFLICT (source, source_id) DO UPDATE 
    SET xml_text = EXCLUDED.xml_text
  `;
  
  await client.query(sql, ['TED', native_id, xml]);
}

// Upsert cleaned rows
async function upsertRows(client, rows, runId) {
  const filtered = rows.filter((r) => r.tb_id && r.native_id && r.published_at);
  if (!filtered.length) return;

  const sql = `
    INSERT INTO tb.ted_staging_std
      (tb_id, native_id, title, short_description, buyer_country, cpv_main,
       deadline, raw_deadline_date, raw_deadline_time, detail_url,
       is_award, competition_flag, published_at, run_id, source_row_hash)
    VALUES
      ${filtered
        .map((_, i) => {
          const base = i * 15;
          const p = (n) => "$" + (base + n);
          return "(" + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15].map((n) => p(n)).join(", ") + ")";
        })
        .join(",\n")}
    ON CONFLICT (tb_id) DO UPDATE SET
      native_id = EXCLUDED.native_id,
      title = EXCLUDED.title,
      short_description = EXCLUDED.short_description,
      buyer_country = EXCLUDED.buyer_country,
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
    r.title,
    r.short_description,
    r.buyer_country,
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

  await client.query(sql, flat);
}

// Main
async function main() {
  const dateArg = process.argv.find((a) => a.startsWith("--date="));
  const issueArg = process.argv.find((a) => a.startsWith("--issue="));
  if (!dateArg) {
    console.error("Usage: node ingest_daily_package.mjs --date=YYYY-MM-DD [--issue=YYYYNNNNN]");
    process.exit(1);
  }
  const date = dateArg.split("=")[1];
  let issue = issueArg ? issueArg.split("=")[1] : null;

  const RUN_ID = `ted-run:${issue ?? "auto"}:${date}:${randomUUID()}`;

  const client = await connectDb();
  console.log(`DB connected. Ingesting ${date}…`);
  
  try {
    if (!issue) {
      issue = await resolveIssueForDate(date);
      console.log(`Resolved issue: ${issue}`);
    }
    const url = `https://ted.europa.eu/packages/daily/${issue}`;

    const rows = [];
    let xmlCount = 0;

    await downloadAndExtract(url, async (xml) => {
      xmlCount += 1;
      const source_row_hash = sha256Hex(xml);
      const row = extractRow(xml, date, source_row_hash);
      
      if (row.native_id) {
        // Save raw XML
        await saveRawXml(client, row.native_id, xml);
        rows.push(row);
      }
    });

    console.log(`Parsed XML files: ${xmlCount}. Rows with native_id: ${rows.length}`);
    
    if (rows.length) {
      await upsertRows(client, rows, RUN_ID);
      console.log(`✅ Upserted ${rows.length} rows (raw + staging)`);
      console.log(`   Run ID: ${RUN_ID}`);
    } else {
      console.log("No rows to insert.");
    }
  } catch (err) {
    console.error("❌ Fatal error:", err.message || err);
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main();
