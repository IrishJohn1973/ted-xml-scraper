import { Client } from 'pg';
import { readFileSync } from 'fs';
import 'dotenv/config';

const client = new Client({
  host: process.env.SUPA_DB_HOST,
  database: process.env.SUPA_DB,
  user: process.env.SUPA_DB_USER,
  password: process.env.SUPA_DB_PASS,
  port: 5432,
  ssl: false
});

async function runSchema() {
  try {
    console.log('🔌 Connecting to database...');
    await client.connect();
    console.log('✅ Connected to database');
    
    console.log('📄 Reading schema.sql...');
    const schema = readFileSync('./schema.sql', 'utf8');
    console.log(`📝 Schema file size: ${schema.length} bytes`);
    
    console.log('🔧 Running schema.sql...');
    await client.query(schema);
    
    console.log('✅ Schema created successfully!');
    console.log('');
    console.log('Tables created:');
    console.log('  - tb.ted_staging_std (cleaned data)');
    console.log('  - tb.ted_raw_xml (raw XML storage)');
    console.log('  - tb.ted_parsed (legacy)');
    console.log('');
    console.log('🚀 Next: Test with node ingest_daily_package.mjs --date=2025-10-10');
  } catch (err) {
    console.error('❌ Error:', err.message);
    console.error('Full error:', err);
    process.exit(1);
  } finally {
    await client.end();
  }
}

runSchema();
