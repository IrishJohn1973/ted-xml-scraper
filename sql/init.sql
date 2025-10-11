-- TED XML Scraper Database Schema
-- Drop existing schema to start fresh (WARNING: deletes all data)
DROP SCHEMA IF EXISTS tb CASCADE;

-- Create schema
CREATE SCHEMA tb;

-- Raw XML storage table
CREATE TABLE tb.ted_raw_xml (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'ted',
  source_id TEXT NOT NULL,
  xml_text TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ted_raw_xml_unique UNIQUE (source, source_id)
);

-- Parsed data table
CREATE TABLE tb.ted_parsed (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'ted',
  source_id TEXT NOT NULL,
  title TEXT,
  description TEXT,
  buyer_name TEXT,
  buyer_country TEXT,
  cpv_codes TEXT[],
  published_at TIMESTAMPTZ,
  deadline TIMESTAMPTZ,
  url_notice TEXT,
  url_detail TEXT,
  attachments JSONB DEFAULT '[]'::JSONB,
  parsed_json JSONB,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  CONSTRAINT ted_parsed_unique UNIQUE (source, source_id)
);

-- Indexes for better query performance
CREATE INDEX idx_ted_raw_xml_source_id ON tb.ted_raw_xml(source, source_id);
CREATE INDEX idx_ted_raw_xml_inserted_at ON tb.ted_raw_xml(inserted_at);

CREATE INDEX idx_ted_parsed_source_id ON tb.ted_parsed(source, source_id);
CREATE INDEX idx_ted_parsed_buyer_country ON tb.ted_parsed(buyer_country);
CREATE INDEX idx_ted_parsed_published_at ON tb.ted_parsed(published_at);
CREATE INDEX idx_ted_parsed_deadline ON tb.ted_parsed(deadline);
CREATE INDEX idx_ted_parsed_cpv_codes ON tb.ted_parsed USING GIN(cpv_codes);
CREATE INDEX idx_ted_parsed_json ON tb.ted_parsed USING GIN(parsed_json);

-- Optional: Create a view for easy querying
CREATE VIEW tb.ted_summary AS
SELECT 
  p.id,
  p.source_id,
  p.title,
  p.buyer_name,
  p.buyer_country,
  p.published_at,
  p.deadline,
  p.url_notice,
  CASE 
    WHEN p.deadline IS NOT NULL AND p.deadline > NOW() THEN 'Open'
    WHEN p.deadline IS NOT NULL AND p.deadline <= NOW() THEN 'Closed'
    ELSE 'Unknown'
  END AS status,
  p.cpv_codes,
  r.inserted_at AS scraped_at
FROM tb.ted_parsed p
LEFT JOIN tb.ted_raw_xml r ON p.source_id = r.source_id
ORDER BY p.published_at DESC NULLS LAST;

-- Grant permissions (adjust user as needed)
-- GRANT USAGE ON SCHEMA tb TO your_app_user;
-- GRANT ALL ON ALL TABLES IN SCHEMA tb TO your_app_user;
-- GRANT ALL ON ALL SEQUENCES IN SCHEMA tb TO your_app_user;
