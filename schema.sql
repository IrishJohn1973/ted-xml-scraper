-- TED XML Scraper Database Schema
-- Clean structure for European public procurement data
-- Drop existing schema to start fresh (WARNING: deletes all data)
DROP SCHEMA IF EXISTS tb CASCADE;

-- Create schema
CREATE SCHEMA tb;

-- Main staging table for standardized TED notices
-- This is the core table used by ingest_daily_package.mjs and run_yesterday.mjs
CREATE TABLE tb.ted_staging_std (
  -- Primary identifiers
  tb_id TEXT PRIMARY KEY,
  native_id TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'TED',
  
  -- Notice metadata
  is_award BOOLEAN DEFAULT FALSE,
  competition_flag BOOLEAN DEFAULT TRUE,
  published_at TIMESTAMPTZ,
  
  -- Content fields
  title TEXT,
  short_description TEXT,
  full_description TEXT,
  
  -- Buyer/location information
  buyer_country TEXT,
  
  -- Classification
  cpv_main TEXT,
  
  -- Deadline information
  deadline TIMESTAMPTZ,
  raw_deadline_date TEXT,
  raw_deadline_time TEXT,
  
  -- URLs and metadata
  detail_url TEXT,
  run_id TEXT,
  source_row_hash TEXT,
  
  -- Timestamps
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  
  -- Constraints
  CONSTRAINT ted_staging_std_native_id_check CHECK (native_id IS NOT NULL AND native_id != ''),
  CONSTRAINT ted_staging_std_source_check CHECK (source IN ('TED'))
);

-- RAW XML table - stores original XML for future re-processing
CREATE TABLE tb.ted_raw_xml (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL DEFAULT 'ted',
  source_id TEXT NOT NULL,
  xml_text TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ted_raw_xml_unique UNIQUE (source, source_id)
);

-- Legacy parsed table
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

-- Indexes for performance
CREATE INDEX idx_ted_staging_native_id ON tb.ted_staging_std(native_id);
CREATE INDEX idx_ted_staging_tb_id ON tb.ted_staging_std(tb_id);
CREATE INDEX idx_ted_staging_source ON tb.ted_staging_std(source);
CREATE INDEX idx_ted_staging_published_at ON tb.ted_staging_std(published_at DESC NULLS LAST);
CREATE INDEX idx_ted_staging_deadline ON tb.ted_staging_std(deadline) WHERE deadline IS NOT NULL;
CREATE INDEX idx_ted_staging_inserted_at ON tb.ted_staging_std(inserted_at DESC);
CREATE INDEX idx_ted_staging_buyer_country ON tb.ted_staging_std(buyer_country) WHERE buyer_country IS NOT NULL;
CREATE INDEX idx_ted_staging_cpv_main ON tb.ted_staging_std(cpv_main) WHERE cpv_main IS NOT NULL;
CREATE INDEX idx_ted_staging_is_award ON tb.ted_staging_std(is_award);
CREATE INDEX idx_ted_staging_competition_flag ON tb.ted_staging_std(competition_flag);
CREATE INDEX idx_ted_staging_run_id ON tb.ted_staging_std(run_id);
CREATE INDEX idx_ted_staging_source_row_hash ON tb.ted_staging_std(source_row_hash);
CREATE INDEX idx_ted_staging_country_published ON tb.ted_staging_std(buyer_country, published_at DESC) WHERE buyer_country IS NOT NULL;
CREATE INDEX idx_ted_staging_deadline_country ON tb.ted_staging_std(deadline, buyer_country) WHERE deadline IS NOT NULL;
CREATE INDEX idx_ted_raw_xml_source_id ON tb.ted_raw_xml(source, source_id);
CREATE INDEX idx_ted_raw_xml_inserted_at ON tb.ted_raw_xml(inserted_at);
CREATE INDEX idx_ted_parsed_source_id ON tb.ted_parsed(source, source_id);
CREATE INDEX idx_ted_parsed_published_at ON tb.ted_parsed(published_at);

-- Useful views for querying
CREATE VIEW tb.ted_active_competitions AS
SELECT 
  tb_id,
  native_id,
  title,
  short_description,
  buyer_country,
  cpv_main,
  published_at,
  deadline,
  detail_url,
  CASE 
    WHEN deadline IS NULL THEN 'Unknown'
    WHEN deadline > NOW() THEN 'Open'
    ELSE 'Closed'
  END AS status,
  EXTRACT(DAY FROM (deadline - NOW())) AS days_remaining
FROM tb.ted_staging_std
WHERE competition_flag = TRUE
  AND (deadline IS NULL OR deadline > NOW() - INTERVAL '7 days')
ORDER BY deadline ASC NULLS LAST;

CREATE VIEW tb.ted_recent_awards AS
SELECT 
  tb_id,
  native_id,
  title,
  short_description,
  buyer_country,
  cpv_main,
  published_at,
  detail_url
FROM tb.ted_staging_std
WHERE is_award = TRUE
ORDER BY published_at DESC NULLS LAST;

CREATE VIEW tb.ted_summary AS
SELECT 
  COUNT(*) AS total_notices,
  COUNT(*) FILTER (WHERE is_award = TRUE) AS awards,
  COUNT(*) FILTER (WHERE competition_flag = TRUE) AS competitions,
  COUNT(*) FILTER (WHERE deadline > NOW()) AS open_tenders,
  COUNT(DISTINCT buyer_country) AS countries,
  COUNT(DISTINCT cpv_main) AS unique_cpv_codes,
  MAX(published_at) AS latest_publication,
  MAX(inserted_at) AS last_ingested
FROM tb.ted_staging_std;

CREATE VIEW tb.ted_by_country AS
SELECT 
  buyer_country,
  COUNT(*) AS total_notices,
  COUNT(*) FILTER (WHERE is_award = TRUE) AS awards,
  COUNT(*) FILTER (WHERE competition_flag = TRUE) AS competitions,
  COUNT(*) FILTER (WHERE deadline > NOW()) AS open_tenders,
  MAX(published_at) AS latest_notice
FROM tb.ted_staging_std
WHERE buyer_country IS NOT NULL
GROUP BY buyer_country
ORDER BY total_notices DESC;

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION tb.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update updated_at
CREATE TRIGGER update_ted_staging_std_updated_at
  BEFORE UPDATE ON tb.ted_staging_std
  FOR EACH ROW
  EXECUTE FUNCTION tb.update_updated_at_column();
