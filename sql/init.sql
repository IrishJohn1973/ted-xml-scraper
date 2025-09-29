create schema if not exists tb;

create table if not exists tb.ted_raw_xml (
  id bigserial primary key,
  source text not null default 'ted',
  source_id text not null,
  xml_text text not null,
  inserted_at timestamptz not null default now(),
  unique (source, source_id)
);

create table if not exists tb.ted_parsed (
  id bigserial primary key,
  source text not null default 'ted',
  source_id text not null,
  title text,
  description text,
  buyer_name text,
  buyer_country text,
  cpv_codes text[],                 -- normalize later if you want jsonb[]
  published_at timestamptz,
  deadline timestamptz,
  url_notice text,
  url_detail text,
  attachments jsonb default '[]'::jsonb,
  parsed_json jsonb,                -- full object as parsed by Saxon pipeline
  inserted_at timestamptz not null default now(),
  updated_at timestamptz,
  unique (source, source_id)
);
