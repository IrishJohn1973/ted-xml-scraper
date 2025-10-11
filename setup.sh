#!/bin/bash
# Database setup script for TED XML Scraper

set -e

echo "🗄️  TED XML Scraper - Database Setup"
echo "===================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    exit 1
fi

# Load environment variables
set -a
source .env
set +a

# Validate required variables
if [ -z "$SUPA_DB_HOST" ] || [ -z "$SUPA_DB" ] || [ -z "$SUPA_DB_USER" ] || [ -z "$SUPA_DB_PASS" ]; then
    echo "❌ Error: Missing required environment variables"
    exit 1
fi

# Build connection string
DB_URL="postgresql://${SUPA_DB_USER}:${SUPA_DB_PASS}@${SUPA_DB_HOST}:5432/${SUPA_DB}?sslmode=require"

echo "📋 Database: ${SUPA_DB_HOST}"
echo ""
echo "⚠️  WARNING: This will DROP the existing 'tb' schema and all its data!"
echo ""
read -p "Are you sure you want to continue? Type 'yes' to proceed: " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Setup cancelled"
    exit 0
fi

echo ""
echo "🔧 Running schema.sql..."

psql "$DB_URL" -f schema.sql

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Database schema created successfully!"
    echo ""
    echo "🚀 Next: Test with 'node ingest_daily_package.mjs --date=2025-10-10'"
else
    echo "❌ Error creating schema"
    exit 1
fi
