#!/bin/bash
set -e
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    exit 1
fi
export $(cat .env | xargs)
node ingest_daily_package.mjs "$@"
