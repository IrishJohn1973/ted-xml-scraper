#!/usr/bin/env bash
set -euo pipefail
URLS_FILE="${1:-urls.txt}"
OUT_DIR="data/raw"
mkdir -p "$OUT_DIR"
i=0
while IFS= read -r url; do
  [[ -z "$url" || "$url" =~ ^# ]] && continue
  i=$((i+1))
  ts="$(date +%Y%m%d%H%M%S)"
  out="$OUT_DIR/ted_${ts}_${i}.xml"
  echo "GET $url -> $out"
  curl -fsSL "$url" -o "$out"
done < "$URLS_FILE"
echo "Done. Saved $i file(s) to $OUT_DIR"
