#!/usr/bin/env bash
set -euo pipefail
if [ $# -ne 2 ]; then
  echo "Usage: $0 <input.xml> <output.xml>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SAXON_JAR="$ROOT_DIR/tools/saxon/saxon-he.jar"
RESOLVER_JAR="$ROOT_DIR/tools/saxon/xmlresolver.jar"
CP="$SAXON_JAR:$RESOLVER_JAR"

exec java -cp "$CP" net.sf.saxon.Transform \
  -xsl:"$ROOT_DIR/xslt/ted_to_simple_ubl.xsl" \
  -s:"$1" \
  -o:"$2"
