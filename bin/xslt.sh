#!/usr/bin/env bash
set -euo pipefail
in_xml="${1:?input XML required}"
out_xml="${2:?output XML required}"
BASE="$(dirname "$0")/.."
CP="$BASE/tools/saxon/saxon-he.jar:$BASE/tools/saxon/xmlresolver.jar"
exec java -cp "$CP" net.sf.saxon.Transform \
  -s:"$in_xml" \
  -xsl:"$BASE/xslt/ted_to_simple_ubl.xsl" \
  -o:"$out_xml"
