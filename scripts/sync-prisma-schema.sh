#!/usr/bin/env bash
set -euo pipefail

SRC="${1:-../novoapex/prisma/migrations}"
OUT="internal/db/schema"

mkdir -p "$OUT"
: > "$OUT/schema.sql"

shopt -s nullglob
count=0
for dir in "$SRC"/*/; do
  name="$(basename "$dir")"
  file="$dir/migration.sql"
  if [ -f "$file" ]; then
    {
      echo "-- migration: $name"
      cat "$file"
      echo ""
      echo ""
    } >> "$OUT/schema.sql"
    count=$((count + 1))
  fi
done

if [ "$count" -eq 0 ]; then
  echo "error: no prisma migrations found under $SRC" >&2
  exit 1
fi

echo "concatenated $count migrations into $OUT/schema.sql"
