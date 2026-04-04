#!/bin/sh
# On first deploy: copy bundled crm.db to the persistent volume
if [ -n "$DB_PATH" ] && [ ! -f "$DB_PATH" ]; then
  echo "First run: copying crm.db to $DB_PATH"
  mkdir -p "$(dirname "$DB_PATH")"
  cp "$(dirname "$0")/crm.db" "$DB_PATH"
fi

# Same for uploads dir
UPLOADS="${UPLOADS_PATH:-./public/uploads}"
mkdir -p "$UPLOADS"

node server.js
