#!/bin/bash

set -euo pipefail

# Ensure goose binary is available
if ! command -v ./goose &> /dev/null; then
    echo "Error: goose binary not found in current directory" >&2
    exit 1
fi

echo "Starting database migration..."

if [ -z "${MIGRATION_VERSION}" ]; then
  echo "No specific version specified, migrating to latest version"
  ./goose up -dir ./migrations
else
  echo "Migrating to version: ${MIGRATION_VERSION}"
  ./goose up-to "${MIGRATION_VERSION}" -dir ./migrations
fi

echo "Migration completed successfully"