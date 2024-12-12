#!/bin/bash

set -e

if [ -z "${MIGRATION_VERSION}" ]; then
  ./goose up -dir ./migrations
else
  ./goose up-to "${MIGRATION_VERSION}" -dir ./migrations
fi
