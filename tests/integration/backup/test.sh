#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

set -e

# Load
echo "Loading Data...."
go run ./tests/integration/backup/load.go
echo ""

# Backup (Change name to backup script name)
echo "Running Backup...."
go run backup/save/main.go
echo ""

# Restore (change to restore script with args)
echo "Running Restore...."
go run backup/restore/main.go
echo ""

# Check Values
echo "Checking Values...."
go test ./tests/integration/backup/ -tags=backup
echo ""
