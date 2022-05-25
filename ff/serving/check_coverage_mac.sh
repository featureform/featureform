#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


# Run this script to check code coverage and open results in the browser

# Run Coverage Test
go test -coverpkg=./... -coverprofile cover.out ./...

# Convert Coverage To HTML
go tool cover -html=cover.out -o coverage.html

# Cleanup Files
rm cover.out

# Open In Browser
open coverage.html