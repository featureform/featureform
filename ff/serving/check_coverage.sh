#!/bin/bash

# Run this script to check code coverage and open results in the browser

# Run Coverage Test
go test -coverpkg=./... -coverprofile cover.out ./...

# Convert Coverage To HTML
go tool cover -html=cover.out -o coverage.html

# Cleanup Files
rm cover.out

# Open In Browser
open coverage.html