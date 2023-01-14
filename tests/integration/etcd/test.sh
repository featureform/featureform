#!/bin/bash

set -e

# Load
go run ./tests/integration/etcd/load.go

# Backup (Change name to backup script name)
go run backup/save/main.go

## Clear etcd
etcdctl del "" --prefix

# Check empty
if [$(etcdctl get "" --prefix | wc -l) -eq 0 ];
then
  echo "ETCD is empty";
else
  echo "ETCD is not empty"
  exit 1
fi

# Restore (change to restore script with args)
go run backup/restore/main.go

# Check Values
go test ./tests/integration/etcd/
