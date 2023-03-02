#!/bin/bash

lines=$(etcdctl get "" --prefix | wc -l)
if [[ "lines" -eq 0 ]] ;
then
  echo "ETCD is empty";
else
  echo "ETCD is not empty"
  exit 1
fi