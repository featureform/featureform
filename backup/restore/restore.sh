#!/bin/bash

echo "WARNING: Executing on context $(kubectl config current-context)"
echo "This will overwrite any data on the cluster"
echo "NOTE: Requires Go 1.18 to run"

if [[ $* != *--force* ]]; then
  echo "Press 'y' to continue"
  while : ; do
    read -n 1 k <&1
    if [[ $k = y ]] ; then
      break
    else
      exit 1
    fi
  done
fi

kubectl port-forward svc/featureform-etcd 2379:2379 &

go run main.go
