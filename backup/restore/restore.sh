#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

echo "WARNING: Executing on context $(kubectl config current-context)"
echo "This will overwrite any data on the cluster"
echo "NOTE: Requires Go 1.21 to run"

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
