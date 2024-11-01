#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

lines=$(etcdctl get "" --prefix | wc -l)
if [[ "lines" -eq 0 ]] ;
then
  echo "ETCD is empty";
else
  echo "ETCD is not empty"
  exit 1
fi