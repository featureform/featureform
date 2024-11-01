#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

docker build -f ./api/Dockerfile . -t local/api-server:stable &
docker build -f ./dashboard/Dockerfile . -t local/dashboard:stable &
docker build -f ./coordinator/Dockerfile . -t local/coordinator:stable &
docker build -f ./metadata/Dockerfile . -t local/metadata:stable &
docker build -f ./metadata/dashboard/Dockerfile . -t local/metadata-dashboard:stable &
docker build -f ./serving/Dockerfile . -t local/serving:stable &
docker build -f ./runner/Dockerfile . -t local/worker:stable

