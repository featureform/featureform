#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

echo "Installing Python packages"
sudo pip3 install boto3 dill azure-storage-blob==12.13.1 google-cloud-storage==2.7.0