#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

set -e

if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -x "$(command -v apt-get)" ]]; then 
        sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install gflags lz4 snappy zstd
else
        echo "OS Type not supported"
        exit 1
fi