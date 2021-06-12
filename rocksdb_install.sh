#!/bin/bash

set -e

if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -x "$(command -v apt-get)" ]]; then 
        sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install gflags lz4 snappy zstd
else
        echo "OS Type not supported"
        exit 1
fi