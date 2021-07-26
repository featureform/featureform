#!/bin/bash

set -e

# Install RocksDB dependencies
if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -x "$(command -v apt-get)" ]]; then 
        sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install gflags lz4 snappy zstd
else
        echo "OS Type not supported"
        exit 1
fi

# Install sphinx to generate docs
pip3 install sphinx sphinxcontrib-napoleon

# Install protobuf for use in docs in this case
pip3 install protobuf

# Install yapf for formatting
pip3 install yapf

# Install clang-format for formatting
if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -x "$(command -v apt-get)" ]]; then 
        sudo apt-get install clang-format
elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install clang-format
else
        echo "OS Type not supported"
        exit 1
fi
