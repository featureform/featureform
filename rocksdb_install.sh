#!/bin/bash

if [[ "$OSTYPE" == "linux-gnu"* ]]; then 
        sudo apt-get install libgflags-dev
        sudo apt-get install libsnappy-dev
        sudo apt-get install zlib1g-dev
        sudo apt-get install libbz2-dev
        sudo apt-get install liblz4-dev
        sudo apt-get install libzstd-dev
elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install rocksdb
fi