#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

# This script checks if all Dockerfiles have the same HADOOP_HOME path
# The purpose of this is to make sure that all Dockerfiles are using the same 
# HADOOP_HOME path. This is important because HDFS utilizes the cli directly 
# to perform operations for the HDFS Filestore.


# Define the expected HADOOP_HOME path
EXPECTED_HADOOP_HOME="/usr/local/hadoop"

# List all Dockerfiles in the current directory
DOCKERFILES=("./coordinator/Dockerfile.old" "./serving/Dockerfile" "./runner/Dockerfile")

SCRIPT_FAILED=0
PASSED_CHECKMARK="\xE2\x9C\x94"
FAILED_CROSS="\xE2\x9C\x97"

# Function to print formatted line
print_formatted() {
    local path="$1"
    local symbol="$2"
    printf "%-30s %s\n" "$path" $(echo -e $symbol)
}

# Loop through each Dockerfile
for DOCKERFILE in ${DOCKERFILES[@]}; do
  # Check if the Dockerfile contains the expected HADOOP_HOME
  if grep -q "ENV HADOOP_HOME=$EXPECTED_HADOOP_HOME" "$DOCKERFILE"; then
    print_formatted $DOCKERFILE $PASSED_CHECKMARK
  else
    print_formatted $DOCKERFILE $FAILED_CROSS
    SCRIPT_FAILED=1
  fi
done

if [ $SCRIPT_FAILED -eq 1 ]; then
  echo -e "\nAll Dockerfiles DO NOT have the same HADOOP_HOME"
  exit 1
fi
