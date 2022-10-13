#!/bin/bash
# set to fail if any command fails
set -e

TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"

for f in $TESTING_DIRECTORY/definitions/*
do
    printf -- '-%.0s' $(seq 100); echo ""
    filename="${f##*/}"
    echo "Applying $filename definition"
    featureform apply $f

    echo -e "\nNow serving $filename for $TEST_CASE_VERSION version"
    python $TESTING_DIRECTORY/serving.py
    echo -e "Successfully completed $filename for $TEST_CASE_VERSION version."
done

echo -e "\n\n"
printf -- '-%.0s' $(seq 100); echo ""

numberOfDefinitions="$(ls -1q $TESTING_DIRECTORY/definitions/* | wc -l)"
echo -e "COMPLETED $numberOfDefinitions definitions"
printf -- '-%.0s' $(seq 100); echo ""
