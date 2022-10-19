#!/bin/bash
# set to fail if any command fails
set -e

TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"
export FEATUREFORM_TEST_PATH=$TESTING_DIRECTORY

if [ $# -eq 0 ]; then
    echo -e "Exporting FEATUREFORM_HOST='$FEATUREFORM_URL'"
    export FEATUREFORM_HOST=$FEATUREFORM_HOST_URL
    unset FEATUREFORM_CERT
elif [ $# -eq 2 ]; then
    echo -e "Exporting FEATUREFORM_HOST='$1' and FEATUREFORM_CERT='$2'\n"
    export FEATUREFORM_HOST=$1
    export FEATUREFORM_CERT=$2
fi

for f in $TESTING_DIRECTORY/definitions/*
do
    printf -- '-%.0s' $(seq 100); echo ""
    filename="${f##*/}"
    echo "Applying '$filename' definition"
    featureform apply $f

    echo -e "\nNow serving '$filename'"
    python $TESTING_DIRECTORY/serving.py
    echo -e "Successfully completed '$filename'"
done

echo -e "\n\n"
printf -- '-%.0s' $(seq 100); echo ""

numberOfDefinitions="$(ls -1q $TESTING_DIRECTORY/definitions/* | wc -l)"
echo -e "COMPLETED $numberOfDefinitions definitions"
printf -- '-%.0s' $(seq 100); echo ""
