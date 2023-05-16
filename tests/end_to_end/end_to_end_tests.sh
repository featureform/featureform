#!/bin/bash
# set to fail if any command fails
set -e

TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"
export FEATUREFORM_TEST_PATH=$TESTING_DIRECTORY

if [ $# -eq 2 ]; then
    echo -e "Exporting FEATUREFORM_HOST='$1' and FEATUREFORM_CERT='$2'\n"
    export FEATUREFORM_HOST=$1
    export FEATUREFORM_CERT=$2
fi

for f in $TESTING_DIRECTORY/definitions/*
do
    printf -- '-%.0s' $(seq 100); echo ""
    filename="${f##*/}"
    echo "Applying '$filename' definition"
    if [ $# -eq 2 ]; then
        # we need to do this on Minikube in order to avoid the grpc failure because of the wait.
        # this is a temporary fix until we can figure out why the wait is failing on Minikube.
        featureform apply --no-wait $f
    else
        featureform apply $f
    fi
done

for f in $TESTING_DIRECTORY/feature_*
do
  filename="${f##*/}"
  echo -e "\nNow serving '$filename'"
  python $TESTING_DIRECTORY/serving.py $filename
  echo -e "Successfully completed '$filename'"
done

echo -e "\n\n"
printf -- '-%.0s' $(seq 100); echo ""

numberOfDefinitions="$(ls -1q $TESTING_DIRECTORY/definitions/* | wc -l)"
echo -e "COMPLETED $numberOfDefinitions definitions"
printf -- '-%.0s' $(seq 100); echo ""
