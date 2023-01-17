#!/bin/bash

if [ "$#" -ne 5 ]; then
    echo "5 arguments required. $# given: "
    echo "./fill templace.sh <CLOUD_PROVIDER> <AZURE_STORAGE_ACCOUNT> <AZURE_STORAGE_TOKEN> <AZURE_CONTAINER> <STORAGE_PATH>"
    exit 1
fi

sed "s/\"cloud-provider\"/$1/g" secret_template.yaml | \
sed "s/\"azure-storage-account\"/$2/g"  | \
sed "s/\"azure-storage-token\"/$3/g" | \
sed "s/\"azure-container\"/$4/g" | \
sed "s/\"storage-path\"/$5/g" > backup_secret.yaml