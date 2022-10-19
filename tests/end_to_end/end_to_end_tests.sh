#!/bin/bash
# set to fail if any command fails
set -e

TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"
export FEATUREFORM_TEST_PATH=$TESTING_DIRECTORY


if [ $# -eq  ]; then
    echo -e "Exporting FEATUREFORM_HOST='$FEATUREFORM_URL'"
    export FEATUREFORM_HOST=$FEATUREFORM_HOST_URL
elif [ $# -eq 2 ]; then
    echo -e "Exporting FEATUREFORM_HOST='$1' and FEATUREFORM_CERT='$2'\n"
    export FEATUREFORM_HOST=$1
    export FEATUREFORM_CERT=$2
fi

export ETCD_VER="v3.4.19"
export GOOGLE_URL="https://storage.googleapis.com/etcd"

rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
rm -rf /tmp/etcd-download-test && mkdir -p /tmp/etcd-download-test

curl -L ${GOOGLE_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /tmp/etcd-download-test --strip-components=1
rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz

/tmp/etcd-download-test/etcd --version
/tmp/etcd-download-test/etcdctl version

for f in $TESTING_DIRECTORY/definitions/*
do
    printf -- '-%.0s' $(seq 100); echo ""
    filename="${f##*/}"
    echo "Applying '$filename' definition"
    featureform apply $f

    echo -e "\nDumping ETCD keys"
    /tmp/etcd-download-test/etcdctl --user=root:secretpassword get "" --prefix

    echo -e "\nNow serving '$filename'"
    python $TESTING_DIRECTORY/serving.py
    echo -e "Successfully completed '$filename'"
done

echo -e "\n\n"
printf -- '-%.0s' $(seq 100); echo ""

numberOfDefinitions="$(ls -1q $TESTING_DIRECTORY/definitions/* | wc -l)"
echo -e "COMPLETED $numberOfDefinitions definitions"
printf -- '-%.0s' $(seq 100); echo ""
