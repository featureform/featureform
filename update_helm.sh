#!/bin/bash
set -e
gsutil cp -r gs://featureform-helm ./
sed -i -e "s/0.0.0/$1/g" ./charts/featureform/values.yaml # Sets the default image tag value in the chart
helm package ./charts/featureform -d featureform-helm --app-version $1 --version $1
rm ./featureform-helm/index.yaml
helm repo index ./featureform-helm
gsutil cp ./featureform-helm/index.yaml gs://featureform-helm/index.yaml
gsutil cp -n ./featureform-helm/* gs://featureform-helm
