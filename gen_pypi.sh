#!/bin/bash

./gen_grpc.sh
rm ./client/dist/*
cd dashboard && npm run build
cd ../
mkdir -p client/src/featureform/dashboard/
cp -r dashboard/out client/src/featureform/dashboard/
set -e
python3 -m build ./client/
#python3 -m twine upload -r pypi client/dist/*