#!/bin/bash
pip3 uninstall featureform -y
rm -r client/dist/*
set -e
cd dashboard && npm run build
cd ../
mkdir -p client/src/featureform/dashboard/
cp -r dashboard/out client/src/featureform/dashboard/
python3 -m build ./client/
pip3 install client/dist/*.whl
