#!/bin/bash
pip3 uninstall featureform -y
rm -r client/dist/*
cd dashboard
rm -r node_modules
npm i
npm run build
cd ../
mkdir -p client/src/featureform/dashboard/
cp -r dashboard/out client/src/featureform/dashboard/
python3 -m build ./client/
pip3 install client/dist/*.whl
