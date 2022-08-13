#!/bin/bash
pip uninstall featureform -y
rm -r client/dist/*
cd dashboard && npm run build
cd ../
mkdir -p client/src/featureform/dashboard/
cp -r dashboard/out client/src/featureform/dashboard/
python3 -m build ./client/
pip install client/dist/*.whl
