#!/bin/bash

# Initialize a variable to track if --no-dash is passed
NO_DASH=false

# Loop through all arguments
for arg in "$@"
do
    if [ "$arg" == "--no-dash" ]; then
        NO_DASH=true
    fi
done

pip3 uninstall featureform -y
rm -r client/dist/*

# Check if --no-dash is not passed
if [ "$NO_DASH" == "false" ]; then
    cd dashboard
    rm -r node_modules
    npm i
    npm run build
    cd ../
    mkdir -p client/src/featureform/dashboard/
    cp -r dashboard/out client/src/featureform/dashboard/
fi

python3 -m build ./client/
pip3 install client/dist/*.whl
