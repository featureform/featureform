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


python3 -m build ./client/
pip3 install client/dist/*.whl
