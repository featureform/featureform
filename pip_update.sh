#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 


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
pip3 uninstall featureform-enterprise -y
rm -r client/dist/*


python3 -m build ./client/
pip3 install client/dist/*.whl
