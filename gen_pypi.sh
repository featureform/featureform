#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

./gen_grpc.sh
rm ./client/dist/*
cd dashboard && npm run build
cd ../
mkdir -p client/src/featureform/dashboard/
cp -r dashboard/out client/src/featureform/dashboard/
set -e
python3 -m build ./client/
python3 -m twine upload -r pypi client/dist/*