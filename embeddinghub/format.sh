#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

set -e
find . -type f | grep -e '.*\.cc' -e '.*\.h' | grep -v "node_modules" | xargs clang-format -i --sort-includes -style=google;
find . -type f | grep -e '.py$' | grep -v "node_modules" | xargs python -m yapf -i --style google;
bazelisk run :buildifier
