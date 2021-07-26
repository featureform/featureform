#!/bin/bash
set -e
find . -type f | grep -e '.*\.cc' -e '.*\.h' | xargs clang-format -i --sort-includes -style=google;
find . -type f | grep -e '.py$' | xargs python3 -m yapf -i --style google;
bazelisk run :buildifier
