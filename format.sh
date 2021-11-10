#!/bin/bash
set -e
find . -type f | grep -e '.*\.cc' -e '.*\.h' | grep -v "node_modules" | xargs clang-format -i --sort-includes -style=google;
find . -type f | grep -e '.py$' | grep -v "node_modules" | xargs python -m yapf -i --style google;
bazelisk run :buildifier
