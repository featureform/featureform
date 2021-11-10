#!/bin/bash
set -e
find . -type f | grep -e '.*\.cc' -e '.*\.h' | grep -v 'node_modules' | xargs clang-format --sort-includes -style=google -Werror -n;
find . -type f | grep -e '.py$' | grep -v 'node_modules' | xargs python -m yapf --style google -q;
bazelisk test :buildifier_test
