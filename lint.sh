#!/bin/bash
set -e
find . -type f | grep -e '.*\.cc' -e '.*\.h' | xargs clang-format --sort-includes -style=google -Werror -n;
find . -type f | grep -e '.py$' | xargs yapf --style google -q;
bazelisk test :buildifier_test
