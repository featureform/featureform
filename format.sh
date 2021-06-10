#!/bin/bash
set -e
find . -type f | grep -e '.*\.cc' -e '.*\.h' | xargs clang-format -i --sort-includes -style=google;
find . -type f | grep -e '.py$' | xargs yapf -i --style google;
bazelisk run :buildifier
