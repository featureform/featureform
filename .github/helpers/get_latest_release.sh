#!/bin/bash

prefix="release/"  # Replace 'your-prefix' with the desired prefix

# Check if the prefix is provided as an argument
if [ $# -eq 1 ]; then
  prefix="$1"
fi

branches=()

# Iterate through all branches and filter by the specified prefix
for branch in $(git for-each-ref --format="%(refname:short)" refs/heads/); do
  if [[ "$branch" == "$prefix"* ]]; then
    branches+=("$branch")
  fi
done

# Sort the list in descending order
latest_value=$(printf "%s\n" "${branches[@]}" | sort -t. -k1,1 -k2,2nr | head -n 1)

echo $latest_value

