#!/bin/bash

prefix="release-"  # Specify the prefix you want to filter

# Check if the prefix is provided as an argument
if [ $# -eq 1 ]; then
  prefix="$1"
fi

# List branches with the specified prefix
branches=$(git for-each-ref --format="%(refname:short)" refs/heads/"$prefix"*)

# Sort the branches numerically
sorted_branches=$(echo "$branches" | grep -oE "$prefix[0-9.]+" | sort -t/ -k2n)

# Get the most recent branch (the last one in the sorted list)
most_recent_branch=$(echo "$sorted_branches" | tail -n 1)

# Print the most recent branch
echo "$most_recent_branch"
