#!/bin/bash

# Get current date in seconds
current_date=$(date +%s)

# Fetch open PRs using the GitHub CLI
PRs=$(gh pr list --json title,author,createdAt,updatedAt,url --state open)

# Filter PRs based on criteria and save to a temporary array
filtered_prs=()
while IFS= read -r pr; do
    filtered_prs+=("$pr")
done < <(echo "$PRs" | jq -c ".[] | select((.updatedAt | fromdateiso8601) < ($current_date - 86400) and (.createdAt | fromdateiso8601) < ($current_date - 172800))")

# Count the number of PRs
num_prs=${#filtered_prs[@]}

# Start the JSON output
echo -n "{\"data\":\"Number of Open PRs: $num_prs\\n"

# Process each PR from the filtered list
for pr in "${filtered_prs[@]}"; do
    title=$(echo "$pr" | jq -r .title)
    author=$(echo "$pr" | jq -r .author.login)
    url=$(echo "$pr" | jq -r .url)
    created_at_seconds=$(echo "$pr" | jq -r '.createdAt | fromdateiso8601')
    updated_at_seconds=$(echo "$pr" | jq -r '.updatedAt | fromdateiso8601')
    days_open=$(( (current_date - created_at_seconds) / 86400 ))
    last_modified=$(( (current_date - updated_at_seconds) / 86400 ))

    # Combine title and URL for the PR name
    name="$url : ($title) "

    # Output the PR data as a JSON string
    printf "\\n%s :\n\tAuthor: %s | Opened: %d days ago | Last Modified: %d days ago\n" "$name" "$author" "$days_open" "$last_modified" | sed 's/"/\\"/g' # Escape any existing double quotes
done

# End the JSON output
echo '"}'
