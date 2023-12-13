#!/bin/bash

# Function to get the next version based on the input parameters
get_next_version() {
    local release_branch=$1
    local deployment_type=$2
    local last_release
    local next_version

    # Check if the release branch exists on Docker Hub
    if docker pull your_docker_image:"$release_branch" > /dev/null 2>&1; then
        # Get the last release version
        last_release=$(docker run --rm your_docker_image:"$release_branch" cat /version.txt 2>/dev/null || echo "0.0.0")

        # Extract major, minor, and patch versions
        major=$(echo "$last_release" | cut -d '.' -f 1)
        minor=$(echo "$last_release" | cut -d '.' -f 2)
        patch=$(echo "$last_release" | cut -d '.' -f 3 | cut -d '-' -f 1)
        rc=$(echo "$last_release" | cut -d '-' -f 2 | cut -d 'c' -f 2)

        # Determine next version based on deployment type
        if [ "$deployment_type" == "pre-release" ]; then
            if [ -z "$rc" ]; then
                # No previous pre-release, start with rc0
                next_version="$major.$minor.$((patch + 1))-rc0"
            else
                # Increment the release candidate
                next_version="$major.$minor.$patch-rc$((rc + 1))"
            fi
        elif [ "$deployment_type" == "release" ]; then
            # Increment the patch version for the release
            next_version="$major.$minor.$((patch + 1))"
        else
            echo "Invalid deployment type. Please specify 'pre-release' or 'release'."
            exit 1
        fi

        echo "Next $deployment_type version for $release_branch: $next_version"
    else
        # If the release branch doesn't exist, assume it's the first release
        echo "No previous release for $release_branch. First $deployment_type version is 0.0.0"
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--branch)
            release_branch="$2"
            shift 2
            ;;
        -d|--deployment)
            deployment_type="$2"
            shift 2
            ;;
        *)
            echo "Invalid argument: $1"
            exit 1
            ;;
    esac
done

# Check if required arguments are provided
if [ -z "$release_branch" ] || [ -z "$deployment_type" ]; then
    echo "Usage: $0 -b|--branch <release_branch> -d|--deployment <pre-release|release>"
    exit 1
fi

# Call the function to get the next version
get_next_version "$release_branch" "$deployment_type"
