#!/bin/bash

##### File reading ignores the release number command line argument
# Function to get the next version based on the input parameters
get_next_version_from_file() {
    local release_branch=$1
    local deployment_type=$2
    local last_release
    local next_version

    ##### Check if a file exists, create it if it doesnt, read from it if it does

    file_path="release_version.txt"

    # Check if the file exists
    if [ ! -e "$file_path" ]; then
        # Create the file if it doesn't exist
        touch "$file_path"
        echo "File $file_path created."
    fi

    file_content=$(cat $file_path)


    ##### Read the last release or pre lease version depending on the tag

    if [ "$deployment_type" == "pre-release" ]; then
        last_release=$( echo "$file_content" | grep "^prerelease=" | cut -d '=' -f 2)

    elif [ "$deployment_type" == "release" ]; then
        last_release=$( echo "$file_content" | grep "^release=" | cut -d '=' -f 2)
    
    else
        echo "Invalid deployment type. Please specify 'pre-release' or 'release'."
        exit 1
    fi 

    echo "The last release is $last_release"


    ##### Extract the values of major, minor, patches

    major=$(echo "$last_release" | cut -d '.' -f 1)
    minor=$(echo "$last_release" | cut -d '.' -f 2)
    patch=$(echo "$last_release" | cut -d '.' -f 3 | cut -d '-' -f 1)
    rc=$(echo "$last_release" | cut -d '.' -f 3 | cut -d 'c' -f 2)


    ##### Pre release: math to get the next version

    # Determine next version based on deployment type
    if [ "$deployment_type" == "pre-release" ]; then
        if [ "$last_release" == "" ]; then
            next_version="$release_branch.0-rc0"
        else
            # Increment the release candidate
            next_version="$major.$minor.$patch-rc$((rc + 1))"
        fi
        write_to_file="${file_content/prerelease=$last_release/prerelease=$next_version}"
        echo "$write_to_file" > "$file_path"


    ##### Release: math to get the next version

    elif [ "$deployment_type" == "release" ]; then
        if [ "$last_release" == "" ]; then
            next_version="$release_branch.0"
        else
            # Increment the patch version for the release
            next_version="$major.$minor.$((patch + 1))"
        fi
        write_to_file="${file_content/release=$last_release/release=$next_version}"
        echo "$write_to_file" > "$file_path"

    else
        echo "Invalid deployment type. Please specify 'pre-release' or 'release'."
        exit 1
    fi

    echo "Next version is $next_version"
}

##### Parse command line arguments
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

##### Check if required arguments are provided
if [ -z "$release_branch" ] || [ -z "$deployment_type" ]; then
    echo "Usage: $0 -b|--branch <release_branch> -d|--deployment <pre-release|release>"
    exit 1
fi

# Call the function to get the next version
get_next_version_from_file "$release_branch" "$deployment_type"
