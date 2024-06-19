#!/bin/bash

# List of OSes and architectures
platforms=("linux/amd64" "darwin/amd64" "windows/amd64" "linux/386" "windows/386")

# Name of your Go application
app_name="mExpoter"

# Directory to store build files
output_dir="builds"

# Create output directory if it doesn't exist
mkdir -p $output_dir

for platform in "${platforms[@]}"
do
    platform_split=(${platform//\// })
    GOOS=${platform_split[0]}
    GOARCH=${platform_split[1]}
    output_name=$app_name'-'$GOOS'-'$GOARCH

    if [ $GOOS = "windows" ]; then
        output_name+='.exe'
    fi

    echo "Building for $platform..."
    env GOOS=$GOOS GOARCH=$GOARCH go build -o $output_dir/$output_name main.go

    if [ $? -ne 0 ]; then
        echo "An error has occurred! Aborting the script execution..."
        exit 1
    fi
done

echo "Builds completed successfully."
