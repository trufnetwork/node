#!/bin/bash

# go to the script dir
cd "$(dirname "$0")"
# go to the root dir
cd ..

download_binaries() {
    local ARCH=$(uname -m)
    local OS=$(uname -s | tr '[:upper:]' '[:lower:]')

    # Determine the architecture
    if [[ "$ARCH" == "x86_64" ]]; then
        ARCH="amd64"
    elif [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
        ARCH="arm64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi

    # Determine the operating system
    # Map darwin to linux, since only linux binaries are available.
    if [[ "$OS" == "linux" ]]; then
        OS="linux"
    elif [[ "$OS" == "darwin" ]]; then
        OS="darwin"
    else
        echo "Unsupported operating system: $OS"
        exit 1
    fi

    # Set the URL for the binary
    URL="https://www.dropbox.com/scl/fi/ibczzxjkol826mlfn8ity/kgw_0.3.4_${OS}_${ARCH}.tar.gz?rlkey=qwtjplh8el11nfynzjwrdzew2&st=6dmd4q0y&dl=0"

    echo "Detected platform: ${OS}-${ARCH}"
    echo "Downloading binary from $URL..."

    wget -O kgw.tar.gz $URL

    if [[ $? -eq 0 ]]; then
        echo "Binary downloaded successfully"

        tar -xzvf kgw.tar.gz 'kgw'
        mkdir -p ./.build
        mv ./kgw .build
        rm ./kgw.tar.gz

        chmod +x ./.build/kgw
    else
        echo "Failed to download binary"
        exit 1
    fi
}

download_binaries
