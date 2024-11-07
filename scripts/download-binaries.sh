#!/bin/bash

# go to the script dir
cd "$(dirname "$0")"
# go to the root dir
cd ..

download_binaries() {
    local ARCH=$(uname -m)
    local OS=$(uname -s | tr '[:upper:]' '[:lower:]')

    # Determine the architecture
    # If running on darwin/arm64, map to linux/amd64 to avoid architecture mismatch issue in provided binaries.
    if [[ "$ARCH" == "x86_64" || "$ARCH" == "arm64" ]]; then
        ARCH="amd64"
    elif [[ "$ARCH" == "aarch64" ]]; then
        ARCH="arm64"
    else
        echo "Unsupported architecture: $ARCH"
        exit 1
    fi

    # Determine the operating system
    # Map darwin to linux, since only linux binaries are available.
    if [[ "$OS" == "linux" ||  "$OS" == "darwin" ]]; then
        OS="linux"
    else
        echo "Unsupported operating system: $OS"
        exit 1
    fi

    # Set the URL for the binary
    URL="https://github.com/kwilteam/kwil-db/releases/download/v0.8.4/kwil-db_0.8.4_${OS}_${ARCH}.tar.gz"

    echo "Detected platform: ${OS}-${ARCH}"
    echo "Downloading binary from $URL..."

    wget -O kwil-db.tar.gz $URL

    if [[ $? -eq 0 ]]; then
        echo "Binary downloaded successfully"

        tar -xzvf kwil-db.tar.gz 'kwil-admin'
        mkdir -p ./.build
        mv ./kwil-admin .build
        rm ./kwil-db.tar.gz

        chmod +x ./.build/kwil-admin
    else
        echo "Failed to download binary"
        exit 1
    fi
}

download_binaries