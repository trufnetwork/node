#!/bin/bash

# go to the script dir
cd "$(dirname "$0")"
# go to the root dir
cd ..

BINARY=${1:-}

download_binaries() {
    local ARCH=$(uname -m)
    local OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    local BINARY_NAME=""

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

    if [ "$BINARY" = "kwil-cli" ] && [ "$OS" = "darwin" ]; then
        BINARY_NAME="kwil-cli"
    else 
        BINARY_NAME="kwil-admin"
    fi

    # Set the URL for the binary
    URL="https://github.com/kwilteam/kwil-db/releases/download/v0.8.4/kwil-db_0.8.4_${OS}_${ARCH}.tar.gz"

    echo "Detected platform: ${OS}-${ARCH}"
    echo "Downloading binary from $URL..."

    wget -O kwil-db.tar.gz $URL

    if [[ $? -eq 0 ]]; then
        echo "Binary downloaded successfully"

        tar -xzvf kwil-db.tar.gz $BINARY_NAME
        mkdir -p ./.build
        mv ./$BINARY_NAME .build

        rm ./kwil-db.tar.gz

        chmod +x ./.build/$BINARY_NAME
    else
        echo "Failed to download binary"
        exit 1
    fi
}

download_binaries