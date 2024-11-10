#!/bin/bash

# go to the script dir
cd "$(dirname "$0")"
# go to the root dir
cd ..

download_binaries() {
    local ARCH=$(uname -m)
    local OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    local URL=

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
    if [[ "$OS" == "linux" ]]; then
        OS="linux"
    elif [[ "$OS" == "darwin" ]]; then
        OS="darwin"
    else
        echo "Unsupported operating system: $OS"
        exit 1
    fi
    
    # Set the URL for the binary
    if [[ "$OS" == "linux" ]] && [[ "$ARCH" == "amd64" ]]; then
        URL="https://www.dropbox.com/scl/fi/ibczzxjkol826mlfn8ity/kgw_0.3.4_linux_amd64.tar.gz?rlkey=t67l0o1yue052pupn0ag7vlq9&st=nkm0s1r2&dl=0"
    elif [[ "$OS" == "linux" ]] && [[ "$ARCH" == "arm64" ]]; then
        URL="https://www.dropbox.com/scl/fi/q99j4mufe8drfvi4fw38m/kgw_0.3.4_linux_arm64.tar.gz?rlkey=u1gudxremhr7jvrovmw66qmbm&st=yy25ad2o&dl=0"
    elif [[ "$OS" == "darwin" ]] && [[ "$ARCH" == "amd64" ]]; then
        URL="https://www.dropbox.com/scl/fi/580oatp39osevyqev4e2p/kgw_0.3.4_darwin_amd64.tar.gz?rlkey=qwtjplh8el11nfynzjwrdzew2&st=csca2bxi&dl=0"
    elif [[ "$OS" == "darwin" ]] && [[ "$ARCH" == "arm64" ]]; then
        URL="https://www.dropbox.com/scl/fi/tcrpnphqzzpktgnxq6uvm/kgw_0.3.4_darwin_arm64.tar.gz?rlkey=y4bbo05zvm6j27iwxcmq65g5c&st=5ig6coef&dl=0"
    else
        echo "Unsupported: $OS $ARCH"
        exit 1
    fi

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
