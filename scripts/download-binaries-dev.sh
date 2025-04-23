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
        URL="https://www.dropbox.com/scl/fi/hawzml0uwr2pk8kebqdco/kgw_0.4.1_linux_amd64.tar.gz?rlkey=zkdghw7bn0ml0ojfnvybh4cjg&st=f5qp0zk1&dl=0"
    elif [[ "$OS" == "linux" ]] && [[ "$ARCH" == "arm64" ]]; then
        URL="https://www.dropbox.com/scl/fi/4zi8s03j4fqovo36zrcmd/kgw_0.4.1_linux_arm64.tar.gz?rlkey=zsa7ugpklkrr7vfdw7qdgtak8&st=jfvijppe&dl=0"
    elif [[ "$OS" == "darwin" ]] && [[ "$ARCH" == "amd64" ]]; then
        URL="https://www.dropbox.com/scl/fi/fa6dddlo48bv2b6usc1wf/kgw_0.4.1_darwin_amd64.tar.gz?rlkey=bupfsaif9wldyhxomawjdhwum&st=wuscf2ns&dl=0"
    elif [[ "$OS" == "darwin" ]] && [[ "$ARCH" == "arm64" ]]; then
        URL="https://www.dropbox.com/scl/fi/xkyj7ul6dt08jssiva7oh/kgw_0.4.1_darwin_arm64.tar.gz?rlkey=7l9fcfq8im8f01vkuz6lhm06s&st=zcx2thj3&dl=0"
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
