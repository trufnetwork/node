#!/bin/bash

# go to the script dir
cd "$(dirname "$0")"
# go to the root dir
cd ..

# Detect architecture and operating system
ARCH_RAW=$(uname -m)
OS_RAW=$(uname -s | tr '[:upper:]' '[:lower:]')

case "$ARCH_RAW" in
    x86_64) ARCH="amd64";;
    aarch64|arm64) ARCH="arm64";;
    *) echo "Unsupported architecture: $ARCH_RAW"; exit 1;;
 esac

case "$OS_RAW" in
    linux) OS="linux";;
    darwin) OS="darwin";;
    *) echo "Unsupported operating system: $OS_RAW"; exit 1;;
esac

PLATFORM="${OS}_${ARCH}"

# Helper to get download URL for a given tool
get_url() {
    local TOOL="$1"
    case "$TOOL" in
        kgw)
            case "$PLATFORM" in
                linux_amd64) echo "https://www.dropbox.com/scl/fi/hawzml0uwr2pk8kebqdco/kgw_0.4.1_linux_amd64.tar.gz?rlkey=zkdghw7bn0ml0ojfnvybh4cjg&st=f5qp0zk1&dl=0";;
                linux_arm64) echo "https://www.dropbox.com/scl/fi/4zi8s03j4fqovo36zrcmd/kgw_0.4.1_linux_arm64.tar.gz?rlkey=zsa7ugpklkrr7vfdw7qdgtak8&st=jfvijppe&dl=0";;
                darwin_amd64) echo "https://www.dropbox.com/scl/fi/fa6dddlo48bv2b6usc1wf/kgw_0.4.1_darwin_amd64.tar.gz?rlkey=bupfsaif9wldyhxomawjdhwum&st=wuscf2ns&dl=0";;
                darwin_arm64) echo "https://www.dropbox.com/scl/fi/xkyj7ul6dt08jssiva7oh/kgw_0.4.1_darwin_arm64.tar.gz?rlkey=7l9fcfq8im8f01vkuz6lhm06s&st=zcx2thj3&dl=0";;
                *) return 1;;
            esac;;
        kwil-indexer)
            case "$PLATFORM" in
                # TODO: this file has a temporary token, and it's transient. Once the official new version of indexer is released, we should use that instead.
                linux_amd64) echo "https://kwil-binaries.s3.us-east-2.amazonaws.com/indexer/kwil-indexer_v0.3.0-dev_linux_amd64?response-content-disposition=inline&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEIP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMiJGMEQCIBw%2Fb3o0BFX6EWgLjbMI3BMFKjXi6aGUpw8T5R%2FtkiiSAiAHe04oyetSnVRoNRPtBNGMybJ06WVTBn880%2Bs8acrXKyrMAwgcEAAaDDM0NDM3NTY0NjkzMSIMPYFKRXERzV%2BxFJKJKqkDHao0KFRh41mrDUU73IISqJkCK3sxQ8jOBmeix0HX1Ro19G2xmh5h2kUJ4MKBE%2BXVwi2fslqzxyRvJA%2FBnjhJxogTzx025bIPy41Add4LoIRMWFllBi36oEuXfxHfdttFsgX6nPoKp4b1FFJrXBWCEKgZskK9tWaeB903TrTrFTuoyUK6iYQ3qE2ungdsOt2oO%2BV0tFCDPOeqvFzD3v1MCT0ygwkaThZjup1uGIr2ej8A%2Fkk0b0nQ%2BhrlvZ12m1Iu7Pf2jDTooEWvE6SwbLWPY5gG%2BYsC4G89UNIu7Mj5m1BcDq%2BdVzi4sPRhmc8U9tvc3EFzPJNyAawwhA8%2B404A0mRxekbE5WfBN2fonE0%2BynrTMs5eG32ulW%2Bt0k3hBB8%2BH6jK6M8lXgVZHn19urUrMGYKFifQUb7fuwDhViFwvAy1rLkNPuY5hPNbv38EJYuUnsxGlETJLNX2NMkK32P0WDM38Xjjoj1J2qO6pEtXBe1rXeNyJo%2F88HWdl2knB4MjM0T07exItFt9qz2Rytp%2BmKnQ%2FFqCz7Taq0hgK0BQ4e8hh9stnlRZR2MwjuypwAY63wLJVKINLcxX45IYV8fSOgj%2F9bgwUiJWUziFuVHosBF%2Bm9F0bOSzuypacDC1EVbxHzHeK4AGs2jwYNNU7pIvJdPTY3RaVA5H40WjzuTKaG4jEznS%2FPvmk%2BjAGdxq5nVeTKeZ5D83vPM%2BTSpf6%2F2KiXc%2FWitZP5GYVbq0HjBbeMciwpUgGPejLQIClT5AuEkRmmEsjXg185jS23awUdM3R5QZb2LBSrcEBUDs9OMD0QfzayPy3TxAXpbUwlyDM%2FsbHgfV%2Bu28IQ5s5uoKNcFae8X0jpsRapham8cqd4Y9vp%2B6D30bQ9xDFI0wG8B7lmVvwfaPPKDrTZpZE6PWXZCzRFt7X0xsqltDpf3IwC22QElDrU38A3WDOUR2kKOaJ%2BG%2Fa%2Br5g0wUrM54hWvv9nXm9tUKvBaWSqlPfRIFJ6nS%2FopSrK39vlt93rrprbA6umrR4jhU5ET%2BcQPa2sOhSm52O9g%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAVALTDLLJVVNUW4EU%2F20250424%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250424T183545Z&X-Amz-Expires=43200&X-Amz-SignedHeaders=host&X-Amz-Signature=b9317f54e1850d4992f13afd107205a0c494cea06da050be20c48eb35b65ff56";;
                *) return 1;;
            esac;;
        *) return 1;;
    esac
}

# Generic download function
download_tool() {
    local TOOL="$1"
    local URL

    URL=$(get_url "$TOOL") || { echo "Unsupported platform for $TOOL: $PLATFORM"; exit 1; }

    echo "Detected platform for $TOOL: $PLATFORM"
    echo "Downloading $TOOL from $URL..."
    wget -O "${TOOL}.tar.gz" "$URL" || { echo "Failed to download $TOOL"; exit 1; }

    tar -xzvf "${TOOL}.tar.gz" "$TOOL"
    mkdir -p ".build"
    mv "./$TOOL" ".build"
    rm "${TOOL}.tar.gz"
    chmod +x ".build/$TOOL"
}

# Parse arguments
DOWNLOAD_INDEXER=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --indexer) DOWNLOAD_INDEXER=true;;
        *) echo "Unknown option: $1"; exit 1;;
    esac
    shift
done

# Download core tool
download_tool kgw

# Optionally download indexer
if [[ "$DOWNLOAD_INDEXER" == true ]]; then
    download_tool kwil-indexer
fi
