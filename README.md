# Kwil DB

The database for Web3.

## Overview

Learn more about Kwil at [kwil.com](https://kwil.com).

## Build instructions

### Prerequisites

To build Kwil, you will need to install:

1. [Go](https://golang.org/doc/install)
2. [Protocol Buffers](https://protobuf.dev/downloads/) (optional), with the `protoc` executable binary on your `PATH`.
3. [Taskfile](https://taskfile.dev/installation)
4. Protocol buffers go plugins and other command line tools. The `tool` task will install the required versions of the tools into your `GOPATH`, so be sure to include `GOPATH/bin` on your `PATH`.

    ```shell
    task tools
    ```

### Build

Invoke `task` command to see all available tasks. The `build` task will compile `kwild`, `kwil-cli`, and `kwil-admin`. They will be generated in `.build/`:

```shell
task build
```

## Local deployment

You can start a single node network using the `kwild` binary built in the step above:

```shell
.build/kwild --autogen
```

For more information on running nodes, and how to run a multi-node network, refer to the Kwil [documentation](<https://docs.kwil.com/docs/node/quickstart>).

## Building and Using Docker Image

To build a Docker image of TSN-DB with seed data, run the following command:

```shell
docker build -t tsn-db:latest . -f ./truflation/docker/tsn.dockerfile
```

To run the Docker image, use the following command:

```shell
docker run --name tsn-db -p 8080:8080 tsn-db:latest
```

## Resetting local deployments

By default, `kwild` stores all data in `~/.kwild`. To reset the data on a deployment, remove the data directory while the node is stopped:

```shell
rm -r ~/.kwild
```

## License

The kwil-db repository (i.e. everything outside of the `core` directory) is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.

The kwil golang SDK (i.e. everything inside of the `core` directory) is licensed under the MIT License. See [core/LICENSE.md](core/LICENSE.md) for more details.
