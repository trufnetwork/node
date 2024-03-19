# Kwil DB

The database for Web3.

## Overview

Learn more about Kwil at [kwil.com](https://kwil.com).

## Build instructions

### Prerequisites

To build Kwil, you will need to install:

1. [Go](https://golang.org/doc/install)
2. [Taskfile](https://taskfile.dev/installation)

### Build

Invoke `task` command to see all available tasks. The `build` task will compile `kwild`. They will be generated in `.build/`:

```shell
task build
```

## Local deployment

### Run Postgres

Before running the custom kwild binary, you will have to start Postgres.
The Kwil team has provided a default image with the necessary configurations. 
For more information on how to configure your own Postgres database, please refer to the [Postgres setup guide](https://docs.kwil.com/docs/daemon/installation#postgresql).

```
docker run -d -p 5432:5432 --name kwil-postgres -e "POSTGRES_HOST_AUTH_METHOD=trust" \
    kwildb/postgres:latest
```

### Run Kwild

You can start a single node network using the `kwild` binary built in the step above:

```shell
task kwild
```

For more information on running nodes, and how to run a multi-node network, refer to the Kwil [documentation](<https://docs.kwil.com/docs/node/quickstart>).

### Resetting local deployments

You can clear the local Kwil data by running the following command:

```shell
task clear-data
```

### Configure the kwil-cli

To interact with the the TSN-DB, you will need to configure the kwil-cli.
```shell
kwil-cli configure

# Enter the following values:
Kwil RPC URL: http://localhost:8080
Kwil Chain ID: <leave blank>
Private Key: <any ethereum private key>
# use private key 0000000000000000000000000000000000000000000000000000000000000001 for testing
```

## Docker Compose Deployment

### Run TSN-DB with Postgres using Docker Compose

To run the TSN-DB with Postgres using Docker Compose, run the following command:
```shell
task compose
```

This will start the TSN-DB and Postgres in Docker containers, which is already seeded.

#### Seed Data
If you need to manually seed data into the TSN-DB, run the following command:
```shell
task seed
```

## License

The kwil-db repository (i.e. everything outside of the `core` directory) is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.

The kwil golang SDK (i.e. everything inside of the `core` directory) is licensed under the MIT License. See [core/LICENSE.md](core/LICENSE.md) for more details.
