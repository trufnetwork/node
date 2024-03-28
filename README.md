# TSN DB

The database for Truflation Stream Network (TSN). It is built on top of the Kwil framework.

## Overview

Learn more about Truflation at [Truflation.com](https://truflation.com)

## Build instructions

### Prerequisites

To build and run the TSN-DB, you will need the following installed on your system:

1. [Go](https://golang.org/doc/install)
2. [Taskfile](https://taskfile.dev/installation)
3. [Docker Compose](https://docs.docker.com/compose)
4. [Python](https://www.python.org/downloads) (optional for running the seed script)
5. [Pandas](https://pandas.pydata.org) (optional for running the seed script)

### Build Locally

#### Run With Docker Compose (Recommended)

To run the TSN-DB using Docker Compose, run the following command:
```
task compose
```
It will build and start the TSN-DB in Docker containers, which is already seeded.

#### Build and Run the TSN-DB without Docker Compose

Alternatively, you can build and run the TSN-DB without Docker Compose. 
This is useful if you want to run the TSN-DB locally without Docker. i.e. for development or debugging purposes.
To build and run the TSN-DB without Docker Compose, follow the steps below:

##### Build the binary
Invoke `task` command to see all available tasks. The `build` task will compile the binary for you. They will be generated in `.build/`:

```shell
task # list all available tasks
task build # build the binary
```

##### Run Postgres

Before running the, you will have to start Postgres. You can start Postgres using the following command:
```
task postgres
```

##### Run Kwild
You can start a single node network using the `kwild` binary built in the step above:

```shell
task kwild
```

##### Resetting local deployments

You can clear the local data by running the following command:

```shell
task clear-data
```

##### Configure the kwil-cli

To interact with the the TSN-DB, you will need to configure the kwil-cli.
```shell
kwil-cli configure

# Enter the following values:
Kwil RPC URL: http://localhost:8080
Kwil Chain ID: <leave blank>
Private Key: <any ethereum private key>
# use private key 0000000000000000000000000000000000000000000000000000000000000001 for testing
```

##### Seed Data
If you need to manually seed data into the TSN-DB, run the following command:
```shell
task seed
```

## License

The tsn-db repository is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.
