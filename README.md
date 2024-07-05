# TSN DB

The database for Truflation Stream Network (TSN). It is built on top of the Kwil framework.

## Overview

Learn more about Truflation at [Truflation.com](https://truflation.com)

## Terminology

See [TERMINOLOGY.md](./TERMINOLOGY.md) for a list of terms used in the TSN project.

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
task kwil-binaries # download and extract the kwil binaries
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

If you use Docker Desktop, you can also reset the local deployments by simply deleting containers, images, and volumes.

##### Configure the kwil-cli

To interact with the the TSN-DB, you will need to configure the kwil-cli.
```shell
kwil-cli configure

# Enter the following values:
Kwil RPC URL: http://localhost:8484
Kwil Chain ID: <leave blank>
Private Key: <any ethereum private key>
# use private key 0000000000000000000000000000000000000000000000000000000000000001 for testing
```

#### Run the Kwil Gateway (optional)

Kwil Gateway (KGW) is a load-balancer with authentication ([authn](https://www.cloudflare.com/learning/access-management/authn-vs-authz/)) capability, which enables data privacy protection for a Proof of Authority (POA) Kwil blockchain networks.

Although we use it on our servers, it's not required to be able to develop on the TSN-DB. However, if you want to run the KGW locally or test it, you can follow the instructions in the [Kwil Gateway Directory](./deployments/dev-gateway/README.md)

#### Indexer

Indexer is started by default when you run the TSN-DB using Docker Compose.
Alternatively, you can start the indexer using the following command:
```shell
task indexer
```

You can view the metrics dashboard at http://localhost:1337/v0/swagger. 
Replace `localhost` with the IP address or domain name of the server where the indexer is running.

You can view our deployed indexer at https://staging.tsn.test.truflation.com/v0/swagger. 
There you can see the list of available endpoints and their descriptions. 
For example, you can see the list of transactions by calling the [/chain/transactions](https://staging.tsn.test.truflation.com/v0/chain/transactions) endpoint.

### System Contract

System Contract is a contract that stores the accepted streams by TSN Gov. It also serves as an entry point for queries.

Currently for development purposes, private key 001 will be used to interact with the system contract. 
It still need to be updated to use the correct private key.

#### Fetching through System Contract

As our system contract is currently live on our staging server, you can fetch records from the system contract using the following command:
```shell
kwil-cli database call -a=get_unsafe_record -n=tsn_system_contract -o=34f9e432b4c70e840bc2021fd161d15ab5e19165 data_provider:4710a8d8f0d845da110086812a32de6d90d7ff5c stream_id:st1b148397e2ea36889efad820e2315d date_from:2024-06-01 date_to:2024-06-17 --private-key 0000000000000000000000000000000000000000000000000000000000000001 --provider https://staging.tsn.test.truflation.com
```
in this example, we are fetching records from the system contract for the stream `st1b148397e2ea36889efad820e2315d` 
from the data provider `4710a8d8f0d845da110086812a32de6d90d7ff5c` which is Electric Vehicle Index that provided by 
Truflation Data Provider.

The `unsafe` in the `get_unsafe_record` action is used so that the system contract can fetch records from the stream
contract directly without waiting for the stream contract to be officialized.

list of available actions in the system contract:
- `get_unsafe_record(data_provider, stream_id, date_from, date_to, frozen_at)` - fetch records from the stream contract without waiting for the stream contract to be officialized
- `get_unsafe_index(data_provider, stream_id, date_from, date_to, frozen_at)` - fetch index from the stream contract without waiting for the stream contract to be officialized
- `get_record(data_provider, stream_id, date_from, date_to, frozen_at)` fetch records from the stream contract, stream needs to be officialized
- `get_index(data_provider, stream_id, date_from, date_to, frozen_at)` - fetch index from the stream contract, stream needs to be officialized
- `get_index_change` - fetch index change from the stream contract, stream needs to be officialized
- `stream_exists(data_provider, stream_id)` - check if the stream exists in the system contract
- `accept_stream(data_provider, stream_id)` - accept the stream as official, owner only
- `revoke_stream(data_provider, stream_id)` - revoke official status of the stream, owner only

#### Fetching through Contact Directly

Right now, users can fetch records from the contract directly.
```shell
kwil-cli database call -a=get_record -n=st1b148397e2ea36889efad820e2315d -o=4710a8d8f0d845da110086812a32de6d90d7ff5c date_from:2024-06-01 date_to:2024-06-17 --private-key 0000000000000000000000000000000000000000000000000000000000000001 --provider https://staging.tsn.test.truflation.com
```
Users are able to fetch records from the stream contract without through the system contract to keep it simple at this 
phase, and in the hands of the Truflation as a data provider. Normally, before fetching records from the stream
contract, the stream needs to be officialized by the system contract. It can be done by calling the `accept_stream` action in the system contract as the owner of the stream.

## License

The tsn-db repository is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.
