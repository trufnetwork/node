# Truf Network

The database for Truf Network (TN). It is built on top of the Kwil framework.

## Overview

Learn more about Truflation at [Truflation.com](https://trufnetwork.com). Check our internal components status [here](https://trufnetwork.grafana.net/public-dashboards/6fe3021962bb4fe1a4aebf5baddecab6).

### SDKs

To interact with TN, we provide official SDKs in multiple languages:

- **Go SDK** ([sdk-go](https://github.com/trufnetwork/sdk-go)): A Go library for interacting with TN, providing tools for publishing, composing, and consuming economic data streams. Supports primitive streams, composed streams, and system streams.

- **TypeScript/JavaScript SDK** ([sdk-js](https://github.com/trufnetwork/sdk-go-js)): A TypeScript/JavaScript library that offers the same capabilities as the Go SDK, with specific implementations for both Node.js and browser environments.

Both SDKs provide high-level abstractions for:

- Stream deployment and initialization
- Data insertion and retrieval
- Stream composition and management
- Configurable integration with any deployed TN Node

## Terminology

See [TERMINOLOGY.md](./TERMINOLOGY.md) for a list of terms used in the TN project.

## Build instructions

### Prerequisites

To build and run the TN-DB, you will need the following installed on your system:

1. [Go](https://golang.org/doc/install)
2. [Taskfile](https://taskfile.dev/installation)
3. [Docker Compose](https://docs.docker.com/compose)
4. [Python](https://www.python.org/downloads) (optional for running the seed script)
5. [Pandas](https://pandas.pydata.org) (optional for running the seed script)


### Taskfile Quickstart 🚦

For full details open **[Taskfile.yml](./Taskfile.yml)**

| What you probably want to do                                   |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **Spin up a single-node playground**                           | `task single:start` → `task single:stop`                                                                     |
| **Spin up the full 2-node devnet (nodes + gateway + indexer)** | `task devnet:start` → `task devnet:stop`                                                                     |
| **Bring up just one devnet piece**                             | `task devnet:gateway:start` / `devnet:indexer:start` / `devnet:observer:start` (each has a matching `:stop`) |
| **Build project binaries**                                     | `task build` (release) • `task build:debug` (dlv-friendly)                                                   |
| **Download upstream Kwil helper binaries**                     | `task build:binaries` (auto-runs when any compose task needs them)                                           |
| **Run unit tests or coverage**                                 | `task test:unit` • `task coverage`                                                                           |
| **Refresh genesis.json from the operator repo**                | `task get-genesis` (needs `READ_TOKEN` in `.env`)                                                            |
| **Migrate SQL schemas against any node**                       | `task action:migrate PRIVATE_KEY=<hex> PROVIDER=<url>`                                                       |

### Patterns to remember

- All compose-based stacks follow **`<stack>:start` / `<stack>:stop`**.
  `task -l` shows every available stack in two lines.
- Tasks that require extra input (`PRIVATE_KEY`, `PROVIDER`, etc.) fail fast if you forget a variable; just append `VAR=value` after the task name.

### Build Locally

#### Run With Docker Compose (Recommended)

To run the TN-DB using Docker Compose, run the following command:

```
task single:start # For a single-node playground
```

It will build and start the TN-DB in Docker containers, which is already seeded.

Alternatively, you can run the following commands to run TN-DB in Docker containers with similar setup as our the deployed server.
It has 2 nodes, gateway, and indexer enabled.

```shell
task devnet:start # For the full 2-node devnet with gateway and indexer
```

Accessing the nodes from gateway will be default to `http://localhost:443` and accessing the indexer will be default to `http://localhost:1337/v0/swagger`.

#### Build and Run the TN-DB without Docker Compose

Alternatively, you can build and run the TN-DB without Docker Compose.
This is useful if you want to run the TN-DB locally without Docker. i.e. for development or debugging purposes.
To build and run the TN-DB without Docker Compose, follow the steps below:

##### Build the binary

Invoke `task` command to see all available tasks. The `build` task will compile the binary for you. They will be generated in `.build/`:

```shell
task # list all available tasks
task build # build the binary
task kwil-binaries # download and extract the kwil binaries
```

##### Note for macOS/darwin Users:

If you're using macOS (darwin architecture), you need to perform an additional steps to download the `kwil-cli`.

1. Download the kwil-cli compatible with macOS from the kwil-db GitHub releases page

```shell
wget -O kwil-db.tar.gz https://github.com/kwilteam/kwil-db/releases/download/v0.8.4/kwil-db_0.8.4_darwin_amd64.tar.gz
```

2. Extract the kwil-cli:

```shell
tar -xzvf kwil-db.tar.gz 'kwil-cli'
```

3. Move to the `/.build` directory and make the binary executable:

```shell
mkdir -p ./.build
mv ./kwil-cli .build
chmod +x ./.build/kwil-cli
```

4. Export the kwil-cli before using it:

```shell
export PATH="$PATH:$HOME/tn/.build"
```

##### Run Postgres

Before running the, you will have to start Postgres. You can start Postgres using the following command:

```
task host:postgres:start
```

##### Run Kwild

You can start a single node network using the `kwild` binary built in the step above:

```shell
task host:kwild:start --autogen
```

##### Resetting local deployments

To stop and clean up Docker-based deployments, use the corresponding `:stop` tasks:

```shell
task single:stop
task devnet:stop
```

To clean the local data for a non-Docker `kwild` instance, run:

```shell
task host:kwild:clean
```

##### Configure the kwil-cli

To interact with the the TN-DB, you will need to configure the kwil-cli.

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

Although we use it on our servers, it's not required to be able to develop on the TN-DB. However, if you want to run the KGW locally or test it, you can follow the instructions in the [Kwil Gateway Directory](./deployments/dev-gateway/README.md)

#### Indexer

The indexer is started automatically when you run `task devnet:start`.
If you need to start it separately (e.g., for non-Docker setups or specific testing), use:

```shell
task host:indexer:start # For non-Docker setups
# or
task devnet:indexer:start # If running alongside other devnet components via compose
```

You can view the indexer API documentation at http://localhost:1337/v0/swagger.

You can view our deployed indexer at https://staging.tn.trufnetwork.com/v0/swagger.
There you can see the list of available endpoints and their descriptions.
For example, you can see the list of transactions by calling the [/chain/transactions](https://staging.tn.trufnetwork.com/v0/chain/transactions) endpoint.

### Genesis File

The genesis file for the TN-DB is located in the `deployments/networks` directory. It contains the initial configuration for the genesis block of the TN network.

#### Fetching Genesis File

In order to fetch the latest genesis file, make sure you have read access to the repository. Then, create `.env` file in the root directory similar to the `.env.example` file and put your GitHub token in it.
After that, you can fetch the latest genesis file using the following command:

```shell
task get-genesis
```

## System Contract

System Contract is a contract that stores the accepted streams by TN Gov. It also serves as an entry point for queries.
It also serves as an entry point for queries.
Currently for development purposes, private key 001 will be used to interact with the system contract.
It still needs to be updated to use the correct private key.

### Fetching through System Contract

As our system contract is currently live on our staging server, you can fetch records from the system contract using the following command:

```shell
kwil-cli database call -a=get_unsafe_record -n=tn_system_contract -o=34f9e432b4c70e840bc2021fd161d15ab5e19165 data_provider:4710a8d8f0d845da110086812a32de6d90d7ff5c stream_id:st1b148397e2ea36889efad820e2315d date_from:2024-06-01 date_to:2024-06-17 --private-key 0000000000000000000000000000000000000000000000000000000000000001 --provider https://staging.tn.trufnetwork.com
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
- `get_index_change` - fetch index change from the stream contract, which must be officialized
- `stream_exists(data_provider, stream_id)` - check if the stream exists in the system contract
- `accept_stream(data_provider, stream_id)` - accept the stream as official, owner only
- `revoke_stream(data_provider, stream_id)` - revoke official status of the stream, owner only

### Fetching through Contact Directly

Currently, users can fetch records from the contract directly.

```shell
kwil-cli database call -a=get_record -n=st1b148397e2ea36889efad820e2315d -o=4710a8d8f0d845da110086812a32de6d90d7ff5c date_from:2024-06-01 date_to:2024-06-17 --private-key 0000000000000000000000000000000000000000000000000000000000000001 --provider https://staging.tn.trufnetwork.com
```

Users are able to fetch records from the stream contract without through the system contract to keep it simple at this
phase, and in the hands of the Truflation as a data provider. Normally, before fetching records from the stream
contract, the stream must be officialized by the system contract. It can be done by calling the `accept_stream` action in the system contract as the owner of the stream.

## Metrics and Monitoring

The TN-DB includes metrics collection for improved monitoring and performance analysis. When running the development setup using `task compose-dev`, the following monitoring tools are available:

- Prometheus: Accessible at `http://localhost:9090`
- Grafana: Accessible at `http://localhost:3000` (default credentials: admin/admin)

These tools provide insights into the performance and behavior of the TN-DB system. Prometheus collects and stores metrics, while Grafana offers customizable dashboards for visualization.

For more details on the metrics configuration, refer to the files in the `deployments/dev-gateway` directory.

## Deployment

TN DB uses GitHub Actions for automated deployments. Both are triggered manually via workflow dispatch.

### Auto Deployment

The `deploy-auto.yaml` workflow allows for on-demand deployment of test environments:

- Inputs:
  - `NUMBER_OF_NODES`: Number of nodes to deploy (max 5, default 1)
  - `SUBDOMAIN`: Subdomain for the environment (default 'dev')
- Deploys to AWS using CDK
- Uses `RESTART_HASH` to control full redeployment of TN instances

### Staging Deployment

The `deploy-staging.yaml` workflow handles staging environment deployments:

- Requires specific secrets (AWS credentials, session secrets, private keys)
- Deploys to AWS using CDK
- Uses existing genesis file and private keys for TN nodes
- Instances are not redeployed on every execution

For detailed configuration and usage, refer to the workflow files in the `.github/workflows/` directory.

## License

The tn-db repository is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.

