# Truf Network
 
The database for TRUF.NETWORK (TN). It is built on top of the Kwil framework.

## Overview

Learn more about TRUF.NETWORK at [TRUF.NETWORK](https://truf.network).

Want to join TN as a node operator? Follow the guidelines here: [Node Operator Guide](https://docs.truf.network/node-operator-guide)

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
6. build-essential (for gcc) - install with `sudo apt install build-essential`

Make sure Taskfile is in your PATH:
```shell
export PATH=$PATH:$HOME/go/bin
```

You may also want to add the `.build` directory to your PATH for easier access to the built binaries:
```shell
export PATH=$PATH:$(pwd)/.build
```

If you plan to use Docker, ensure your user is in the docker group:
```shell
sudo usermod -aG docker $USER
```
Note: You'll need to log out and back in for this change to take effect.

### Taskfile Quickstart 🚦

For full details open **[Taskfile.yml](./Taskfile.yml)**

| Task | Command |
|------|---------|
| **Spin up a single-node playground** | `task single:start` → `task single:stop` |
| **Spin up the full 2-node devnet** (nodes + gateway + indexer) | `task devnet:start` → `task devnet:stop` |
| **Bring up just one devnet piece** | `task devnet:gateway:start` / `devnet:indexer:start` / `devnet:observer:start` (each has a matching `:stop`) |
| **Build project binaries** | `task build` (release) • `task build:debug` (dlv-friendly) |
| **Download upstream Kwil helper binaries** | `task build:binaries` (auto-runs when any compose task needs them) |
| **Run unit tests or coverage** | `task test:unit` • `task coverage` |
| **Refresh genesis.json from the operator repo** | `task get-genesis` (needs `READ_TOKEN` in `.env`) |
| **Migrate SQL schemas against any node** | `task action:migrate PRIVATE_KEY=<hex> PROVIDER=<url>` |

### Patterns to remember

- All compose-based stacks follow **`<stack>:start` / `<stack>:stop`**.
  `task -l` shows every available stack in two lines.
- Tasks that require extra input (`PRIVATE_KEY`, `PROVIDER`, etc.) fail fast if you forget a variable; just append `VAR=value` after the task name.

### Build Locally

#### Run With Docker Compose (Recommended)

To run the TN-DB using Docker Compose, run the following command:

```shell
task build # Build the binaries first
task single:start # For a single-node playground
```

It will build and start the TN-DB in Docker containers, which is already seeded.

Alternatively, you can run the following commands to run TN-DB in Docker containers with similar setup as our the deployed server.
It has 2 nodes, gateway, and indexer enabled.

```shell
task build # Build the binaries first
task devnet:start # For the full 2-node devnet with gateway and indexer
```

Accessing the nodes from gateway will be default to `http://localhost:443` and accessing the indexer will be default to `http://localhost:1337/v0/swagger`.

#### (Optional) Download the Indexer

If you plan to use the indexer when running
```shell
task devnet:start
```
you must first grab the binary release of the kwil-indexer binary:
1. Download the appropriate archive for your platform from
https://github.com/trufnetwork/indexer/releases
2. Extract it into your .build/ folder

> Note: For local development and testing you can use the "all-zeros" private key: 0000000000000000000000000000000000000000000000000000000000000001

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

You have two options to make the kwil-cli accessible:

1. Add the .build directory to your PATH (replace `/path/to/your/project` with your actual project location):
```shell
export PATH=$PATH:/path/to/your/project/.build
```

For example, if your project is in `$HOME/trufnetwork/node`, you would use:
```shell
export PATH=$PATH:$HOME/trufnetwork/node/.build
```

2. Or move the binary to your system path (e.g., /usr/local/bin):
```shell
sudo mv .build/kwil-cli /usr/local/bin/
```

##### Run Postgres

Before running the TN-DB, you will have to start Postgres. You can start Postgres using the following command:

```shell
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

You can view our deployed indexer at https://indexer.mainnet.truf.network/v0/swagger.
There you can see the list of available endpoints and their descriptions.
For example, you can see the list of transactions by calling the [/chain/transactions](https://indexer.mainnet.truf.network/v0/chain/transactions) endpoint.

### Genesis File

The genesis file for the TN-DB is located in the `deployments/networks` directory. It contains the initial configuration for the genesis block of the TN network.

#### Fetching Genesis File

In order to fetch the latest genesis file, make sure you have read access to the repository. Then, create `.env` file in the root directory similar to the `.env.example` file and put your GitHub token in it.
After that, you can fetch the latest genesis file using the following command:

```shell
task get-genesis
```

To run migrations on the local development environment, use:
```shell
task action:migrate:dev
```

### Fetching through Contract Directly

Currently, users can fetch records from the contract directly.

```shell
kwil-cli call-action get_record text:0x4710a8d8f0d845da110086812a32de6d90d7ff5c text:st1b148397e2ea36889efad820e2315d int:null int:null int:null
```

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

For detailed configuration and usage, refer to the workflow files in the `.github/workflows/` directory.

## License

The tn-db repository is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more details.