# TN Node

**TN Node** is the database layer powering **[TRUF.NETWORK](https://truf.network)**, built on top of the Kwil framework.


## Overview

This documentation is intended for node operators, developers and contributors. It includes guides for setup, upgrades, development, and SDK usage.

## Quick Start with AMI

Deploy a TRUF.NETWORK node in minutes using our pre-configured Amazon Machine Image:

**🚀 [AMI Deployment Guide](./docs/ami-deployment-guide.md)** - *Recommended for beginners*

## Node Operator Guide

Choose your preferred deployment method:

| Method | Time | Skill Level | Best For |
|--------|------|-------------|----------|
| **[AMI Deployment](./docs/ami-deployment-guide.md)** | 5-10 min | Beginner | Quick start, AWS users |
| **[Manual Setup](./docs/node-operator-guide.md)** | 45-60 min | Advanced | Full control, custom infrastructure |

📊 **[Compare All Deployment Options](./docs/deployment-options.md)**

## Terminology

For definitions of key terms used throughout this project, refer to the [Terminology Reference](./TERMINOLOGY.md).

## Schema

[Schema](./docs/schema.md)


## Node Upgrade Guide

Steps and best practices for upgrading TN nodes:

[Node Upgrade Guide](./docs/node-upgrade-guide.md)


## SDKs

We provide official SDKs to integrate and interact with the TN network:

- **Go SDK**  
  📦 [trufnetwork/sdk-go](https://github.com/trufnetwork/sdk-go)  
  A comprehensive Go client for deploying and managing TN data streams.

- **TypeScript SDK**  
  📦 [trufnetwork/sdk-js](https://github.com/trufnetwork/sdk-js)  
  A TypeScript library for both Node.js and browser environments.

### Supported Features:

- Stream deployment and initialization
- Data insertion, querying, and retrieval
- Composed stream creation and updates
- Configurable node connections

## MCP Server

Learn how to query data through AI Clients: [TRUF.NETWORK MCP](./docs/mcp-server.md)

For production deployments behind reverse proxies: [MCP Reverse Proxy Configuration](./docs/mcp-reverse-proxy.md)

## Development Guide

For contributors and developers building on top of TN:

[Development Guide](./docs/development.md)

## Attestations

Need to request, sign, or verify TrufNetwork attestations? Start with the quick guide:

**[Attestations Overview](./docs/attestations.md)**


## License

This project is licensed under the **Apache License 2.0**. See the [LICENSE](./LICENSE) file for details.
