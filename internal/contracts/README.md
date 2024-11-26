# TSN Contracts

This directory contains the Kuneiform contracts used in the Truflation Stream Network (TSN).

## Contents

- `composed_stream_template.kf`: Template for composed stream contracts
- `primitive_stream_template.kf`: Template for primitive stream contracts
- `system_contract.kf`: System-level contract for managing official streams
- `contracts.go`: Go file for embedding contract contents

## Purpose

These contracts define the core functionality of the TSN, including:

- Data stream management
- Permissions and access control
- Index calculations
- Metadata handling

## Synchronization

We aim to keep these contracts in sync with the public versions in the [sdk-go repository](https://github.com/trufnetwork/sdk-go). This private repository serves as the primary development environment.

## Additional Resources

- [Detailed Contract Documentation](https://github.com/trufnetwork/sdk-go/blob/main/docs/contracts.md)
- Benchmark tool (located in this directory)
- Kuneiform logic tests (located in this directory)

For more information on contract methods and usage, please refer to the detailed documentation linked above.