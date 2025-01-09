# Terminology

This document is a reference for the terminology used in the TN project. It is intended to be a living document that evolves as the project evolves. It is meant to be a reference for developers and users of the TN project.

## Definitions

- STREAM: a sequence of data elements made available over time.
- PRIMITIVE STREAM: A type of stream where the data elements are primitive and not composed. Meaning, a direct source of the data elements is a Data Provider.
- COMPOSED STREAM: A type of stream that is composed of other streams. Its output is calculated based on inputs from primitive or other composed streams.
- TAXONOMY: A scheme of hierarchical stream classification, in which streams are organized into categories and types.
- DATA CONTRACTS: Or simply contracts. Is a set of rules that define the structure and behavior of data. A contract is defined in a Kuneiform file.
- TABLE: One of the contract's building blocks, that defines the underlying data structure of the Stream.
- ACTION: One of the contract's building blocks; defines Contract methods that can be called by the end-user, i.e.: data read-only or write methods.
- EXTENSION: Short for Kwil extension.
- PROCEDURE: Short for Kwil procedure.
- DATA PROVIDER: An entity that creates/pushes primitive data OR creates taxonomy definitions.
- ENVIRONMENT: A TN deployment that serves a purpose. E.g., local, staging, production.
- PRIMITIVE: A data element that is supplied directly from Data Provider.
- STREAM RECORD: Or just RECORD. It's the value used to calculate indexes. If it's a primitive stream, it's the primitive value.
- INDEX: A calculation over _RECORD_. E.g., `currentDateRecord / baseDateRecord`.
- STREAM ID: A generated hash used as a unique identifier of a stream.
- UPGRADEABLE CONTRACT: A contract that doesn't need redeployment for important structural changes.
- CHILD OF: A relation between streams, where a child stream is a subset of a parent stream
- PARENT OF: A relation between streams, where a parent stream is a superset of a child stream. All streams that have children are Composed Streams.
- TRUFLATION DATABASE: The MariaDB instance that stores trufnetwork data. It may be an instance of some environment (test, staging, prod).
- TRUFLATION DATABASE TABLE: We should NOT use _TABLE_ to refer it without correctly specifying; Otherwise, it creates confusion with kuneiform tables.
- WHITELIST: A list of wallets that defines permission to perform a certain action. It may be "write" or "read" specific.
- PRIVATE KEY: A secret key that refers to a wallet. It may own contracts, or refer to an entity/user that needs to interact with the TN-DB.
- SYSTEM CONTRACT: The unique contract within TN that manages official streams and serves as the primary access point for stream queries.
- OFFICIAL STREAM: A stream that has been approved by TN governance and registered in the System Contract.
- UNOFFICIAL STREAM: A stream that exists within TN but has not been approved by governance or registered in the System Contract.
- TN GOVERNANCE: The entity or group responsible for approving streams and managing the System Contract.
- SAFE READ: A query made through the System Contract that only returns data from official streams.
- UNSAFE READ: A query made through the System Contract that can return data from any stream, official or unofficial.
- STREAM DEPLOYER: A wallet address that deployed a specific stream contract.
- STREAM OWNER: A wallet address that owns a stream contract. The Stream Owner is not necessarily the Stream Deployer but can create a Composed Stream using contracts deployed by other wallets.
- STREAM READER: A wallet address with permission to read data from a stream. For public streams, any address can be a reader. For private streams, the address must be whitelisted.
- STREAM WRITER: A wallet address with permission to write data to a stream. Must be whitelisted by a stream owner to have write access.
- STREAM VISIBILITY: Access control setting that determines data read permissions:
  - PUBLIC: Data can be read by any wallet address
  - PRIVATE: Data can only be read by whitelisted wallet addresses
- COMPOSITION VISIBILITY: Access control setting that determines which streams can use this stream as an input:
  - PUBLIC: Any stream can compose using this stream
  - PRIVATE: Only whitelisted streams can compose using this stream
- WALLET PROFILE: Public information associated with a wallet address in the Explorer, including:
  - Identity information
  - Associated streams (deployed, owned, writable, readable)
  - Public activity and contributions


## Avoid
If something is being frequently used that could create confusion, let's be explicit in this section.

- Categories: However it may be used by marketing as it resembles more something usual to end users.
- Sub-stream.
- Don't use `index` for streams indistinctly. Although CPI is, for marketing, an index, we should refer to it as a STREAM. Unless we want to say the `index` from CPI, which is a calculation over `record`.
- SCHEMA: Avoid using this term to refer to kuneiform files, or other similar concepts due to ambiguity of this term.
