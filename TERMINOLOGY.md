# Terminology

This document is a reference for the terminology used in the TSN-DB project. It is intended to be a living document that evolves as the project evolves. It is meant to be a reference for developers and users of the TSN-DB project.

## Definitions

- STREAM: a sequence of data elements made available over time.
- PRIMITIVE STREAM: A type of stream where the data elements are primitive and not composed. Meaning, a direct source of the data elements is a Data Provider.
- COMPOSED STREAM: A type of stream that is composed of other streams. Its output is calculated based on inputs from primitive or other composed.
- TAXONOMY: A scheme of hierarchical stream classification, in which streams are organized into categories and types.
- SCHEMA: Short for kuneiform schema. It may be read as a database. Depending on the architecture, it may store more than a stream.
- TABLE: One of the contract's building blocks, that defines the underlying data structure of the Stream.
- ACTION: One of the contract's building blocks; defines Contract methods that can be called by the end-user, i.e.: data read-only or write methods.
- EXTENSION: Short for Kwil extension
- PROCEDURE: Short for Kwil procedure (upcoming)
- DATA PROVIDER: An entity that creates/pushes primitive data OR creates taxonomy definitions
- ENVIRONMENT: A TSN deployment that serves a purpose. E.g., local, staging, production.
- INDEX: A calculation over _VALUE_. E.g., `currentValue / baseValue`
- PRIMITIVE: A data element that is supplied directly from Data Provider. It is usually used to calculate indexes. For example, CPI can be queried for its value or its index of a date.
- STREAM ID: A generated hash used as an identifier of a stream
- UPGRADEABLE SCHEMA: A Kuneiform Schema that does need redeployment for important structural changes
- [Stream A is] CHILD OF [Stream B]
- [Stream B is] PARENT OF [Stream A]
- [Stream B is] COMPOSED OF [Streams A and C]
- TRUFLATION DATABASE: The MariaDB instance that stores truflation data. It may be an instance of some environment (test, staging, prod)
- TRUFLATION DATABASE TABLE: We should NOT use _TABLE_ to refer it without correctly specifying; Otherwise, it creates confusion with kuneiform tables.
- WHITELIST: A wallet may be whitelisted, meaning it is allowed to perform certain actions. It maybe write or read specific.
- PRIVATE KEY: A secret key that refers to a wallet. It may own schemas, or refer to an entity/user that needs to interact with the TSN-DB.


## Avoid
If something is being frequently used that could create confusion, let's be explicit in this section

- Categories: However it may be used by marketing as it resembles more something usual to end users
- Sub-stream
- Don't use `index` for streams indistinctly. Although CPI is, for marketing, an index, we should refer to it as a STREAM. Unless we want to say the `index` from CPI, which is a calculation over `value`.
