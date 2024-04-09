# Terminology

This document is a reference for the terminology used in the TSN-DB project. It is intended to be a living document that evolves as the project evolves. It is meant to be a reference for developers and users of the TSN-DB project.

## Definitions

- STREAM: A flow of data that can either be composed or primitive. Its data source may be calculated or just stored.
- COMPOSED STREAM: A type of stream that is composed of other streams. Its output is calculated based on inputs.
- PRIMITIVE STREAM: A type of stream that is stored as is. Its output is based on what is purely stored.
- TAXONOMY: Description of stream compositions
- SCHEMA: Short for kuneiform schema. It may be read as a database. Depending on the architecture, it may store more than a stream.
- TABLE: A kuneiform schema may have many tables inside it. E.g. values table, metadata table, etc.
- ACTION: A kuneiform schema may contain many actions, which can be read-only or mutations, which write data.
- EXTENSION: Short for Kwil extension
- PROCEDURE: Short for Kwil procedure (upcoming)
- DATA PROVIDER: An entity that creates/pushes primitive data OR creates taxonomy definitions
- ENVIRONMENT: A TSN-DB deployment that serves a purpose. E.g., staging, development, production, local
- INDEX: A calculation over _VALUE_. It's the `currentValue / baseValue`
- VALUE: The raw value used to calculate indexes. For example, CPI can be queried for its value or its index of a date.
- STREAM ID: Identifier of a stream. It may be a code such as `CPI` or a hash.
- UPGRADEABLE SCHEMA: A Kuneiform Schema that does need redeployment for important structural changes
- [Stream A is] CHILD OF [Stream B]
- [Stream B is] PARENT OF [Stream A]
- [Stream B is] COMPOSED OF [Streams A and C]
- TRUFLATION DATABASE: The MariaDB instance that stores truflation data. It may be an instance of some environment (test, staging, prod)
- TRUFLATION DATABASE TABLE: We should NOT use _TABLE_ to refer it without correctly specifying; Otherwise, it creates confusion with kuneiform tables.


## Avoid
If something is being frequently used that could create confusion, let's be explicit in this section

- Categories: However it may be used by marketing as it resembles more something usual to end users
- Sub-stream
- Don't use `index` for streams indistinctly. Although CPI is, for marketing, an index, we should refer to it as a STREAM. Unless we want to say the `index` from CPI, which is a calculation over `value`.
