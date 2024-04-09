# Terminology

This document is a reference for the terminology used in the TSN-DB project. It is intended to be a living document that evolves as the project evolves. It is meant to be a reference for developers and users of the TSN-DB project.

## Definitions

- STREAM: Flow of data that can either be composed or primitive. It's data source may be calculated or just stored.
- COMPOSED STREAM: Type of stream which is composed of other streams. It's output is calculated based on inputs.
- PRIMITIVE STREAM: Type of stream which is stored as is. It's output is based on what is purely stored.
- TAXONOMY: Description of stream compositions
- SCHEMA: Short for kuneiform schema. It may be read as a database. Depending on the architecture, it may store more than a stream.
- TABLE: A kuneiform schema may have many tables inside it. E.g. values table, metadata table, etc.
- ACTION: A kuneiform schema may have many actions inside it, which can be readonly, or mutations, which write data.
- EXTENSION: Short for Kwil extension
- PROCEDURE: Short for Kwil procedure (upcoming)
- DATA PROVIDER: An entity which creates/pushes primitive data OR create taxonomy definitions
- ENVIRONMENT: A TSN-DB deployment which serves a purpose. E.g. staging, development, production, local
- INDEX: A calculation over _VALUE_. It's the `currentValue / baseValue`
- VALUE: The raw value which is used to calculate indexes. E.g. CPI can be queried for its value or its index of a date.
- STREAM ID: Identifier of a stream. May be a code such as `CPI` or a hash.
- UPGRADEABLE SCHEMA: A Kuneiform Schema that does need redeployment for important structural changes
- [Stream A is] CHILD OF [Stream B]
- [Stream B is] PARENT OF [Stream A]
- [Stream B is] COMPOSED OF [Streams A and C]
- TRUFLATION DATABASE: The mariaDB instance storing truflation data. It may be an instance of some environment (test, staging, prod)
- TRUFLATION DATABASE TABLE: We should NOT use _TABLE_ to refer it without correctly specifying, otherwise it creates confusion with kuneiform tables.
- WHITELIST: A wallet may be whitelisted, meaning it is allowed to perform certain actions. It maybe write or read specific.
- PRIVATE KEY: A secret key that refers to a wallet. It may own schemas, or refer to an entity/user that needs to interact with the TSN-DB.


## Avoid
If something is being frequently used that could create confusion, let's be explicit on this section

- Categories: However it may be used by marketing as it resembles more something usual to end users
- Sub-stream
- Don't use `index` for streams indistinctly. Although CPI is, for marketing, an index, we should refer to it as a STREAM. Unless we want to say the `index` from CPI which is a calculation over `value`.
