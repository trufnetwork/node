# TSN-DB Benchmark

This directory contains benchmark tests for the Truflation Stream Network Database (TSN-DB), focusing on evaluating the performance impact of stream composition depth.

This benchmark is part of a larger system used to evaluate TSN-DB performance across different environments and AWS EC2 instance types. The results from these tests are used to generate markdown reports for each instance, providing valuable insights into the system's performance under various conditions.

For information on how to trigger these benchmarks and view the resulting reports, please refer to the [Getting Benchmarks](../../infra/docs/getting-benchmarks.md) documentation.

## Objective

The primary goal of this benchmark is to identify the limits regarding the depth at which composed streams can be created and queried efficiently.

## Key Concepts

- **Stream Composition Depth**: Refers to the number of dependencies between streams. For example, if Stream D depends on Stream C, which depends on Streams A and B, then D has a composition depth of 2.
- **Query Complexity**: As depth increases, queries become more complex due to recursive operations and multiple permission checks.

## Benchmark Parameters

The tests vary across several dimensions, [defined here](./constants.go):

## Running the Benchmark

To run the benchmark:

From the root of the project, run the following command:

```
go test -v ./internal/benchmark
```

To run a specific test case, use the `-run` flag:
```
go test -run TestBenchUnix ./internal/benchmark -v
```
The above command will run the `TestBenchUnix` test case.

## Results

After running, the benchmark will output performance metrics for each test case, including:

- Mean duration
- Minimum duration
- Maximum duration

These results help evaluate the efficiency of TSN-DB operations under different conditions.

## Note

This benchmark is intended for development and testing purposes. The results may vary depending on your system configuration and current load.