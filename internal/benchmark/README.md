# TSN-DB Benchmark

This directory contains benchmark tests for the Truflation Stream Network Database (TSN-DB), focusing on evaluating the performance impact of stream composition depth.

## Objective

The primary goal of this benchmark is to identify the limits regarding the depth at which composed streams can be created and queried efficiently.

## Key Concepts

- **Stream Composition Depth**: Refers to the number of dependencies between streams. For example, if Stream D depends on Stream C, which depends on Streams A and B, then D has a composition depth of 2.
- **Query Complexity**: As depth increases, queries become more complex due to recursive operations and multiple permission checks.

## Benchmark Parameters

The tests vary across several dimensions:

1. **Stream Depth**: 0, 1, 10, 50, 100
2. **Time Range**: 1, 7, 30, 365 days
3. **Visibility**: Public and Private
4. **Procedures**: get_record, get_index, get_index_change

## Running the Benchmark

To run the benchmark:

From the root of the project, run the following command:

```
go test -v ./internal/benchmark
```

## Results

After running, the benchmark will output performance metrics for each test case, including:

- Mean duration
- Minimum duration
- Maximum duration

These results help evaluate the efficiency of TSN-DB operations under different conditions.

## Note

This benchmark is intended for development and testing purposes. The results may vary depending on your system configuration and current load.