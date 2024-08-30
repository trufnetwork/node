# Generating and Viewing Benchmark Reports for TSN-DB

This document explains how to trigger benchmark tests and view reports for the Truflation Stream Network Database (TSN-DB) using AWS services.

## Deployment

Before running benchmarks, ensure that the TSN-DB infrastructure is properly deployed. For deployment instructions, please refer to the [main README.md](../README.md) in the `infra` directory.

## Triggering Benchmark Generation

To generate benchmark reports:

1. Log in to the AWS Management Console.
2. Navigate to AWS Step Functions.
3. Locate and select the TSN-DB benchmark Step Function (the name should include "TSN-Benchmark").
4. Click "Start execution" to trigger the benchmark process.
5. Monitor the execution progress. When it completes, check for any errors in the execution details.

**Warning**: If the Step Function execution fails, check for any orphaned EC2 instances created by this stack and terminate them manually if necessary.

## Benchmark Process

The benchmark process follows these steps:

1. Spin up multiple EC2 instances of various types.
2. Run benchmark tests on each instance.
3. Save CSV reports with test results.
4. Upload CSV files to S3, using a timestamp as the key.
5. After all instances complete, a Lambda function:
   - Converts CSV files to markdown reports.
   - Uploads the markdown reports to S3.

## S3 Bucket Structure

The reports are stored in an S3 bucket with the following structure:

```
s3://tsn-benchmark-results/
|── YYYY-MM-DD-HH-MM-SS_<instanceType>.csv <- individual instance reports
|── ...
└── reports/
    |── YYYY-MM-DD-HH-MM-SS.md <- combined report
    |── ...
```

Where `YYYY-MM-DD-HH-MM-SS` is the timestamp of the state machine execution.

## Viewing Reports

To view the generated reports:

1. Log in to the AWS Management Console.
2. Navigate to the S3 service.
3. Find and open the TSN benchmark results bucket.
4. Go to the `reports` directory.
5. Look for the most recent timestamp folder.
6. Download and view the markdown reports in that folder.

The `reports/YYYY-MM-DD-HH-MM-SS.md` file will contain an overview of all benchmark results, while individual instance reports may provide more detailed information.

## Troubleshooting

If you encounter issues:

1. Check the Step Function execution logs for any error messages.
2. Verify that all required AWS resources (EC2, S3, Lambda) have the necessary permissions.
3. Ensure that the benchmark code in the EC2 instances is up-to-date.

For further assistance, please contact the TSN-DB development team.