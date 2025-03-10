# Pure SQL Tests

This folder contains tests that are written in pure SQL. The only intention here is to explore capabilities of raw postgres queries, to understand how we could leverage them in Kwil if needed.

## How to run

To run the tests, you need to first start a postgres container:

```bash
docker run --rm --name postgres-container -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=postgres -p 5432:5432 -d postgres
```

Then, you can run the pgsql files one by one and explore the data.