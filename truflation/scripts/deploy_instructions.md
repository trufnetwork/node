## Produce Source Maps

This step will process ./produce_source_maps/categories-tables-us.csv and generate a file called `all_tables.csv` which contains data to:

- fetch data from database
- create composed schemas

```shell
python ./produce_source_maps/process_all.py
```

Feel free to inspect the output, as it contains pretty much all the information you need to deploy and query schemas.

This does NOT need to be committed.

## Fetch data from DB (Optional)

You'll do this only if there's data in the DB that you want to fetch, and you have access to the DB. Otherwise, don't worry about this step as the data is already in the repo.

It's necessary to have mysqlclient and pandas available to run it

Make sure you've copied secret_db_credentials.example.json to secret_db_credentials.json and filled in the necessary fields.

```shell
python ./pull_db_data.py
```

Will output data to `./raw_from_db/` directory. This must be committed.

### Generate CSV files with clean data from primitives

This will process the data generate csv files with clean data. Output is at `./temp_csv/` dir. The `all_tables.csv` file is used as input to know which tables and files to process.

```shell
./generate_clean_csv_from_raw.sh
```

## Create Composed Schemas

```shell
./generate_composed_schemas.sh
```

Will output data to `./temp_composed_schemas/` directory. This does not need to be committed.

## Deploy Primitives & Composed Streams

MAKE SURE KWIL-DB IS RUNNING

`--skip-drop` will not drop the database before deploying the schemas.

```shell
./database_deploy.sh
```

This will deploy the primitives and composed streams to the kwil database.

## Add data to the database

```shell
./database_add_primitives.sh
```

This will add the data to the database, from the files in `./temp_csv/`. This is also slow, as there's a lot of transactions to occur.

150 tables can take 10 secs * 150 tables = 25 minutes.

## Test querying the latest data for a primitive schema

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=com_numbeo_us_bread_3m_avg
```

## Test querying the latest data for a composed schema downstream

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=food_at_home
```

## Get CPI result

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=cpi
```

Querying with an address that is not allowed

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=cpi --private-key="0000000000000000000000000000000000000000000000000000000000000123" --owner="$(../../.build/kwil-cli account id)"
```