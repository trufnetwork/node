## Produce Source Maps

This step will process ./produce_source_maps/categories-tables-us.csv and generate a file called `all_tables.csv` which contains data to:
- fetch data from database
- create composed schemas

```shell
python ./process_all.py
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

### Generate CSV files with clean data from primitives

This will process the data from the database and generate csv files with clean data. Output is at `./temp_csv/` dir

You may add the `--filter-by-all-tables` flag to filter the data by the tables in `all_tables.csv`. Otherwise it will generate CSV from all files from `./raw_from_db/` directory. This is useful for testing purposes: i.e., we test less data.

```shell
./generate_clean_csv_from_raw.sh --filter-by-all-tables
```

Will output data to `./raw_from_db/` directory. This must be committed.

## Create Composed Schemas

```shell
./generate_composed_schemas.sh
```

Will output data to `./temp_composed_schemas/` directory. This does not need to be committed.

## Deploy Primitives & Composed Streams

MAKE SURE KWIL-DB IS RUNNING

`--skip-drop` flag is useful for testing purposes, as it will not drop the database before deploying the schemas.

```shell
./database_deploy.sh --skip-drop
```

This will deploy the primitives and composed streams to the kwil database.

Beware: This step takes a lot of time, as there's a lot of transactions to occur. Be patient.

## Add data to the database

```shell
./database_add_primitives.sh
```

This will add the data to the database, from the files in `./temp_csv/`. This is also slow, as there's a lot of transactions to occur.

## Or both

```shell
./database_deploy.sh --skip-drop;
./database_add_primitives.sh;
```

## Test querying the latest data for a primitive schema

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=cmnme_saatet_3bed_in_city_3m_avg
```

## Test querying the latest data for a composed schema downstream

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=rented_dwellings
```

```shell
../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=cpi
```