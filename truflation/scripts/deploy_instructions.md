## Produce Source Maps

This step will process ./produce_source_maps/categories-tables-us.csv and generate a file called `all_tables.csv` which contains data to:

- fetch data from database
- create composed schemas

If you wish to test with less data, use `--minimum` flag. This will get data from a mini version of `categories-tables-us.csv`. This speeds up deployment and testing.

```shell
python ./produce_source_maps/process_all.py --minimum
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

`--skip-drop` flag is useful for testing purposes, as it will not drop the database before deploying the schemas. May get things done quicker.

```shell
./database_deploy.sh --skip-drop
```

This will deploy the primitives and composed streams to the kwil database.

Beware: This step can take a lot of time, as there's a lot of transactions to occur. Be patient.

If you are using the full data, this can take 10 secs * +200 tables ~= 30 minutes. Use `--minimum` flag on `Produce Source Maps` step to speed up deployment and testing.

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