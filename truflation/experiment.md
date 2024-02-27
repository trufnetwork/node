## Build Kwil Binaries
Run it when you make changes to the source code.
```shell
cd .. && task build;
```

## Run Kwil Node
```shell
../.build/kwild --autogen
```

or debugging with dlv
```shell
dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ../.build/kwild -- --autogen
```

## Clear Kwil Data
```shell
rm -r ~/.kwild
```

## Create CSV Files
Some adjustments are needed to data that comes directly from the database. Integer is not accepted, for example.
```shell
python ./temp_sources/transform_source.py
```

## Seed Kwil Data
```shell
../.build/kwil-cli database deploy -p=./tsn/base_schema/base_schema.kf --sync --name=com_truflation_us_hotel_price
../.build/kwil-cli database batch --sync --path ./temp_sources/transformed/com_truflation_us_hotel_price.csv --action add_record --name=com_truflation_us_hotel_price
```

```shell
../.build/kwil-cli database deploy --sync -p=./tsn/base_schema/base_schema.kf --name=com_yahoo_finance_corn_futures --sync
../.build/kwil-cli database batch --sync --path ./temp_sources/transformed/com_yahoo_finance_corn_futures.csv --action add_record --name=com_yahoo_finance_corn_futures --sync
```

## List Kwil Databases
Run if you need to ensure that the database is deployed.
```shell
../.build/kwil-cli database list --self
```


## Query Kwil Data
```shell
../.build/kwil-cli database call -a=get_index date:"2000-07-19" date_to:"" -n=com_yahoo_finance_corn_futures
```

```shell
../.build/kwil-cli database call -a=get_index date:"2000-07-19" date_to:"2000-07-24" -n=com_yahoo_finance_corn_futures
```

## Deploy Experiment Table

```shell
../.build/kwil-cli database drop experiment_table --sync
../.build/kwil-cli database deploy -p=./example_schemas/experiment_table.kf --name=experiment_table --sync
```

## Query experiment_table

```shell
../.build/kwil-cli database call -a=get_index date:"2010-03-01" date_to:"" -n=experiment_table
```

```shell
../.build/kwil-cli database call -a=get_index date:"2010-03-01" date_to:"2010-03-05" -n=experiment_table
```