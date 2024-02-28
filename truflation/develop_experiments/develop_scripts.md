# Develop Scripts

This file aims at providing a quick reference for the most common tasks during the development.

## Build Kwil Binaries

Run it when you make changes to the source code.

```shell
cd ../../ && task build:kwild;
```

If you need to have a better time debugging after building, run this to tell compiler to preserve variables while debugging.

```shell
cd ../../ && GO_GCFLAGS="all=-N -l" task build:kwild;
```

## Run Kwil Node

```shell
../.build/kwild --autogen
```

or debugging with dlv

```shell
dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ../../.build/kwild -- --autogen
```

## Clear Kwil Data

```shell
rm -r ~/.kwild
```

## Create CSV Files

Some adjustments are needed to data that comes directly from the database. Integer is not accepted, for example.

Why don't we just include the processed files in the repository?
R: The csv files are generated from the database, and they don't fit our data before the transformation. So this shows necessary steps to transform the data.

```shell
python ./test_samples/transform_source.py
```

## Seed Kwil Data

```shell
../../.build/kwil-cli database drop com_truflation_us_hotel_price --sync
../../.build/kwil-cli database deploy -p=./tsn/base_schema/base_schema.kf --sync --name=com_truflation_us_hotel_price
../../.build/kwil-cli database batch --sync --path ./test_samples/transformed/com_truflation_us_hotel_price.csv --action add_record --name=com_truflation_us_hotel_price
```

```shell
../../.build/kwil-cli database drop com_yahoo_finance_corn_futures --sync
../../.build/kwil-cli database deploy --sync -p=./tsn/base_schema/base_schema.kf --name=com_yahoo_finance_corn_futures --sync
../../.build/kwil-cli database batch --sync --path ./test_samples/transformed/com_yahoo_finance_corn_futures.csv --action add_record --name=com_yahoo_finance_corn_futures --sync
```

## List Kwil Databases

Run if you need to ensure that the database is deployed.

```shell
../../.build/kwil-cli database list --self
```

## Query Kwil Data

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"" -n=com_yahoo_finance_corn_futures
```

Expected:

| value  |
|--------|
| 150000 |

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"2000-07-22" -n=com_yahoo_finance_corn_futures
```

| value  |
|--------|
| 150000 |
| 200000 |
| 250000 |
| 300000 |
| 250000 |

## Deploy Composed Table

```shell
../../.build/kwil-cli database drop composed --sync
../../.build/kwil-cli database deploy -p=./composed.kf --name=composed --sync
```

## Query Composed Table

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-19" date_to:"" -n=composed
```

| value |
|-------|
| 20000 |

This value should be 10% of corn futures value on 2000-07-19. We purposely set hotels value to 0 to easily verify the
weights are correct.

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"2000-07-22" -n=composed
```

| value  |
|--------|
| 150000 |
| 20000  |
| 250000 |
| 300000 |
| 250000 |

See again the 20000 value. It is 10% of the corn futures value on 2000-07-19.