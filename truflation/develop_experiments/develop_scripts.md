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
../../.build/kwild --autogen
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
../../.build/kwil-cli database deploy -p=../base_schema/base_schema.kf --sync --name=com_truflation_us_hotel_price
../../.build/kwil-cli database batch --sync --path ./test_samples/transformed/com_truflation_us_hotel_price.csv --action add_record --name=com_truflation_us_hotel_price
```

```shell
../../.build/kwil-cli database drop com_yahoo_finance_corn_futures --sync
../../.build/kwil-cli database deploy --sync -p=../base_schema/base_schema.kf --name=com_yahoo_finance_corn_futures --sync
../../.build/kwil-cli database batch --sync --path ./test_samples/transformed/com_yahoo_finance_corn_futures.csv --action add_record --name=com_yahoo_finance_corn_futures --sync
```

## List Kwil Databases

Run if you need to ensure that the database is deployed.

```shell
../../.build/kwil-cli database list --self
```

## Query Kwil Data

```shell
# query latest
../../.build/kwil-cli database call -a=get_index date:"" date_to:"" -n=com_yahoo_finance_corn_futures
```

Expected:

| date       | value  |
|------------|--------|
| 2000-07-30 | 500000 |

Query after latest:

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-08-02" date_to:"" -n=com_yahoo_finance_corn_futures
```

Expected answer with the latest date.

| date       | value  |
|------------|--------|
| 2000-07-30 | 500000 |

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"" -n=com_yahoo_finance_corn_futures
```

Expected:

| date       | value  |
|------------|--------|
| 2000-07-18 | 150000 |

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"2000-07-22" -n=com_yahoo_finance_corn_futures
```

| date       | value  |
|------------|--------|
| 2000-07-18 | 150000 |
| 2000-07-19 | 200000 |
| 2000-07-20 | 250000 |
| 2000-07-21 | 300000 |
| 2000-07-22 | 250000 |

### Expect all of these to error:

```shell
# wrong date format
../../.build/kwil-cli database call -a=get_index date:"2000/07/18" date_to:"" -n=com_yahoo_finance_corn_futures
```

```shell
# wrong date_to format
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"2000/07/22" -n=com_yahoo_finance_corn_futures
```

```shell
# before any available data
../../.build/kwil-cli database call -a=get_index date:"1999-07-17" date_to:"1999-07-22" -n=com_yahoo_finance_corn_futures
```

```shell
# before any available data
../../.build/kwil-cli database call -a=get_index date:"1999-07-17" date_to:"" -n=com_yahoo_finance_corn_futures
```

## Composed Table

### Deploy

```shell
../../.build/kwil-cli database drop composed --sync
../../.build/kwil-cli database deploy -p=./composed.kf --name=composed --sync
```

### Query

| date       | corn | hotel | expected |
|------------|------|-------|----------|
| 2000-07-19 | 20   | 1     | 2,9      |

```shell
../../.build/kwil-cli database call -a=get_value date:"2000-07-19" date_to:"" -n=composed
```

| date       | value |
|------------|-------|
| 2000-07-19 | 2900  |

This value should be 10% of corn futures value on 2000-07-19. We purposely set hotels value to 0 to easily verify the weights are correct.

```shell
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"2000-07-22" -n=composed
```

| date       | value  |
|------------|--------|
| 2000-07-18 | 150000 |
| 2000-07-19 | 29000  |
| 2000-07-20 | 250000 |
| 2000-07-21 | 300000 |
| 2000-07-22 | 250000 |

### Fill behavior

| date       | corn | hotel | expected |
|------------|------|-------|----------|
| 2000-07-23 | 30   | 30    | 30       |
| 2000-07-24 | 25   | 25    | 25       |
| 2000-07-25 | 30   |       | 25,5     |
| 2000-07-26 | 35   | 25    | 26       |
| 2000-07-27 | 40   | 30    | 31       |
| 2000-07-28 | 45   |       | 31,5     |
| 2000-07-29 |      | 25    | 27       |
| 2000-07-30 | 50   | 50    | 50       |

```shell
../../.build/kwil-cli database call -a=get_value date:"2000-07-23" date_to:"2000-07-30" -n=composed
```

Expected:

| date       | value |
|------------|-------|
| 2000-07-23 | 30000 |
| 2000-07-24 | 25000 |
| 2000-07-25 | 25500 |
| 2000-07-26 | 26000 |
| 2000-07-27 | 31000 |
| 2000-07-28 | 31500 |
| 2000-07-29 | 27000 |
| 2000-07-30 | 50000 |

```shell
../../.build/kwil-cli database call -a=get_value date:"2000-07-28" date_to:"2000-07-30" -n=composed
```

| date       | value |
|------------|-------|
| 2000-07-28 | 31500 |
| 2000-07-29 | 27000 |
| 2000-07-30 | 50000 |

### Expect all of these to error:

```shell
# wrong date format
../../.build/kwil-cli database call -a=get_index date:"2000/07/18" date_to:"" -n=composed
```

```shell
# wrong date_to format
../../.build/kwil-cli database call -a=get_index date:"2000-07-18" date_to:"2000/07/22" -n=composed
```