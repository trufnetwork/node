
```shell
../.build/kwild --autogen
```

```shell
rm -r ~/.kwild
```

```shell
../.build/kwil-cli database deploy -p=./tsn/base_schema/base_schema.kf --sync --name=com_truflation_us_hotel_price
```

```shell
../.build/kwil-cli database batch --sync --path ./temp_sources_transformed/com_truflation_us_hotel_price.csv --action add_record --name=com_truflation_us_hotel_price
```

```shell
../.build/kwil-cli database deploy --sync -p=./tsn/base_schema/base_schema.kf --name=com_yahoo_finance_corn_futures --sync
```

```shell
../.build/kwil-cli database batch --sync --path ./temp_sources_transformed/com_yahoo_finance_corn_futures.csv --action add_record --name=com_yahoo_finance_corn_futures --sync
```

```shell
../.build/kwil-cli database deploy -p=./example_schemas/experiment_table.kf --name=experiment_table --sync
```



```shell
cd ..;
task build;
```

```shell
../.build/kwil-cli database list --self
```