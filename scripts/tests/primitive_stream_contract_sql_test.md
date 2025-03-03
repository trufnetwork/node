test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse -i ../../internal/contracts/primitive_stream_unix.sql
```

### Deploy Namespace

deploy namespace of primitive stream
```shell
../../.build/kwil-cli exec-sql --file ../../internal/contracts/primitive_stream_unix.sql --sync
```