test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse ../../internal/contracts/system_contract.kf
```

deploy system contract
```shell
../../.build/kwil-cli database drop system_contract --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/system_contract.kf --name system_contract --sync
```

### Accept & Revoke Stream

To prepare:

- head to [primitive scripts](primitive_stream_contract_test.md#deploy--init), deploy and init
- [Insert records](primitive_stream_contract_test.md#insert-record)

accept stream
```shell
owner=$(../../.build/kwil-cli account id)
../../.build/kwil-cli database execute data_provider:$owner stream_id:primitive_stream_000000000000001 --action=accept_stream -n=system_contract --sync 
```

revoke stream
```shell
owner=$(../../.build/kwil-cli account id)
../../.build/kwil-cli database execute data_provider:$owner stream_id:primitive_stream_000000000000001 --action=revoke_stream -n=system_contract --sync
```

cannot accept inexistent stream
```shell
../../.build/kwil-cli database execute data_provider:fC43f5F9dd45258b3AFf31Bdbe6561D97e8B71de stream_id:st123456789012345678901234567890 --action=accept_stream -n=system_contract --sync 
```

### Get Unsafe Methods

Get record

```shell
owner=$(../../.build/kwil-cli account id)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000001 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract
```

Get index
```shell
owner=$(../../.build/kwil-cli account id)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000001 date_from:2021-01-01 --action=get_unsafe_index -n=system_contract
```

### Get Safe Methods

get record
```shell
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000001 date_from:2021-01-01 --action=get_record -n=system_contract
```

get index
```shell
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000001 date_from:2021-01-01 --action=get_index -n=system_contract
```

#### Error from unnoficial streams

deploy and fetch

```shell
../../.build/kwil-cli database drop primitive_stream_000000000000002 --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/primitive_stream_template.kf --name primitive_stream_000000000000002 --sync
../../.build/kwil-cli database execute --action=init -n=primitive_stream_000000000000002 --sync
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000002 date_value:2021-01-01 value:1 --sync 

owner=$(../../.build/kwil-cli account id)
# try getting from unofficial streams (should work)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000002 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract

# try getting from official streams (should error)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000002 date_from:2021-01-01 --action=get_index -n=system_contract

```