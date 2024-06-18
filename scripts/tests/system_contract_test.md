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

### Allowed Composability Streams

deploy and fetch from primitive stream

```shell
../../.build/kwil-cli database drop system_contract --sync
../../.build/kwil-cli database drop primitive_stream_000000000000003 --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/system_contract.kf --name system_contract --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/primitive_stream_template.kf --name primitive_stream_000000000000003 --sync
../../.build/kwil-cli database execute --action=init -n=primitive_stream_000000000000003 --sync
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000003 date_value:2021-01-01 value:1 --sync

owner=$(../../.build/kwil-cli account id)
# try getting from allowed composability streams (should work)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000003 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract

# set compose_visibility to 1 (private) from the stream by inserting a metadata
../../.build/kwil-cli database execute --action=insert_metadata -n=primitive_stream_000000000000003 key:compose_visibility value:1 val_type:int --sync

# try getting from allowed composability streams (should error)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000003 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract

# insert a metadata allow_compose_stream to allow composability, assuming the private key is 001, the dbid of the system contract is xa9595222f0c9bdf337c51153f473998c30eaa3007e329c425078b18f
../../.build/kwil-cli database execute --action=insert_metadata -n=primitive_stream_000000000000003 key:allow_compose_stream value:xa9595222f0c9bdf337c51153f473998c30eaa3007e329c425078b18f val_type:ref --sync

# try getting from allowed composability streams (should work)
../../.build/kwil-cli database call data_provider:$owner stream_id:primitive_stream_000000000000003 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract
```

test with composed stream

```shell
# deploy composed stream
../../.build/kwil-cli database drop composed_stream_0000000000000003 --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/composed_stream_template.kf --name composed_stream_0000000000000003 --sync
../../.build/kwil-cli database execute --action=init -n=composed_stream_0000000000000003 --sync

# get owner
owner=$(../../.build/kwil-cli account id)

# set data_provider to owner
../../.build/kwil-cli database execute data_providers:$owner stream_ids:primitive_stream_000000000000003 weights:1 --action=set_taxonomy -n=composed_stream_0000000000000003 --sync

# try getting from allowed composability streams (should error)
../../.build/kwil-cli database call data_provider:$owner stream_id:composed_stream_0000000000000003 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract

# insert a metadata allow_compose_stream to allow composability, assuming the private key is 001, the dbid of the composed stream is xad21e6518020850a2af996148c535e9c4ade45c88d28f46b29cdd727 to primitive_stream_000000000000003
../../.build/kwil-cli database execute --action=insert_metadata -n=primitive_stream_000000000000003 key:allow_compose_stream value:xad21e6518020850a2af996148c535e9c4ade45c88d28f46b29cdd727 val_type:ref --sync

# insert a metadata allow_compose_stream to allow composability, assuming the private key is 001, the dbid of the system contract is xa9595222f0c9bdf337c51153f473998c30eaa3007e329c425078b18f to composed_stream_0000000000000003
../../.build/kwil-cli database execute --action=insert_metadata -n=composed_stream_0000000000000003 key:allow_compose_stream value:xa9595222f0c9bdf337c51153f473998c30eaa3007e329c425078b18f val_type:ref --sync

# try getting from allowed composability streams (should work)
../../.build/kwil-cli database call data_provider:$owner stream_id:composed_stream_0000000000000003 date_from:2021-01-01 --action=get_unsafe_record -n=system_contract
```