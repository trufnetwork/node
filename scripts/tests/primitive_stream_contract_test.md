test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse ../../internal/contracts/primitive_stream_template.kf
```

### Deploy & Init

deploy contract
```shell
../../.build/kwil-cli database drop primitive_stream_a --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/primitive_stream_template.kf --name primitive_stream_a --sync
```

call init. If you run twice, it should error.
```shell
../../.build/kwil-cli database execute --action=init -n=primitive_stream_a --sync 
```

### Metadata

insert `compose_visibility` -> 1
```shell
../../.build/kwil-cli database execute key:compose_visibility value:1 val_type:int --action=insert_metadata -n=primitive_stream_a --sync 
```

get `compose_visibility`
```shell
../../.build/kwil-cli database call key:compose_visibility only_latest:false --action=get_metadata -n=primitive_stream_a
```

disable latest `compose_visibility`
```shell
# Extract the latest row_id of key compose_visibility and convert to UUID
row_id=$(../../.build/kwil-cli database call key:compose_visibility only_latest:true --action=get_metadata -n=primitive_stream_a --output json | jq -r '.result[0].row_id | @sh')
uuid=$(python3 -c 'import uuid, sys; print(uuid.UUID(bytes=bytes(map(int, sys.argv[1].split()))).urn[9:])' "$row_id")

# Disable the metadata
../../.build/kwil-cli database execute row_id:$uuid --action=disable_metadata -n=primitive_stream_a --sync
```

#### Metadata Errors

insert with bad type
```shell
../../.build/kwil-cli database execute key:compose_visibility value:1 val_type:bad_type --action=insert_metadata -n=primitive_stream_a --sync 
```

insert readonly prop
```shell
../../.build/kwil-cli database execute key:type value:other val_type:string --action=insert_metadata -n=primitive_stream_a --sync 
```

disable readonly metadata
```shell
# Extract the latest row_id of key `type` and convert to UUID
row_id=$(../../.build/kwil-cli database call key:type only_latest:true --action=get_metadata -n=primitive_stream_a --output json | jq -r '.result[0].row_id | @sh')
uuid=$(python3 -c 'import uuid, sys; print(uuid.UUID(bytes=bytes(map(int, sys.argv[1].split()))).urn[9:])' "$row_id")

# Disable the metadata
../../.build/kwil-cli database execute row_id:$uuid --action=disable_metadata -n=primitive_stream_a --sync
```

### Insert Record

insert record
```shell
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_a date_value:2021-01-01 value:1 --sync 
```
