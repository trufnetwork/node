test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse ../../internal/contracts/primitive_stream_template.kf
```

### Deploy & Init

deploy contract
```shell
../../.build/kwil-cli database drop primitive_stream_000000000000001 --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/primitive_stream_template.kf --name primitive_stream_000000000000001 --sync
```

call init. If you run twice, it should error.
```shell
../../.build/kwil-cli database execute --action=init -n=primitive_stream_000000000000001 --sync 
```

### Metadata

insert `read_visibility` -> 1 (private)
```shell
../../.build/kwil-cli database execute key:read_visibility value:1 val_type:int --action=insert_metadata -n=primitive_stream_000000000000001 --sync 
```

get `read_visibility`
```shell
../../.build/kwil-cli database call key:read_visibility only_latest:false --action=get_metadata -n=primitive_stream_000000000000001
```

Check read access for stream
```shell
wallet=0x$(../../.build/kwil-cli account id --private-key 0000000000000000000000000000000000000000000000000000000000000123)
../../.build/kwil-cli database call wallet:$wallet --action=is_wallet_allowed_to_read -n=primitive_stream_000000000000001
```

Give read permission to wallet
```shell
other=0x$(../../.build/kwil-cli account id --private-key 0000000000000000000000000000000000000000000000000000000000000123)

# add permission
../../.build/kwil-cli database execute key:allow_read_wallet value:$other val_type:ref --action=insert_metadata -n=primitive_stream_000000000000001 --sync
# read the permission
../../.build/kwil-cli database call key:allow_read_wallet ref:$other --action=get_metadata -n=primitive_stream_000000000000001
```

Check stream owner
```shell
owner=0x$(../../.build/kwil-cli account id)
other=0x$(../../.build/kwil-cli account id --private-key 0000000000000000000000000000000000000000000000000000000000000123)

# should be true
../../.build/kwil-cli database call wallet:$owner --action=is_stream_owner -n=primitive_stream_000000000000001

# should be false
../../.build/kwil-cli database call wallet:$other --action=is_stream_owner -n=primitive_stream_000000000000001 --owner $owner
```

transfer ownership
```shell
new_owner_pk=0000000000000000000000000000000000000000000000000000000000000456
new_owner=0x$(../../.build/kwil-cli account id --private-key $new_owner_pk)
old_owner=0x$(../../.build/kwil-cli account id)
../../.build/kwil-cli database execute new_owner:$new_owner --action=transfer_stream_ownership -n=primitive_stream_000000000000001 --sync

// should be true
../../.build/kwil-cli database call wallet:$new_owner --action=is_stream_owner -n=primitive_stream_000000000000001

// should be false
../../.build/kwil-cli database call wallet:$old_owner --action=is_stream_owner -n=primitive_stream_000000000000001

# transfer back
../../.build/kwil-cli database execute new_owner:$old_owner --action=transfer_stream_ownership -n=primitive_stream_000000000000001 --sync --private-key $new_owner_pk --owner $old_owner
```

disable latest `read_visibility`
```shell
# Extract the latest row_id of key read_visibility and convert to UUID
row_id=$(../../.build/kwil-cli database call key:read_visibility only_latest:true --action=get_metadata -n=primitive_stream_000000000000001 --output json | jq -r '.result[0].row_id | @sh')
uuid=$(python3 -c 'import uuid, sys; print(uuid.UUID(bytes=bytes(map(int, sys.argv[1].split()))).urn[9:])' "$row_id")

# Disable the metadata
../../.build/kwil-cli database execute row_id:$uuid --action=disable_metadata -n=primitive_stream_000000000000001 --sync
```

#### Metadata Errors

insert with bad type
```shell
../../.build/kwil-cli database execute key:compose_visibility value:1 val_type:bad_type --action=insert_metadata -n=primitive_stream_000000000000001 --sync 
```

insert readonly prop
```shell
../../.build/kwil-cli database execute key:type value:other val_type:string --action=insert_metadata -n=primitive_stream_000000000000001 --sync 
```

disable readonly metadata
```shell
# Extract the latest row_id of key `type` and convert to UUID
row_id=$(../../.build/kwil-cli database call key:type only_latest:true --action=get_metadata -n=primitive_stream_000000000000001 --output json | jq -r '.result[0].row_id | @sh')
uuid=$(python3 -c 'import uuid, sys; print(uuid.UUID(bytes=bytes(map(int, sys.argv[1].split()))).urn[9:])' "$row_id")

# Disable the metadata
../../.build/kwil-cli database execute row_id:$uuid --action=disable_metadata -n=primitive_stream_000000000000001 --sync
```

### Insert Record

insert record
```shell
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000001 date_value:2021-01-01 value:1 --sync 
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000001 date_value:2021-01-02 value:2 --sync 
```

### Get Index

get index
```shell
../../.build/kwil-cli database call --action=get_index date_from: date_to: frozen_at: -n=primitive_stream_000000000000001
../../.build/kwil-cli database call --action=get_index date_from:2021-01-01 date_to: frozen_at: -n=primitive_stream_000000000000001
../../.build/kwil-cli database call --action=get_index date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=primitive_stream_000000000000001
```

try read when it's private (make sure you set to private read access)
```shell
owner=$(../../.build/kwil-cli account id)
../../.build/kwil-cli database call --action=get_index date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=primitive_stream_000000000000001 --private-key 0000000000000000000000000000000000000000000000000000000000000123 --owner $owner
```

### Get Record

get record
```shell
../../.build/kwil-cli database call --action=get_record date_from: date_to: frozen_at: -n=primitive_stream_000000000000001
../../.build/kwil-cli database call --action=get_record date_from: date_to: frozen_at:2 -n=primitive_stream_000000000000001
../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to: frozen_at: -n=primitive_stream_000000000000001
../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=primitive_stream_000000000000001
```

try read when it's private (make sure you set to private read access)
```shell
owner=$(../../.build/kwil-cli account id)
../../.build/kwil-cli database call --action=get_record date_from: date_to: frozen_at: -n=primitive_stream_000000000000001 --private-key 0000000000000000000000000000000000000000000000000000000000000123 --owner $owner
../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to: frozen_at: -n=primitive_stream_000000000000001 --private-key 0000000000000000000000000000000000000000000000000000000000000123 --owner $owner
../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to:2021-01-01 frozen_at: -n=primitive_stream_000000000000001 --private-key 0000000000000000000000000000000000000000000000000000000000000123 --owner $owner
```
