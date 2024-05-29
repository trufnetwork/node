test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse ../../internal/contracts/composed_stream_template.kf
```

### Deploy & Init

deploy contract
```shell
../../.build/kwil-cli database drop composed_stream_a --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/composed_stream_template.kf --name composed_stream_a --sync
```

call init. If you run twice, it should error.
```shell
../../.build/kwil-cli database execute --action=init -n=composed_stream_a --sync 
```

### Metadata

insert `read_visibility` -> 1 (private)
```shell
../../.build/kwil-cli database execute key:read_visibility value:1 val_type:int --action=insert_metadata -n=composed_stream_a --sync 
```

get `read_visibility`
```shell
../../.build/kwil-cli database call key:read_visibility only_latest:false --action=get_metadata -n=composed_stream_a
```

Check read access for stream
```shell
wallet=$(../../.build/kwil-cli account id --private-key 0000000000000000000000000000000000000000000000000000000000000123)
../../.build/kwil-cli database call wallet:$wallet --action=is_wallet_allowed_to_read -n=composed_stream_a
```

Give read permission to wallet
```shell
other=0x$(../../.build/kwil-cli account id --private-key 0000000000000000000000000000000000000000000000000000000000000123)

# add permission
../../.build/kwil-cli database execute key:allow_read_wallet value:$other val_type:ref --action=insert_metadata -n=composed_stream_a --sync
# read the permission
../../.build/kwil-cli database call key:allow_read_wallet ref:$other --action=get_metadata -n=composed_stream_a

```

Check stream owner
```shell
owner=0x$(../../.build/kwil-cli account id)
other=0x$(../../.build/kwil-cli account id --private-key 0000000000000000000000000000000000000000000000000000000000000123)

# should be true
../../.build/kwil-cli database call wallet:$owner --action=is_stream_owner -n=composed_stream_a

# should be false
../../.build/kwil-cli database call wallet:$other --action=is_stream_owner -n=composed_stream_a --owner $owner
```

transfer ownership
```shell
new_owner_pk=0000000000000000000000000000000000000000000000000000000000000456
new_owner=0x$(../../.build/kwil-cli account id --private-key $new_owner_pk)
old_owner=0x$(../../.build/kwil-cli account id)
../../.build/kwil-cli database execute new_owner:$new_owner --action=transfer_stream_ownership -n=composed_stream_a --sync

// should be true
../../.build/kwil-cli database call wallet:$new_owner --action=is_stream_owner -n=composed_stream_a

// should be false
../../.build/kwil-cli database call wallet:$old_owner --action=is_stream_owner -n=composed_stream_a

# transfer back
../../.build/kwil-cli database execute new_owner:$old_owner --action=transfer_stream_ownership -n=composed_stream_a --sync --private-key $new_owner_pk --owner $old_owner
```

disable latest `read_visibility`
```shell
# Extract the latest row_id of key read_visibility and convert to UUID
row_id=$(../../.build/kwil-cli database call key:read_visibility only_latest:true --action=get_metadata -n=composed_stream_a --output json | jq -r '.result[0].row_id | @sh')
uuid=$(python3 -c 'import uuid, sys; print(uuid.UUID(bytes=bytes(map(int, sys.argv[1].split()))).urn[9:])' "$row_id")

# Disable the metadata
../../.build/kwil-cli database execute row_id:$uuid --action=disable_metadata -n=composed_stream_a --sync
```

#### Metadata Errors

insert with bad type
```shell
../../.build/kwil-cli database execute key:read_visibility value:1 val_type:bad_type --action=insert_metadata -n=composed_stream_a --sync 
```

insert readonly prop
```shell
../../.build/kwil-cli database execute key:type value:other val_type:string --action=insert_metadata -n=composed_stream_a --sync 
```

disable readonly metadata
```shell
# Extract the latest row_id of key `type` and convert to UUID
row_id=$(../../.build/kwil-cli database call key:type only_latest:true --action=get_metadata -n=composed_stream_a --output json | jq -r '.result[0].row_id | @sh')
uuid=$(python3 -c 'import uuid, sys; print(uuid.UUID(bytes=bytes(map(int, sys.argv[1].split()))).urn[9:])' "$row_id")

# Disable the metadata
../../.build/kwil-cli database execute row_id:$uuid --action=disable_metadata -n=composed_stream_a --sync
```

### Taxonomies

create a single child, assuming data provider is `7e5f4552091a69125d5dfcb7b8c2659029395bdf` which comes from 001 private key's public key
```shell
../../.build/kwil-cli database execute data_providers:7e5f4552091a69125d5dfcb7b8c2659029395bdf stream_ids:stid weights:1 --action=set_taxonomy -n=composed_stream_a --sync
```

create with multiple child, assuming data provider is `7e5f4552091a69125d5dfcb7b8c2659029395bdf` which comes from 001 private key's public key
```shell
../../.build/kwil-cli database execute data_providers:7e5f4552091a69125d5dfcb7b8c2659029395bdf,7e5f4552091a69125d5dfcb7b8c2659029395bdf stream_ids:stid,stid2 weights:1,2 --action=set_taxonomy -n=composed_stream_a --sync
```

show taxonomies
```shell
../../.build/kwil-cli database call -a=describe_taxonomies -n=composed_stream_a
```

show only latest taxonomy
```shell
../../.build/kwil-cli database call latest_version:true -a=describe_taxonomies -n=composed_stream_a
```

disable taxonomy on version 2
```shell
../../.build/kwil-cli database execute version:2 --action=disable_taxonomy -n=composed_stream_a --sync
```

#### get records with taxonomy

complete test for get records with taxonomy, assuming data provider is `7e5f4552091a69125d5dfcb7b8c2659029395bdf` which comes from 001 private key's public key
```shell
../../.build/kwil-cli database drop composed_stream_0000000000000001
../../.build/kwil-cli database drop primitive_stream_000000000000001
../../.build/kwil-cli database drop primitive_stream_000000000000002 --sync

../../.build/kwil-cli database deploy -p=../../internal/contracts/primitive_stream_template.kf --name=primitive_stream_000000000000001
../../.build/kwil-cli database deploy -p=../../internal/contracts/primitive_stream_template.kf --name=primitive_stream_000000000000002
../../.build/kwil-cli database deploy -p=../../internal/contracts/composed_stream_template.kf --name=composed_stream_0000000000000001 --sync

../../.build/kwil-cli database execute --action=init -n=primitive_stream_000000000000001
../../.build/kwil-cli database execute --action=init -n=primitive_stream_000000000000002
../../.build/kwil-cli database execute --action=init -n=composed_stream_0000000000000001 --sync

../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000001 date_value:2021-01-01 value:1
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000001 date_value:2021-01-02 value:2 
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000002 date_value:2021-01-01 value:3 
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_000000000000002 date_value:2021-01-02 value:4 --sync

../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=primitive_stream_000000000000001
../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=primitive_stream_000000000000002

../../.build/kwil-cli database execute data_providers:7e5f4552091a69125d5dfcb7b8c2659029395bdf,7e5f4552091a69125d5dfcb7b8c2659029395bdf stream_ids:primitive_stream_000000000000001,primitive_stream_000000000000002 weights:1,2 --action=set_taxonomy -n=composed_stream_0000000000000001 --sync
../../.build/kwil-cli database call --action=get_record date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=composed_stream_0000000000000001
../../.build/kwil-cli database call --action=get_index date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=composed_stream_0000000000000001
```