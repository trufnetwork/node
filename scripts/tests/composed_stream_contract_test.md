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

../../.build/kwil-cli database execute key:allow_read_wallet value:$other val_type:ref --action=insert_metadata -n=composed_stream_a --sync
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

create a single child
```shell
../../.build/kwil-cli database execute data_providers:dp stream_ids:stid weights:1 --action=set_taxonomy -n=composed_stream_a --sync
```

create with multiple child
```shell
../../.build/kwil-cli database execute data_providers:dp,dp2 stream_ids:stid,stid2 weights:1,2 --action=set_taxonomy -n=composed_stream_a --sync
```