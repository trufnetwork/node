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

#### Metadata Errors

insert with bad type
```shell
../../.build/kwil-cli database execute key:compose_visibility value:1 val_type:bad_type --action=insert_metadata -n=primitive_stream_a --sync 
```

insert readonly prop
```shell
../../.build/kwil-cli database execute key:type value:other val_type:string --action=insert_metadata -n=primitive_stream_a --sync 
```

### Insert Record

insert record
```shell
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_a date_value:2021-01-01 value:1 --sync 
../../.build/kwil-cli database execute --action=insert_record -n=primitive_stream_a date_value:2021-01-02 value:2 --sync 
```

### Get Index

get index
```shell
../../.build/kwil-cli database call --action=get_index date_from:2021-01-01 date_to:2021-01-02 frozen_at: -n=primitive_stream_a
```
