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

insert `compose_visibility` -> 1
```shell
../../.build/kwil-cli database execute key:compose_visibility value:1 val_type:int --action=insert_metadata -n=composed_stream_a --sync 
```

get `compose_visibility`
```shell
../../.build/kwil-cli database call key:compose_visibility only_latest:false --action=get_metadata -n=composed_stream_a
```

#### Metadata Errors

insert with bad type
```shell
../../.build/kwil-cli database execute key:compose_visibility value:1 val_type:bad_type --action=insert_metadata -n=composed_stream_a --sync 
```

insert readonly prop
```shell
../../.build/kwil-cli database execute key:type value:other val_type:string --action=insert_metadata -n=composed_stream_a --sync 
```