test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse ../../internal/contracts/composed_stream_template.kf
```


deploy contract
```shell
../../.build/kwil-cli database drop composed_stream_a --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/composed_stream_template.kf --name composed_stream_a --sync
```

call init. If you run twice, it should error.
```shell
../../.build/kwil-cli database execute --action=init -n=composed_stream_a --sync 
```