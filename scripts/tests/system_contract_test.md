test parse to find accurate error locations
```shell
../../.build/kwil-cli utils parse ../../internal/contracts/system_contract.kf
```


deploy system contract
```shell
../../.build/kwil-cli database drop system_contract --sync
../../.build/kwil-cli database deploy -p=../../internal/contracts/system_contract.kf --name system_contract --sync
```

kwil-cli

accept stream
```shell
../../.build/kwil-cli database execute data_provider:0xfC43f5F9dd45258b3AFf31Bdbe6561D97e8B71de stream_id:st123456789012345678901234567890 --action=accept_stream -n=system_contract --sync 
```

revoke stream
```shell
../../.build/kwil-cli database execute data_provider:0xfC43f5F9dd45258b3AFf31Bdbe6561D97e8B71de stream_id:st123456789012345678901234567890 --action=revoke_stream -n=system_contract --sync
```