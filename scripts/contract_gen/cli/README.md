# Contract Gen CLI

The contract gen CLI is a toy tool to display generating Kuneiform Contracts from a template. It has 3 flags:

- `name`: the name that the contract should be given
- `import`: the contracts that are being imported and their weights to the "composed_stream". The name and weight should be separated by colon, and the streams should be separated by column: "xdbid:10,ydbid:25".
- `out`: the name of the output contract file

To use it, simply run:

```sh
go run ./cli.go -name mydb -import xdbid:10,ydbid:25 -out mydb.json
```

The outputs are in JSON, which can be deployed to Kwil using the CLI's JSON flag:

```
kwil-cli database deploy ./mydb.json --type json
```
