# Schema Gen CLI

The schema gen CLI is a toy tool to display generating Kuneiform schemas from a template. It has 3 flags:

- `name`: the name that the schema should be given
- `import`: the schemas that are being imported and their weights to the "compose_truflation_streams". The name and weight should be separated by colon, and the streams should be separated by column: "xdbid:10,ydbid:25".
- `out`: the name of the output schema file

To use it, simply run:

```sh
go run ./cli.go -name mydb -import xdbid:10,ydbid:25 -out mydb.json
```

The outputs are in JSON, which can be deployed to Kwil using the CLI's JSON flag:

```
kwil-cli database deploy ./mydb.json --type json
```
