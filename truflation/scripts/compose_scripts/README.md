## Scripts

### Generate Composed Streams
```sh
./generate_composed_streams.sh
```

Generate JSON schemas for streams composed of multiple streams with their respective value, sourced from `composed_streams.csv` file.
Generates schema for each parent stream. The name of the file is the name of the parent stream too.

### Deploy Composed Streams
```sh
./composed_deploy.sh
```

Drops and deploys composed streams using kwil-cli.
