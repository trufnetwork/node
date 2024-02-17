# TSN Extensions

This directory contains Kwil extensions to implement TSN functionality. Extensions are registered in the top-level `extension.go`. Any registered extensions will be included in the binary build process.

There are 3 extensions present here:

## Mathutil

`mathutil` provides a basic function to multiply an integer by a fraction. This allows for weighting values used within composed streams.

Below is an example of multiplying the number 100 by 4/9:

```
use mathutil as math;

action my_action() public {
	$result = math.fraction(100, 4, 9); // 100 * (4/19) = 44.444, rounds to 44
}
```

## Basestream

`basestream` allows any schema storing data to be turned into a Truflation stream. It can be configured to be given a `table_name`, `date_column`, and a `value_column` from which it will get values.

It is capable of calculating both an index as well as a value for any date (in `YYYY-MM-DD` format). The index is the calculation described [here](<https://system.docs.truflation.com/backend/cpi-calculations/workflow/normalizing-data>) The value is simply the value for any given date. If either an empty string or a zero value is passed, it will return the most recent date:

```
use basestream {
    table_name: 'prices',
    date_column: 'date_value',
    value_column: 'value'
} as basestream;

table prices {
  id text primary notnull minlen(36) maxlen(36), 
  date_value text notnull unique minlen(10) maxlen(10),
  value int notnull,
  created_at int notnull 
}

// public action to add record to prices table
action add_record ($id, $date_value, $value, $created_at) public {
  INSERT INTO prices (id, date_value, value, created_at)
  VALUES ($id, $date_value, $value, $created_at);
}

action get_index($date) public view {
    $val = basestream.get_index($date);
    SELECT $val AS result;
}

action get_value($date) public view {
    $val = basestream.get_value($date);
    SELECT $val AS result;
}
```

## Truflation_Streams

`truflation_streams` allows composing of schemas that are valid Truflation streams. Any schema that has `get_index(YYYY-MM-DD)` and `get_value(YYYY-MM-DD)` actions is a valid Truflation stream. Both actions must return a single int64.

Using the `mathutil` extension, we can read individual streams, assign them a weight, and return a value:

```
use truflation_streams as streams;
use mathutil as math;

action get_index($date) public view {
    $beef_idx = streams.get_index('beef_dbid', $date);
    $corn_idx = streams.get_index('corn_dbid', $date);

    $beef = math.fraction($beef_idx, 7, 10);
    $corn = math.fraction($corn_idx, 3, 10);

    select $beef + $corn AS result;
}

action get_value($date) public view {
    $beef_value = streams.get_value('beef_dbid', $date);
    $corn_value = streams.get_value('corn_dbid', $date);

    $beef = math.fraction($beef_value, 7, 10);
    $corn = math.fraction($corn_value, 3, 10);

    select $beef + $corn AS result;
}
```

## Example

I have created and tested a basic example of composing streams together. To run this, we need to build a Kwil binary with these extensions.

The example schemas can also be found in this directory, under `example_schemas`.

### Beef, Corn, And Barley

The example uses 3 base streams, and creates 2 composite streams from them. The base streams are for beef, corn, and barley commodities.

The first composite stream combines the beef and corn stream into a beef_corn stream. The beef is weighted as 70% of the stream, while the corn is weighted as 30% of the stream.

The second composite stream combines the beef_corn stream with the barley base stream. It weights the beef_corn stream at 90%, and the barley stream at 10%.

### Using The Example

To use the example, build this binary using the repo README, and run the `kwild` node. Refer to the Kwil docs on [how to run a node](<https://docs.kwil.com/docs/node/quickstart>).

#### Configure your CLI

Configure your CLI by calling `kwil-cli configure` and then add the following configs:
```bash
$ kwil-cli configure

Kwil RPC URL: http://truflation-testnet.kwil.com
Kwil Chain ID: truflation-testnet
Private Key: <any-ethereum-private-key>
```

You may use the default localhost and empty chain ID if you are running a local node.
Refer to the Kwil docs on [here](https://docs.kwil.com/docs/ref/kwil-cli/database).
To verify that your CLI is configured properly, you can run:

```bash
$ kwil-cli utils ping
```

#### Deploy the Base Streams

Once the node is running, you can deploy the schemas found in `examples`. First, deploy the three base streams:

```bash
kwil-cli database deploy -p=./example_schemas/basestream.kf  -n=beef
kwil-cli database deploy -p=./example_schemas/basestream.kf  -n=corn
kwil-cli database deploy -p=./example_schemas/basestream.kf  -n=barley
```

Once deployed, seed them with some initial values. Remember that, in order to account for accuracy, we add 3 extra 0s to values. For example, 40.101 becomes 40101:

```bash
kwil-cli database execute -n=beef -a=add_record id:24c80091-a203-4182-a56b-e6891441e8aa date_value:2023-01-01 value:40101 created_at:$(date +%s)

kwil-cli database execute -n=corn -a=add_record id:24c80091-a203-4182-a56b-e6891441e8aa date_value:2023-01-01 value:4329 created_at:$(date +%s)

kwil-cli database execute -n=barley -a=add_record id:24c80091-a203-4182-a56b-e6891441e8aa date_value:2023-01-01 value:792 created_at:$(date +%s)
```

#### Deploy beef_corn Index

Once we have seeded data, we can create the beef_corn stream. To do this, edit the `composed_1.kf` stream to add in the DBID for your beef and corn streams. Then, deploy the stream:

```bash
kwil-cli database deploy -p=./example_schemas/composed_1.kf  -n=beef_corn
```

You can check that everything is working properly by getting the combined value from the beef_corn stream:

```bash
kwil-cli database call -a=get_value date: -n=beef_corn
```

#### Deploy beef_corn_barley Index

Just like in the above step, we will now use the `composed_2.kf` schema to compose the beef_corn stream with the barley stream. Edit the `composed_2.kf` to add in the dbids for the beef_corn and barley streams. Then, deploy the stream:

```bash
kwil-cli database deploy -p=./example_schemas/composed_2.kf  -n=beef_corn_barley
```

To check that it is working, we can get the value for the beef_corn_barley stream. If seeded with the values given above, this should give:

```bash
$ kwil-cli database call -a=get_value date: -n=beef_corn_barley
| result |
+--------+
|  26510 |
```
