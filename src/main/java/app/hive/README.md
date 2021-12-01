# Hive Configuration Information

## Configuration file information required for Hive connectors

**All of this must be in JSON format**

Hive Importer:

- `platform`: `HIVE`
- `address`: the address that your hive instance is running from
- `port`: the port that your hive instance is running from
- `query`: initial query to run on input data

Hive Exporter:

- `platform`: `HIVE`
- `address`: the address that your hive instance is running from
- `port`: the port that your hive instance is running from
- `tableName`: the name of the hive table to load your data into
- `tableColumns`: the column names and types of the hive table to load your data into
- `dockerContainerID`: the container ID of your docker container running hive

Example:

```
{
    "importer": {
        "platform": "HIVE",
        "address": "localhost",
        "port": "10000",
        "query": "SELECT * FROM pokes"
    },
    "exporter": {
        "platform": "HIVE",
        "address": "localhost",
        "port": "10000",
        "tableName": "HIVE_OUTPUT",
        "tableColumns": "(COLUMN1 STRING, COLUMN2 STRING)",
        "dockerContainerID": "035f7e57561b"
    }
}
```

