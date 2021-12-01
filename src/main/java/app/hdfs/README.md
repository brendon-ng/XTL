# HDFS Configuration Information

## Configuration file information required for HDFS connectors

**All of this must be in JSON format**

HDFS Importer:

- `platform`: `HDFS`
- `inputFilepath`: input filepath for data
- `address`: string containing HDFS cluster address
  - e.g. `localhost`
- `port`: port number for your HDFS cluster

HDFS Exporter:

- `platform`: `HDFS`
- `outputFilepath`: output filepath for data
- `address`: string containing HDFS cluster address
  - e.g. `localhost`
- `port`: port number for your HDFS cluster

Example:

```
{
    "importer": {
        "platform": "HDFS",
        "address": "127.0.0.1",
        "port": "8020",
        "inputFilepath": "/input/SparkSQLOutputData.csv"
    },
    "exporter": {
        "platform": "HDFS",
        "address": "127.0.0.1",
        "port": "8020",
        "outputFilepath": "/output/SparkSQLOutputData.csv"
    },
    ...
}
```
