# Spark Configuration Information

## Configuration file information required for Spark connectors

**All of this must be in JSON format**

Spark SQL Importer:

- `platform`: `SPARK`
- `executionMode`: string to specify where Spark should run
  - e.g. `local`, `master`, `local[2]`, `{ipAddress}:{port}`
- `inputFilepath`: input filepath for data
- `query`: initial query to run on input data; the table will always be called `XTLInput`, so make sure this is the source of data in the query

Spark Exporter:

- `platform`: `SPARK`
- `executionMode`: string to specify where Spark should run
  - e.g. `local`, `master`, `local[2]`, `{ipAddress}:{port}`
- `appName`: application name for Spark

Example:

```
{
    "importer": {
        "platform": "SPARKSQL",
        "executionMode": "local",
        "inputFilepath": "resources/SparkSQLOutputData.csv",
        "query": "SELECT service FROM XTLInput"
    },
    "exporter": {
        "platform": "SPARK",
        "executionMode": "local",
        "appName": "xtlApp"
    }

}
```
