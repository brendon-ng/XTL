# Transformation Configuration Information

## Configuration file information required for MapReduce transformation

**All of this must be in JSON format**

Transformation:

- `mapper`: Map function - can be any executable
- `reducer`: Reduce function - can be any executable
- `filename`: Optional rename file parameter to save results of MapReduce job (otherwise will be file in the form `part-0000-{GUID}.csv`)

Example:

```

    "transformation": {
    "mapper": "cat",
    "reducer": "\"grep 10*\"",
    "filename": "data2.csv"
    }

```
