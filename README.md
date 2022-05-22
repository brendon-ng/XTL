# XTL

XTL - An Extensible Extract-Transform-Load (ETL) framework

## Usage

[Install Maven](https://maven.apache.org/install.html) and make sure you can call `mvn` from terminal.  

#### To Build XTL
`make build`

#### To Run XTL
`make run <path-to-config-json>`

#### To Build/Run XTL with DAG
`make dag-build <path-to-config-json>`

`make dag-run`

##### Other useful commands:
- `make clean` - cleans working directories/packages from previous builds
- `make dag-clean` - cleans DAG directories from previous builds

## DAG-Enabled XTL
Construct a pipeline of data processing tasks by specifying a Directed Acyclic Graph (DAG) structure for XTL steps. 

Example 2-step XTL pipeline:
![Diagrams](https://user-images.githubusercontent.com/43181799/169713062-738d9ac9-93f3-4afe-9170-b34af034f22c.jpg)

Corresponding configuration JSON:
```
{
    "graph": {
        "nodes": [
            {"label": "1"},
            {"label": "2"}
        ],
        "edges": [
            {"source": "1", "target": "2"}
        ]
    },
    "configs":
    [{
        "label": 1,
        "importer": {
            "platform": "FILE",
            "address": "127.0.0.1",
            "port": "8020",
            "inputFilepath": "dir1/data.csv"
        },
        "exporter": {
            "platform": "HDFS",
            "address": "127.0.0.1",
            "port": "8020",
            "outputFilepath": "/processing"
        }
    }
    ,
{
    "label": 2,
    "importer": {
        "platform": "HDFS",
        "address": "127.0.0.1",
        "port": "8020",
        "inputFilepath": "/processing/data.csv"
    },
    "transformation": {
        "mapper": "cat",
        "reducer": "\"grep 10*\"",
        "filename": "data2.csv"
    },
    "exporter": {
        "platform": "HDFS",
        "address": "127.0.0.1",
        "port": "8020",
        "outputFilepath": "/output"
    }
}]}
```

For individual platforms' importer/transformation/exporter configuration schema, check their README files within their respective directories.
