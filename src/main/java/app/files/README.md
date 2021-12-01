# Files Configuration Information

## Configuration file information required for File connectors

**All of this must be in JSON format**

File Importer:

- `platform`: `File`
- `inputFilepath`: input filepath for data

File Exporter:

- `platform`: `File`
- `outputFilepath`: output filepath for data

Example:

```
{
    "importer": {
        "platform": "FILE",
        "inputFilepath": "/User/JohnDoe/test.csv"
    },
    "exporter": {
        "platform": "FILE",
        "outputFilepath": "/User/JohnDoe/Desktop/"
    },
    ...
}
```
