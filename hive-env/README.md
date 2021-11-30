# Environment Setup

## Starting Hive

To start up Hive running locally on your machine, run `make start`.  
You will then be in a hive shell where you need to enter the script:  
`CREATE TABLE pokes (foo INT, bar STRING);`  
`LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;`

## Stopping Hive

Run `make clear`
