# Environment Setup

## Starting HDFS

To start up HDFS running locally on your machine, run `make start`.  
You will then be in a bash shell within the `namenode` Docker container that serves as the NameNode of the HDFS instance running on your machine.

## Stopping HDFS

Run `make stop`

## Clearing HDFS and deleting all HDFS files

Coming soon...

# Interacting with HDFS

Within the shell of the namenode:

    # HDFS list commands to show all the directories in root "/"
    hdfs dfs -ls /
    # Create a new directory inside HDFS using mkdir tag.
    hdfs dfs -mkdir -p /user/root
    # Copy the files to the input path in HDFS.
    hdfs dfs -put <file_name> <path>
    # Have a look at the content of your input file.
    hdfs dfs -cat <input_file>

HDFS UI Dashboard: [http://localhost:9870/](http://localhost:9870/)
