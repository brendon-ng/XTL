package app;

import java.io.*;

// enum for supported data sources
enum Source {
    SPARKSQL, HDFS
}

public class inbound_connectors {
    // Class for all inbound connectors - extensible
    private Source dataSource;

    // constructor that sets the data source
    // T use configuration file to fill in this value
    public inbound_connectors() {
        dataSource = Source.SPARKSQL;
    }

    public Source getDataSource() {
        return dataSource;
    }

    // run based on data source specified
    public void extract() {
        switch (dataSource) {
        case SPARKSQL:
            return;
        default:
            return;
        }
    }

    // Idea: helper function for reading files in a uniform manner, then connectors
    // deal with that interface to convert to JSON / CSV

    // General readFile function to return BufferedReader
    public BufferedReader readFile(String file) {

        // NOTE: After calling this function, make sure to close the stream -- this
        // function does not close the string in order for lines to be accessible
        // outside of its scope
        InputStream stream = inbound_connectors.class.getResourceAsStream(file);

        BufferedReader buffer = new BufferedReader(new InputStreamReader(stream));
        return buffer;
        // return null;
    }

    public static void main() {
        System.out.println("hello world");
    }
}