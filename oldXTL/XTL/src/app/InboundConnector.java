package app;

import java.io.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.csv.*;

// enum for supported data sources
enum Source {
    SPARKSQL, HDFS
}

public class InboundConnector {
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

    // Convert CSV file to JSON file, save that file in outbound_data folder
    public void convertCSVtoJSON(BufferedReader r) throws IOException {
        // https://www.tutorialspoint.com/convert-csv-to-json-using-the-jackson-library-in-java
        // https://stackoverflow.com/questions/19766266/directly-convert-csv-file-to-json-file-using-the-jackson-library

        // NEED ABSOLUTE PATH FROM ROOT XTL DIRECTORY
        File input = new File("XTL/src/resources/SparkSQLOutputData.csv");
        File output = new File("outbound_data/sparksql.json");

        CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();

        MappingIterator<Map<?, ?>> mappingIterator = csvMapper.readerFor(Map.class).with(bootstrap).readValues(input);

        List<Map<?, ?>> data = mappingIterator.readAll();

        // write to json file
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(output, data);
    }

    public static void main() {
        System.out.println("hello world");
    }
}