package app;

import java.io.BufferedReader;

public class App {
    private:
        InboundConnector in;
        OutboundConnector out;
        
    public static void main(String[] args) throws Exception {
        app.inbound_connectors.main();

        inbound_connectors connector = new app.inbound_connectors();
        System.out.println(connector.getDataSource());
        BufferedReader r = connector.readFile("/resources/SparkSQLOutputData.csv");
        // convert to json
        connector.convertCSVtoJSON(r);
        r.close();
    }
}