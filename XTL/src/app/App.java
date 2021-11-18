package app;

import java.io.BufferedReader;

public class App {
    public static void main(String[] args) throws Exception {
        app.inbound_connectors.main();

        inbound_connectors connector = new app.inbound_connectors();
        System.out.println(connector.getDataSource());
        BufferedReader r = connector.readFile("/resources/SparkSQLOutputData.csv");
        String line;
        while ((line = r.readLine()) != null) {
            System.out.println(line);
            System.out.flush();
        }
        r.close();
    }
}