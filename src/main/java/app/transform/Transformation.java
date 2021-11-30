package app.transform;

import org.json.simple.JSONObject;

import java.io.IOException;
import java.lang.ProcessBuilder;

import app.Connector;
import app.utils.Constants;

public class Transformation extends Connector {
    private String mapper;
    private String reducer;

    public Transformation(JSONObject config) {
        super(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        this.mapper = (String) config.get("mapper");
        this.reducer = (String) config.get("reducer");
    }

    @Override
    public void execute() {
        try {
            Process p = new ProcessBuilder("mapred", "streaming",
                    "-input", Constants.WORKING_DIR,
                    "-output", Constants.OUTGOING_DIR,
                    "-mapper", mapper,
                    "-reducer", reducer).start();
        } catch (IOException e) {
            System.out.println(e);
        } catch (Exception e) {
            System.out.println(e);
        }

    }

}
