package app.transform;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.lang.ProcessBuilder;

import app.Connector;
import app.utils.Constants;

import java.util.ArrayList;

public class Transformation extends Connector {
    private class SingleTransform {
        public String id;
        public String mapper;
        public String reducer;
        public ArrayList<String> dependencies;
    }

    private ArrayList<SingleTransform> nodes;

    public Transformation(JSONObject config) {
        super(config);
    }

    @Override
    protected void parseJSON(JSONObject config) {
        nodes = new ArrayList<SingleTransform>();
        JSONArray entries = (JSONArray) config.get("entries");

        // loop through all entries
        for (int i = 0; i < entries.size(); i++) {
            JSONObject value = (JSONObject) entries.get(i);

            // for each entry add new node to our list of jobs
            SingleTransform node = new SingleTransform();
            node.id = (String) value.get("id");
            node.mapper = (String) value.get("mapper");
            node.reducer = (String) value.get("reducer");
            node.dependencies = new ArrayList<String>();
            JSONArray dependencyJson = (JSONArray) value.get("dependencies");
            for(int j = 0; j < dependencyJson.size(); j++) {
                node.dependencies.add((String) dependencyJson.get(j));
            }

            nodes.add(node);
        }
    }

    @Override
    public void execute() {
        // first need to topologically sort nodes based on dependencies
        try {
            System.out.println(nodes);

            Process p = new ProcessBuilder("mapred", "streaming",
                    "-input", Constants.WORKING_DIR,
                    "-output", Constants.OUTGOING_DIR,
                    "-mapper", "cat",
                    "-reducer", "wc").start();
            p.waitFor();
        } catch (IOException e) {
            System.out.println(e);
        } catch (Exception e) {
            System.out.println(e);
        }

    }

}
