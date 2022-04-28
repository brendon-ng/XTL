package app.transform;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.lang.ProcessBuilder;
import java.lang.String;

import app.Connector;
import app.hdfs.*;
import app.utils.Constants;

import java.util.ArrayList;
import java.util.Iterator;
import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.jgrapht.traverse.*;

public class Transformation extends Connector {
    private class SingleTransform {
        public String id;
        public String mapper;
        public String reducer;
        public ArrayList<String> dependencies;
    }

    private ArrayList<SingleTransform> nodes;

    private Graph<SingleTransform, DefaultEdge> graph;

    private void makeGraph() {
        System.out.println("\n----BUILDING GRAPH----");
        graph = new SimpleDirectedGraph<>(DefaultEdge.class);
        for(int i = 0; i < nodes.size(); i++) {
            graph.addVertex(nodes.get(i));
        }
        for(int i = 0; i < nodes.size() - 1; i++) {
            for(int j = i+1; j < nodes.size(); j++) {
                SingleTransform first = nodes.get(i);
                SingleTransform second = nodes.get(j);
                if (first.dependencies.contains(second.id)) {
                    System.out.println(second.id + "->" + first.id);
                    graph.addEdge(second, first);
                }
                if (second.dependencies.contains(first.id)) {
                    System.out.println(first.id + "->" + second.id);
                    graph.addEdge(first, second);
                }
            }
        }
        System.out.println("--------------------\n");
    }
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

            try {
                HDFSUtils.deleteDir(Constants.OUTGOING_DIR + "_" + node.id, Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT);
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
                System.exit(1);
            }

            nodes.add(node);
        }
    }

    @Override
    public void execute() {
        // first need to topologically sort nodes based on dependencies
        makeGraph();
        Iterator<SingleTransform> iter = new TopologicalOrderIterator<>(graph);
        while (iter.hasNext()) {
            SingleTransform vertex = iter.next();
            try {
                String output = Constants.OUTGOING_DIR + "_" + vertex.id;

                System.out.println("Vertex Id: " + vertex.id + "\noutput: " + output);
                System.out.println("Mapper: " + vertex.mapper + "\nReducer: " + vertex.reducer + "\n");
                
                String command = "mapred streaming -output " + output + " -mapper " + vertex.mapper + " -reducer " + vertex.reducer;
                
                if(vertex.dependencies.size() == 0){
                    System.out.println("Input: " + Constants.WORKING_DIR);
                    command += " -input " + (Constants.WORKING_DIR);
                }
                for(int i = 0; i < vertex.dependencies.size(); i++){
                    System.out.println("Input: " + Constants.OUTGOING_DIR + "_" + vertex.dependencies.get(i));
                    command += " -input " + Constants.OUTGOING_DIR + "_" + vertex.dependencies.get(i);
                }
                
                String[] spl = command.split(" ");

                for(String each: spl) {
                    System.out.println(each);
                }
                Process p = new ProcessBuilder(spl).inheritIO().start();
                p.waitFor();

                while(!HDFSUtils.checkExists(output + "/_SUCCESS",  Constants.HDFS_WORKING_ADDR, Constants.HDFS_WORKING_PORT)) {
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                System.out.println(e);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

}
