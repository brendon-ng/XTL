package app.transform;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.lang.ProcessBuilder;

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
        System.out.println();
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
                String input = "";
                if(vertex.dependencies.size() == 0){
                    input = Constants.WORKING_DIR;
                }
                for(int i = 0; i < vertex.dependencies.size(); i++){
                    input += Constants.OUTGOING_DIR + "_" + vertex.dependencies.get(i) + " ";
                }

                System.out.println("Vertex Id: " + vertex.id + "\n" + "input: " + input);
                Process p = new ProcessBuilder("mapred", "streaming",
                        "-input", input,
                        "-output", Constants.OUTGOING_DIR + "_" + vertex.id,
                        "-mapper", vertex.mapper,
                        "-reducer", vertex.reducer).start();
                p.waitFor();
            } catch (IOException e) {
                System.out.println(e);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

}
