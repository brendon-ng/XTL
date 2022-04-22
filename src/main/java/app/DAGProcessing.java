package app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.jgrapht.*;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.SimpleDirectedGraph;

public class DAGProcessing {
    // variables needed: graph structure, configs for all of those
    private static JSONArray configArray;
    private static DirectedAcyclicGraph<String, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    private static ArrayList<String> nodes = new ArrayList<>();

    private static void parseNodes(JSONObject graph) {
        // nodes array in JSON object contains objects with 'label'
        // store all labels in independent array
        JSONArray nodeInfo = (JSONArray) graph.get("nodes");

        for (int i = 0; i < nodeInfo.size(); i++) {
            JSONObject node = (JSONObject) nodeInfo.get(i);
            String currentLabel = node.get("label").toString();
            if (!nodes.contains(currentLabel)) {
                nodes.add(currentLabel);
            }
        }
    }

    private static void buildGraph(ArrayList<String> allNodes, JSONArray edgeList) {
        // use JGraphT to build graph of XTL steps
        for (int i = 0; i < allNodes.size(); i++) {
            graph.addVertex(allNodes.get(i));
        }

        for (int i = 0; i < edgeList.size(); i++) {
            // add edges iteratively
            JSONObject edgeObject = (JSONObject) edgeList.get(i);
            String srcEdge = edgeObject.get("source").toString();
            String tgtEdge = edgeObject.get("target").toString();
            graph.addEdge(srcEdge, tgtEdge);
        }
    }

    private static ArrayList<String> getTopologicalSort(DirectedAcyclicGraph<String, DefaultEdge> g) {
        // assume g is populated with vertices and edghes
        ArrayList<String> sortedNodes = new ArrayList<>();
        Iterator<String> iter = g.iterator();
        while (iter.hasNext()) {
            sortedNodes.add(iter.next());
        }
        return sortedNodes;
    }

    public static ArrayList<String> graphWorkflow(JSONObject fullObject) {
        JSONObject graphJSON = (JSONObject) fullObject.get("graph"); // has nodes and edges
        JSONArray edgeList = (JSONArray) graphJSON.get("edges");
        System.out.println("\tParsing nodes...");
        parseNodes(graphJSON);
        System.out.println("\tBuilding graph...");
        buildGraph(nodes, edgeList);
        System.out.println("\tCompleting topological sort...");
        ArrayList<String> sortedNodes = getTopologicalSort(graph);
        return sortedNodes;
    }

    public static JSONObject readJSONFile(String[] args) throws IOException {
        if (args.length != 1) {
            IOException e = new IOException("Incorrect argument length");
            throw e;
        }
        // https://howtodoinjava.com/java/library/json-simple-read-write-json-examples/
        JSONParser jsonParser = new JSONParser();
        JSONObject fullObject = new JSONObject();
        try (FileReader reader = new FileReader(args[0])) {
            Object obj = jsonParser.parse(reader);
            fullObject = (JSONObject) obj; // fields: configs, graph

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return fullObject;
    }

    public static Hashtable<String, String> separateJSONFiles(JSONObject fullObject) throws IOException {
        configArray = (JSONArray) fullObject.get("configs");
        // JSONObject graphJSON = (JSONObject) fullObject.get("graph");
        Hashtable<String, String> fileDictionary = new Hashtable<String, String>();

        for (int i = 0; i < configArray.size(); i++) {
            String label = ((JSONObject) configArray.get(i)).get("label").toString();
            String filename = "dag/" + label + ".json";
            File f = new File(filename);
            f.createNewFile();
            System.out.println("File created: " + f.getName());
            FileWriter fw = new FileWriter(f);
            fw.write(configArray.get(i).toString());
            fw.close();

            // populate file dictionary
            fileDictionary.put(label, f.getAbsolutePath());
        }
        return fileDictionary;
    }

    public static void createExecutable(ArrayList<String> order, Hashtable<String, String> dict) throws IOException {
        // Create single bash script with separated JSON files for DAG processing
        File dir = new File("dag");
        File exec = new File("dag/run/execute.sh");
        exec.setExecutable(true);
        if (exec.createNewFile()) {
            System.out.println("Executable file created");
        }
        FileWriter fw = new FileWriter(exec);
        // File[] fileArray = dir.listFiles();
        // Hashtable<String, File> fileDictionary = new Hashtable<String, File>();

        // for (File f : dir.listFiles()) {
        // if (f.isFile()) {
        // fw.write("make run " + f.getAbsolutePath() + "\n");
        // }
        // }
        for (int i = 0; i < order.size(); i++) {
            String label = order.get(i);
            String filepath = dict.get(label);
            fw.write("make run " + filepath + "\n");
        }
        fw.close();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Reading JSON file...");
        JSONObject j = readJSONFile(args);
        System.out.println("Separating configs into separate JSON files...");
        Hashtable<String, String> fileDictionary = separateJSONFiles(j);
        System.out.println("Completing DAG workflow...");
        ArrayList<String> orderOfExecutable = graphWorkflow(j);
        System.out.println("Creating executable...");
        createExecutable(orderOfExecutable, fileDictionary);
        System.out.println("Successfully created executable file");
    }
}
