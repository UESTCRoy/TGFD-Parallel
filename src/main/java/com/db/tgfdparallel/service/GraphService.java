package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.jgrapht.Graph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class GraphService {
    private static final Logger logger = LoggerFactory.getLogger(GraphService.class);
    private final AppConfig config;
    private final LoaderService loaderService;

    @Autowired
    public GraphService(AppConfig config, LoaderService loaderService) {
        this.config = config;
        this.loaderService = loaderService;
    }

    public Map<String, Integer> initializeFromSplitGraph(List<String> paths) {
        Map<String, Integer> VertexFragment = new HashMap<>();

        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            System.out.println("Loading graph from " + path);

            if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt")) {
                System.out.println("File format not supported");
                continue;
            }
            Model dataModel = RDFDataMgr.loadModel(path);

            GraphLoader graphLoader = new GraphLoader();
            switch (config.getDataset()) {
                case "dbpedia":
                    graphLoader = loaderService.loadDBPedia(dataModel);
                case "imdb":
//                    loader = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
                case "synthetic":
//                    loader = new SyntheticLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
            }

            for (Vertex v : graphLoader.getGraph().getGraph().vertexSet()) {
                String uri = v.getUri();
                VertexFragment.put(uri, i + 1);
            }
        }
        return VertexFragment;
    }

    public GraphLoader loadFirstSnapshot(String path) {
        GraphLoader loader = new GraphLoader();

        Model dataModel = RDFDataMgr.loadModel(path);
        switch (config.getDataset()) {
            case "dbpedia":
                loader = loaderService.loadDBPedia(dataModel);
            case "imdb":
//                    loader = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
            case "synthetic":
//                    loader = new SyntheticLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
        }

        return loader;
    }

    /**
     * Updates the entire graph with the given list of changes.
     *
     * @param baseGraph  the graph to update
     * @param allChanges the list of changes to apply
     */
    public void updateEntireGraph(VF2DataGraph baseGraph, List<Change> allChanges) {
        for (Change change : allChanges) {
            ChangeType changeType = change.getChangeType();

            if (change instanceof VertexChange) {
                processVertexChange(baseGraph, changeType, (VertexChange) change);
            } else if (change instanceof EdgeChange) {
                processEdgeChange(baseGraph, changeType, (EdgeChange) change);
            } else if (change instanceof AttributeChange || change instanceof TypeChange) {
                processAttributeOrTypeChange(baseGraph, change, changeType);
            }
        }
    }

    private void processVertexChange(VF2DataGraph graph, ChangeType changeType, VertexChange change) {
        Vertex vertex = change.getVertex();

        if (changeType == ChangeType.insertVertex) {
            addVertex(graph, vertex);
        } else if (changeType == ChangeType.deleteVertex) {
            deleteVertex(graph, vertex);
        }
    }

    private void processEdgeChange(VF2DataGraph graph, ChangeType changeType, EdgeChange change) {
        HashMap<String, Vertex> nodeMap = graph.getNodeMap();
        Vertex srcVertex = nodeMap.getOrDefault(change.getSrcURI(), null);
        Vertex dstVertex = nodeMap.getOrDefault(change.getDstURI(), null);

        if (srcVertex == null || dstVertex == null) {
            return;
        }

        RelationshipEdge edge = new RelationshipEdge(change.getLabel());

        if (changeType == ChangeType.insertEdge) {
            addEdge(graph, srcVertex, dstVertex, edge);
        } else if (changeType == ChangeType.deleteEdge) {
            removeEdge(graph, srcVertex, dstVertex, edge);
        } else {
            throw new IllegalArgumentException("The change is instance of EdgeChange, but type of change is: " + changeType);
        }
    }

    private void processAttributeOrTypeChange(VF2DataGraph graph, Change change, ChangeType changeType) {
        String uri = change instanceof AttributeChange ? ((AttributeChange) change).getVertexURI() : ((TypeChange) change).getPreviousVertex().getUri();
        Vertex vertex = graph.getNodeMap().get(uri);

        if (vertex == null) {
            return;
        }

        if (change instanceof AttributeChange) {
            AttributeChange attributeChange = (AttributeChange) change;
            Attribute attr = attributeChange.getAttribute();
            Set<Attribute> attributes = vertex.getAttributes();

            if (changeType == ChangeType.changeAttr || changeType == ChangeType.insertAttr) {
                attributes.add(attr);
            } else if (changeType == ChangeType.deleteAttr) {
                attributes.remove(attr);
            }
            vertex.setAttributes(attributes);
        } else {
            Vertex newVertex = ((TypeChange) change).getNewVertex();
            String newType = newVertex.getTypes();
            vertex.setTypes(newType);
        }
    }

    // TODO: 与原来的缺了allGroupedChanges
    public List<Change> loadChanges(JSONArray jsonArray, Set<String> vertexSets, Map<String, Set<String>> typeChangeURIs, boolean considerEdgeChanges) {
        List<Change> allChanges = new ArrayList<>();
        for (Object o : jsonArray) {
            JSONObject object = (JSONObject) o;
            Set<String> relevantTypes = new HashSet<>((Collection<? extends String>) object.get("types"));

            ChangeType type = ChangeType.valueOf((String) object.get("typeOfChange"));
            int id = Integer.parseInt(object.get("id").toString());

            if (!considerEdgeChanges && (type == ChangeType.deleteEdge || type == ChangeType.insertEdge)) {
                continue;
            }

            Change change;
            switch (type) {
                case deleteEdge:
                case insertEdge:
                    String srcURI = (String) object.get("src");
                    String dstURI = (String) object.get("dst");

                    if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s))) {
                        if (notURIofInterest(srcURI, vertexSets, typeChangeURIs)) {
                            continue;
                        } else if (notURIofInterest(dstURI, vertexSets, typeChangeURIs)) {
                            continue;
                        }
                    }
                    String label = (String) object.get("label");
                    change = new EdgeChange(id, type, srcURI, dstURI, label);
                    break;
                case changeAttr:
                case deleteAttr:
                case insertAttr:
                    String uri = (String) object.get("uri");

                    if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s)) && notURIofInterest(uri, vertexSets, typeChangeURIs)) {
                        continue;
                    }

                    JSONObject attrObject = (JSONObject) object.get("attribute");
                    String attrName = (String) attrObject.get("attrName");
                    String attrValue = (String) attrObject.get("attrValue");
                    change = new AttributeChange(id, type, uri, new Attribute(attrName, attrValue));
                    break;
                case changeType:
                    Vertex previousVertex = getVertex((JSONObject) object.get("previousVertex"));
                    Vertex newVertex = getVertex((JSONObject) object.get("newVertex"));
                    String uriOfType = (String) object.get("uri");

                    if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s)) && notURIofInterest(uriOfType, vertexSets, typeChangeURIs)) {
                        continue;
                    }

                    change = new TypeChange(id, type, uriOfType, previousVertex, newVertex);
                    break;
                case deleteVertex:
                case insertVertex:
                    Vertex vertex = getVertex((JSONObject) object.get("vertex"));
                    change = new VertexChange(id, type, vertex);
                    break;
                default:
                    continue;
            }
            allChanges.add(change);
        }
        return allChanges;
    }

    public boolean notURIofInterest(String uri, Set<String> vertexSets, Map<String, Set<String>> typeChangeURIs) {
        if (typeChangeURIs.containsKey(uri))
            return vertexSets.stream().noneMatch(s -> typeChangeURIs.get(uri).contains(s));
        else {
            return true;
        }
    }

    public Vertex getVertex(JSONObject vertexObj) {
        String uri = (String) vertexObj.get("vertexURI");
        String type = (String) vertexObj.get("types");

        Set<Attribute> allAttributes = new HashSet<>();
        JSONArray allAttributeLists = (JSONArray) vertexObj.get("allAttributesList");
        for (Object allAttributeList : allAttributeLists) {
            JSONObject attrObject = (JSONObject) allAttributeList;
            String attrName = (String) attrObject.get("attrName");
            String attrValue = (String) attrObject.get("attrValue");
            allAttributes.add(new Attribute(attrName, attrValue));
        }

        Vertex vertex = new Vertex(uri, type);
        vertex.setAttributes(allAttributes);
        return vertex;
    }

    public void addVertex(VF2DataGraph baseGraph, Vertex vertex) {
        Graph<Vertex, RelationshipEdge> graph = baseGraph.getGraph();
        Map<String, Vertex> nodeMap = baseGraph.getNodeMap();
        nodeMap.computeIfAbsent(vertex.getUri(), uri -> {
            graph.addVertex(vertex);
            return vertex;
        });
    }

    public void deleteVertex(VF2DataGraph baseGraph, Vertex vertex) {
        if (vertex == null) {
            return;
        }

        Graph<Vertex, RelationshipEdge> graph = baseGraph.getGraph();
        Map<String, Vertex> nodeMap = baseGraph.getNodeMap();

        if (!nodeMap.containsKey(vertex.getUri())) {
            return;
        }

        List<RelationshipEdge> edgesToDelete = new ArrayList<>(graph.edgesOf(vertex));
        edgesToDelete.forEach(graph::removeEdge);

        boolean deleteVertex = graph.removeVertex(vertex);
        nodeMap.remove(vertex.getUri());

    }

    public void addEdge(VF2DataGraph baseGraph, Vertex src, Vertex dst, RelationshipEdge edge) {
        Graph<Vertex, RelationshipEdge> graph = baseGraph.getGraph();
        graph.addEdge(src, dst, edge);
    }

    public void removeEdge(VF2DataGraph graph, Vertex v1, Vertex v2, RelationshipEdge edge) {
        Graph<Vertex, RelationshipEdge> baseGraph = graph.getGraph();
        for (RelationshipEdge e : baseGraph.outgoingEdgesOf(v1)) {
            Vertex target = e.getTarget();
            if (target.getUri().equals(v2.getUri()) && edge.getLabel().equals(e.getLabel())) {
                baseGraph.removeEdge(e);
                return;
            }
        }
    }

    public ArrayList<RelationshipEdge> getEdgesWithinDiameter(Graph<Vertex, RelationshipEdge> graph, Vertex center, int diameter) {
        ArrayList<RelationshipEdge> edges = new ArrayList<>();
        // Define a Map to store visited vertices
        Map<String, Integer> visited = new HashMap<>();
        // Create a queue for BFS
        Queue<Vertex> queue = new LinkedList<>();

        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(center.getUri(), 0);
        queue.add(center);

        Vertex currentVertex;

        while (!queue.isEmpty()) {
            currentVertex = queue.poll();
            int currentDistance = visited.get(currentVertex.getUri());

            // Outgoing edges
            processEdges(graph.outgoingEdgesOf(currentVertex), true, currentDistance, diameter, visited, queue, edges);

            // Incoming edges
            processEdges(graph.incomingEdgesOf(currentVertex), false, currentDistance, diameter, visited, queue, edges);

        }

        return edges;
    }

    private void processEdges(Set<RelationshipEdge> edges, boolean isOutgoing, int currentDistance, int diameter,
                              Map<String, Integer> visited, Queue<Vertex> queue, List<RelationshipEdge> resultEdges) {
        for (RelationshipEdge edge : edges) {
            Vertex adjacentVertex = isOutgoing ? edge.getTarget() : edge.getSource();

            // Check if the vertex is not visited
            if (!visited.containsKey(adjacentVertex.getUri())) {

                // Check if the vertex is within the diameter
                if (currentDistance + 1 <= diameter) {
                    resultEdges.add(edge);
                    visited.put(adjacentVertex.getUri(), currentDistance + 1);
                    queue.add(adjacentVertex);
                }
            }
        }
    }
}