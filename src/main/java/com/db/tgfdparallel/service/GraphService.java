package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.jgrapht.alg.isomorphism.VF2SubgraphIsomorphismInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class GraphService {
    private static final Logger logger = LoggerFactory.getLogger(GraphService.class);
    private final AppConfig config;
    private final LoaderService loaderService;
    private final ActiveMQService activeMQService;
    private final DataShipperService dataShipperService;

    @Autowired
    public GraphService(AppConfig config, LoaderService loaderService, ActiveMQService activeMQService, DataShipperService dataShipperService) {
        this.config = config;
        this.loaderService = loaderService;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
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

    public Graph<Vertex, RelationshipEdge> getSubGraphWithinDiameter(Graph<Vertex, RelationshipEdge> graph, Vertex center, int diameter) {
        Graph<Vertex, RelationshipEdge> subgraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        List<Vertex> withinDiameter = new ArrayList<>();
        // Define a HashMap to store visited vertices
        HashMap<String, Integer> visited = new HashMap<>();

        // Create a queue for BFS
        LinkedList<Vertex> queue = new LinkedList<>();

        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(center.getUri(), 0);
        queue.add(center);
        // Store the center as the node within the diameter
        withinDiameter.add(center);

        Vertex v, w;
        while (!queue.isEmpty()) {
            // Dequeue a vertex from queue and get its distance
            v = queue.poll();
            int distance = visited.get(v.getUri());

            // Get both outgoing and incoming edges
            Set<RelationshipEdge> edges = new HashSet<>(graph.outgoingEdgesOf(v));
            edges.addAll(graph.incomingEdgesOf(v));

            for (RelationshipEdge edge : edges) {
                w = getConnectedVertex(v, edge);
                // Check if the vertex is not visited
                // Check if the vertex is within the diameter
                if (distance + 1 <= diameter && !visited.containsKey(w.getUri()) && graph.containsVertex(w)) {
                    // Enqueue the vertex and add it to the visited set
                    visited.put(w.getUri(), distance + 1);
                    queue.add(w);
                    withinDiameter.add(w);
                }
            }
        }

        for (Vertex vertex : withinDiameter) {
            subgraph.addVertex(vertex);
        }

        for (Vertex source : withinDiameter) {
            for (RelationshipEdge e : graph.outgoingEdgesOf(source)) {
                if (visited.containsKey(e.getTarget().getUri()))
                    subgraph.addEdge(e.getSource(), e.getTarget(), e);
            }
        }

        return subgraph;
    }

    private Vertex getConnectedVertex(Vertex v, RelationshipEdge edge) {
        if (v.equals(edge.getSource())) {
            return edge.getTarget();
        } else {
            return edge.getSource();
        }
    }

    public Graph<Vertex, RelationshipEdge> extractGraphToBeSent(GraphLoader graphLoader, ArrayList<SimpleEdge> edges) {
        Graph<Vertex, RelationshipEdge> graphToBeSent = new DefaultDirectedGraph<>(RelationshipEdge.class);
        HashSet<String> visited = new HashSet<>();

        for (SimpleEdge edge : edges) {
            Vertex src = addVertexToGraphIfNotVisited(graphLoader, graphToBeSent, visited, edge.getSrc());
            Vertex dst = addVertexToGraphIfNotVisited(graphLoader, graphToBeSent, visited, edge.getDst());

            if (src != null && dst != null) {
                graphToBeSent.addEdge(src, dst, new RelationshipEdge(edge.getLabel()));
            }
        }
        return graphToBeSent;
    }

    private Vertex addVertexToGraphIfNotVisited(GraphLoader graphLoader, Graph<Vertex, RelationshipEdge> graph, HashSet<String> visited, String nodeId) {
        if (visited.contains(nodeId)) {
            return null;
        }

        Vertex node = graphLoader.getGraph().getNodeMap().getOrDefault(nodeId, null);

        if (node != null) {
            graph.addVertex(node);
            visited.add(nodeId);
        }
        return node;
    }

    public void mergeGraphs(VF2DataGraph base, Graph<Vertex, RelationshipEdge> inputGraph) {
        inputGraph.vertexSet().forEach(inputVertex -> {
            Vertex currentVertex = base.getNodeMap().getOrDefault(inputVertex.getUri(), null);

            if (currentVertex == null) {
                base.getGraph().addVertex(inputVertex);
            } else {
                currentVertex.setAttributes(inputVertex.getAttributes());
                currentVertex.setTypes(inputVertex.getTypes());
            }
        });

        inputGraph.edgeSet().forEach(e -> {
            Vertex src = e.getSource();
            Vertex dst = e.getTarget();

            boolean exist = base.getGraph().outgoingEdgesOf(src).stream()
                    .anyMatch(edge -> edge.getLabel().equals(e.getLabel()) &&
                            (edge.getTarget()).getUri().equals(dst.getUri()));

            if (!exist) {
                base.getGraph().addEdge(src, dst, e);
            }
        });
    }

    public VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> checkIsomorphism(Graph<Vertex, RelationshipEdge> dataGraph, VF2PatternGraph pattern, boolean cacheEdges) {
        return new VF2SubgraphIsomorphismInspector<>(dataGraph, pattern.getPattern(), Comparators.vertexComparator, Comparators.edgeComparator, cacheEdges);
    }

    public Attribute copyAttribute(Attribute attribute) {
        Attribute newAttribute = new Attribute();
        newAttribute.setAttrName(attribute.getAttrName());
        newAttribute.setAttrValue(attribute.getAttrValue());
        newAttribute.setNull(attribute.isNull());
        return newAttribute;
    }

    public Vertex copyVertex(Vertex vertex) {
        Vertex newVertex = new Vertex();
        newVertex.setUri(vertex.getUri());
        newVertex.setTypes(vertex.getTypes());
        newVertex.setMarked(vertex.isMarked());

        // Deep copy of attributes
        Set<Attribute> newAttributes = vertex.getAttributes().stream()
                .map(this::copyAttribute)
                .collect(Collectors.toSet());

        newVertex.setAttributes(newAttributes);

        return newVertex;
    }

    public void updateFirstSnapshot(GraphLoader graphLoader) {
        boolean datashipper = false;
        HashMap<Integer, ArrayList<SimpleEdge>> dataToBeShipped = new HashMap<>();
        VF2DataGraph graph = graphLoader.getGraph();

        try {
            while (!datashipper) {
                String msg = activeMQService.receive();
                if (msg.startsWith("#datashipper")) {
                    dataToBeShipped = dataShipperService.readEdgesToBeShipped(msg);
                    logger.info("The data to be shipped has been received.");
                    datashipper = true;
                }
            }

            dataToBeShipped.forEach((workerID, edges) -> {
                try {
                    Graph<Vertex, RelationshipEdge> extractedGraph = extractGraphToBeSent(graphLoader, edges);
                    dataShipperService.sendGraphToBeShippedToOtherWorkers(extractedGraph, workerID);
                } catch (Exception e) {
                    logger.error("Error while extracting and sending graph for workerID: " + workerID, e);
                }
            });

            int receiveData = 0;
            activeMQService.connectConsumer(config.getNodeName() + "_data");

            while (receiveData < dataToBeShipped.size() - 1) {
                logger.info("*WORKER*: Start reading data from other workers...");
                String msg = activeMQService.receive();
                logger.info("*WORKER*: Received a new message.");

                if (msg != null) {
                    logger.info("*DATA RECEIVER*: Graph object has been received from '" + msg + "' successfully");
                    Object obj = dataShipperService.downloadObject(msg);
                    if (obj != null) {
                        Graph<Vertex, RelationshipEdge> receivedGraph = (Graph<Vertex, RelationshipEdge>) obj;
                        logger.info("*WORKER*: Received a new graph.");
                        if (receivedGraph != null) {
                            mergeGraphs(graph, receivedGraph);
                        }
                    }
                }
                receiveData++;
            }

            activeMQService.sendResult(1);
        } catch (Exception e) {
            logger.error("Error while running first snapshot", e);
        }
    }

    public GraphLoader updateNextSnapshot(List<Change> changeList, GraphLoader baseLoader) {
        try {
            updateEntireGraph(baseLoader.getGraph(), changeList);
//            activeMQService.sendResult(superStepNumber);
        } catch (Exception e) {
            logger.error("Error while updating graph and sending results" , e);
        }
        return baseLoader;
    }
}