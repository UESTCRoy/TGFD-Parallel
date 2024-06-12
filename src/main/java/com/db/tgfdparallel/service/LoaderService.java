package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.apache.commons.lang3.tuple.Pair;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

@Service
public class LoaderService {

    private static final Logger logger = LoggerFactory.getLogger(LoaderService.class);
    private static final String TYPE_PREDICATE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    public GraphLoader loadIMDB(Model model) {
        Map<String, Vertex> nodeMap = new HashMap<>();
        Graph<Vertex, RelationshipEdge> dataGraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        try {
            StmtIterator dataTriples = model.listStatements();
            while (dataTriples.hasNext()) {
                Statement stmt = dataTriples.nextStatement();
                String[] temp = processURI(stmt);
                if (temp == null) {
                    continue;
                }

                String subjectType = temp[0];
                String subjectID = temp[1];

//                if (!vertexTypes.contains(subjectType)) {
//                    continue;
//                }

                Vertex subjectVertex = getOrCreateVertex(subjectID, subjectType, nodeMap, dataGraph);

                String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                RDFNode object = stmt.getObject();
                String objectNodeURI;

                if (object.isLiteral()) {
                    objectNodeURI = object.asLiteral().getString().toLowerCase();
                    subjectVertex.getAttributes().add(new Attribute(predicate, objectNodeURI));
                } else {
                    objectNodeURI = object.toString().toLowerCase();
                    if (objectNodeURI.length() > 16) {
                        objectNodeURI = objectNodeURI.substring(16);
                    }

                    temp = objectNodeURI.split("/");

                    if (temp.length != 2) {
                        StringBuilder sb = new StringBuilder(temp[1]);
                        for (int i = 2; i < temp.length; i++) {
                            sb.append("/").append(temp[i]);
                        }
                        String combined = sb.toString();

                        String decodedStr = "";
                        try {
                            decodedStr = URLDecoder.decode(combined, "UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            logger.error("Error decoding URI segment: " + combined, e);
                        }
                        temp[1] = decodedStr;
                    } else {
                        temp[1] = URLDecoder.decode(temp[1], "UTF-8");
                    }

                    String objectType = temp[0];
                    String objectID = temp[1];
                    Vertex objectVertex = getOrCreateVertex(objectID, objectType, nodeMap, dataGraph);
                    boolean edgeAdded = dataGraph.addEdge(subjectVertex, objectVertex, new RelationshipEdge(predicate));
                }
            }
            logger.info("Done. Nodes: {}, Edges: {}", nodeMap.size(), dataGraph.edgeSet().size());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        VF2DataGraph graph = new VF2DataGraph(dataGraph, nodeMap);
        return new GraphLoader(graph);
    }

    // TODO: 待修改Vertex URI
    public GraphLoader loadDBPedia(Model model, Set<String> vertexTypes) {
        Map<String, Vertex> nodeMap = new HashMap<>();
        Graph<Vertex, RelationshipEdge> dataGraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        loadNodeMap(model, nodeMap, dataGraph, vertexTypes);
        loadDataGraph(model, nodeMap, dataGraph);

        VF2DataGraph graph = new VF2DataGraph(dataGraph, nodeMap);
        return new GraphLoader(graph);
    }

    public void loadNodeMap(Model model, Map<String, Vertex> nodeMap, Graph<Vertex, RelationshipEdge> dataGraph, Set<String> types) {
        try {
            StmtIterator typeTriples = model.listStatements();
            while (typeTriples.hasNext()) {
                Statement stmt = typeTriples.nextStatement();
                if (!stmt.getPredicate().getURI().equals(TYPE_PREDICATE_URI)) {
                    continue;
                }

                Pair<String, String> processedData = processStatement(stmt);
                String nodeURI = processedData.getKey();

                String nodeType = stmt.getObject().asResource().getLocalName().toLowerCase();
                if (nodeType.trim().length() == 0) {
                    continue;
                }
                // 仅过滤histogram里的types
                if (types.size() != 0 && !types.contains(nodeType)) {
                    continue;
                }

                Vertex v = nodeMap.getOrDefault(nodeURI, null);
                if (v == null) {
                    v = new Vertex(nodeURI, nodeType);
                    dataGraph.addVertex(v);
                    nodeMap.put(nodeURI, v);
                }
            }
            logger.info("Done. Number of Vertex: " + nodeMap.size());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void loadDataGraph(Model model, Map<String, Vertex> nodeMap, Graph<Vertex, RelationshipEdge> dataGraph) {
        try {
            StmtIterator dataTriples = model.listStatements();
            while (dataTriples.hasNext()) {
                Statement stmt = dataTriples.nextStatement();
                if (stmt.getPredicate().getURI().equals(TYPE_PREDICATE_URI)) {
                    continue;
                }

                Pair<String, String> processedData = processStatement(stmt);
                String subjectNodeURI = processedData.getKey();
                String predicate = processedData.getValue();

                String objectNodeURI = processObjectNodeURI(stmt.getObject());

                Vertex subjVertex = nodeMap.getOrDefault(subjectNodeURI, null);
                if (subjVertex == null) {
                    continue;
                }

                if (!stmt.getObject().isLiteral()) {
                    Vertex objVertex = nodeMap.getOrDefault(objectNodeURI, null);
                    if (objVertex == null) {
                        continue;
                    }
                    if (subjectNodeURI.equals(objectNodeURI)) {
                        continue;
                    }
                    boolean edgeAdded = dataGraph.addEdge(subjVertex, objVertex, new RelationshipEdge(predicate));
                } else {
                    // TODO: There might has a case that an attribute has multi-value attributes
                    subjVertex.getAttributes().add(new Attribute(predicate, objectNodeURI));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private Pair<String, String> processStatement(Statement stmt) {
        String predicate = stmt.getPredicate().getLocalName().toLowerCase();
        String nodeURI = stmt.getSubject().getURI().toLowerCase();
        if (nodeURI.length() > 28) {
            nodeURI = nodeURI.substring(28);
        }
        return new ImmutablePair<>(nodeURI, predicate);
    }

    private String processObjectNodeURI(RDFNode object) {
        String objectNodeURI;
        if (object.isLiteral()) {
            objectNodeURI = object.asLiteral().getString().toLowerCase();
        } else {
            objectNodeURI = object.toString().substring(object.toString().lastIndexOf("/") + 1).toLowerCase();
        }
        return objectNodeURI;
    }

    private String[] processURI(Statement stmt) {
        String[] temp = stmt.getSubject().getURI().toLowerCase().substring(16).split("/");
        if (temp.length == 2) {
            try {
                temp[1] = URLDecoder.decode(temp[1], "UTF-8");
            } catch (UnsupportedEncodingException e) {
                logger.error("Error decoding URI segment: " + temp[1], e);
            }
            return temp;
        }

        // Handle cases where temp.length > 2
        StringBuilder sb = new StringBuilder(temp[1]);
        for (int i = 2; i < temp.length; i++) {
            sb.append("/").append(temp[i]);
        }
        String combined = sb.toString();

        String decodedStr = "";
        try {
            decodedStr = URLDecoder.decode(combined, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("Error decoding URI segment: " + combined, e);
        }

        return new String[]{temp[0], decodedStr};
    }

    private Vertex getOrCreateVertex(String id, String type, Map<String, Vertex> nodeMap, Graph<Vertex, RelationshipEdge> dataGraph) {
        String uniqueId = id + "-" + type;

        return nodeMap.computeIfAbsent(uniqueId, key -> {
            Vertex newVertex = new Vertex(id, type);
            dataGraph.addVertex(newVertex);
            return newVertex;
        });
    }

}