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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class LoaderService {

    private static final Logger logger = LoggerFactory.getLogger(LoaderService.class);
    private static final String TYPE_PREDICATE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    public GraphLoader loadIMDBGraph(Model model) {
        GraphLoader loader = new GraphLoader();
        Set<String> types = new HashSet<>();
        VF2DataGraph graph = new VF2DataGraph();
        try {
            StmtIterator dataTriples = model.listStatements();

            while (dataTriples.hasNext()) {
                Statement stmt = dataTriples.nextStatement();
                String subjectNodeURL = stmt.getSubject().getURI().toLowerCase();
                if (subjectNodeURL.length() > 16) {
                    subjectNodeURL = subjectNodeURL.substring(16);
                }

                String[] temp = subjectNodeURL.split("/");
                if (temp.length != 2) {
                    System.out.println("Error: " + subjectNodeURL);
                    continue;
                }
                String subjectType = temp[0];
                String subjectID = temp[1];

                // TODO: Ignore the node if the type is not in the validTypes and optimizedLoadingBasedOnTGFD is true

                types.add(subjectType);


            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return loader;
    }

    public GraphLoader loadDBPedia(Model model) {
        Map<String, Vertex> nodeMap = new HashMap<>();
        Graph<Vertex, RelationshipEdge> dataGraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        Set<String> types = new HashSet<>();

        loadNodeMap(model, nodeMap, dataGraph, types);
        loadDataGraph(model, nodeMap, dataGraph);

        VF2DataGraph graph = new VF2DataGraph(dataGraph, nodeMap);
        return new GraphLoader(graph, types);
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

                Vertex v = nodeMap.getOrDefault(nodeURI, null);
                if (v == null) {
                    v = new Vertex(nodeURI, nodeType);
                    dataGraph.addVertex(v);
                    nodeMap.put(nodeURI, v);
                }
//                else {
//                    v.addType(nodeType);
//                }
                types.add(nodeType);
            }
            logger.info("Done. Number of Types: " + nodeMap.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
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
                    } else if (subjectNodeURI.equals(objectNodeURI)) {
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

}