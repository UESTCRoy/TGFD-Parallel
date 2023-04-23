package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service
public class LoaderService {

    private static final Logger logger = LoggerFactory.getLogger(LoaderService.class);
    private static final String TYPE_PREDICATE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";;

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
        GraphLoader loader = new GraphLoader();
        Set<String> types = new HashSet<>();
        VF2DataGraph graph = new VF2DataGraph();
        int numberOfObjectsNotFound = 0;
        int numberOfSubjectsNotFound = 0;

        try {
            StmtIterator dataTriples = model.listStatements();

            while (dataTriples.hasNext()) {
                Statement stmt = dataTriples.nextStatement();
                String predicateURI = stmt.getPredicate().getURI().toLowerCase();

                if (predicateURI.equals(TYPE_PREDICATE_URI)) {
                    String nodeURI = stmt.getSubject().getURI().toLowerCase();
                    if (nodeURI.length() > 28) {
                        nodeURI = nodeURI.substring(28);
                    }

                    String nodeType = stmt.getObject().asResource().getLocalName().toLowerCase();
                    if (nodeType.trim().length() == 0) {
                        continue;
                    }

                    if (graph.getNodeMap().containsKey(nodeURI)) {
                        types.add(nodeType);
                    } else {
                        Vertex vertex = new Vertex(nodeURI, nodeType);
                        graph.getNodeMap().put(nodeURI, vertex);
                        graph.getGraph().addVertex(vertex);
                    }
                    loader.getTypes().add(nodeType);
                } else {
                    String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                    String subjectNodeURI = stmt.getSubject().getURI().toLowerCase();
                    if (subjectNodeURI.length() > 28) {
                        subjectNodeURI = subjectNodeURI.substring(28);
                    }

                    RDFNode object = stmt.getObject();
                    String objectNodeURI = null;

                    if (object.isLiteral()) {
                        objectNodeURI = object.asLiteral().getString().toLowerCase();
                    } else {
                        objectNodeURI = object.toString().substring(object.toString().lastIndexOf("/") + 1).toLowerCase();
                    }

                    if (graph.getNodeMap().containsKey(subjectNodeURI)) {
                        if (!object.isLiteral()) {
                            if (graph.getNodeMap().containsKey(objectNodeURI)) {
                                graph.getGraph().addEdge(graph.getNodeMap().get(subjectNodeURI), graph.getNodeMap().get(objectNodeURI), new RelationshipEdge(predicate));
                                loader.setGraphSize(loader.getGraphSize() + 1);
                            } else {
                                numberOfObjectsNotFound++;
                            }
                        } else {
                            graph.getNodeMap().get(subjectNodeURI).getAttributes().add(new Attribute(predicate, objectNodeURI));
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return loader;
    }

}