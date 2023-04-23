package com.db.tgfdparallel.domain;

import lombok.Data;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.HashMap;

@Data
public class VF2DataGraph {
    private Graph<Vertex, RelationshipEdge> graph = new DefaultDirectedGraph<>(RelationshipEdge.class);
    private HashMap<String, Vertex> nodeMap;
}
