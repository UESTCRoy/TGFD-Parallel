package com.db.tgfdparallel.domain;

import lombok.Data;
import org.jgrapht.Graph;

@Data
public class VF2PatternGraph {
    private Graph<Vertex, RelationshipEdge> pattern;
    private int diameter;
    private String centerVertexType;
    private Vertex centerVertex;
}
