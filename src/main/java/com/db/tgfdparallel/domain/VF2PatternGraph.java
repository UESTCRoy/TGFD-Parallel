package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.jgrapht.Graph;

@Data
@NoArgsConstructor
public class VF2PatternGraph {
    private Graph<Vertex, RelationshipEdge> pattern;
    private int diameter;
    private String centerVertexType;
    private Vertex centerVertex;

    public VF2PatternGraph(Graph<Vertex, RelationshipEdge> pattern, String centerVertexType, Vertex centerVertex) {
        this.pattern = pattern;
        this.centerVertexType = centerVertexType;
        this.centerVertex = centerVertex;
    }
}
