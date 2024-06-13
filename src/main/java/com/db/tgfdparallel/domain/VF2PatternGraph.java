package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.jgrapht.Graph;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class VF2PatternGraph implements Serializable {
    private Graph<Vertex, RelationshipEdge> pattern;
//    private int diameter;
    private String centerVertexType;
    private Vertex centerVertex;

    public VF2PatternGraph(Graph<Vertex, RelationshipEdge> pattern, String centerVertexType, Vertex centerVertex) {
        this.pattern = pattern;
        this.centerVertexType = centerVertexType;
        this.centerVertex = centerVertex;
    }

    @Override
    public String toString() {
        return "VF2PatternGraph{" +
                "centerVertex=" + centerVertex +
                ", pattern=" + pattern +
                '}';
    }
}
