package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SimpleEdge {
    private String src;
    private String dst;
    private String label;

    public SimpleEdge(RelationshipEdge edge) {
        this.src = edge.getSource().getUri();
        this.dst = edge.getTarget().getUri();
        this.label = edge.getLabel();
    }
}
