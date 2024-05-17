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
        this.src = edge.getSource().getUri() + "-" + edge.getSource().getType();
        this.dst = edge.getTarget().getUri() + "-" + edge.getTarget().getType();
        this.label = edge.getLabel();
    }
}
