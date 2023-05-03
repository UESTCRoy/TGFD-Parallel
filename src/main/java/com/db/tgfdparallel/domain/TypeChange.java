package com.db.tgfdparallel.domain;

import lombok.Data;

@Data
public class TypeChange extends Change{
    private String uri;
    private Vertex previousVertex;
    private Vertex newVertex;

    public TypeChange(int id, ChangeType type, String uri, Vertex previousVertex, Vertex newVertex) {
        super(id, type);
        this.uri = uri;
        this.previousVertex = previousVertex;
        this.newVertex = newVertex;
    }
}
