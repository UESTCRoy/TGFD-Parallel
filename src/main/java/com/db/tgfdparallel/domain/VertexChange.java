package com.db.tgfdparallel.domain;

import lombok.Data;

@Data
public class VertexChange extends Change{
    private Vertex vertex;

    public VertexChange(int id, ChangeType type, Vertex vertex) {
        super(id, type);
        this.vertex = vertex;
    }
}
