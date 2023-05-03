package com.db.tgfdparallel.domain;

import lombok.Data;

@Data
public class AttributeChange extends Change {
    private String vertexURI;
    private Attribute attribute;

    public AttributeChange(int id, ChangeType type, String vertexURI, Attribute attribute) {
        super(id, type);
        this.vertexURI = vertexURI;
        this.attribute = attribute;
    }
}
