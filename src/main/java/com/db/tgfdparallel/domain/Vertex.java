package com.db.tgfdparallel.domain;


import lombok.Data;

import java.util.Set;

@Data
public class Vertex {
    private String uri;
    private String types;
    private Set<Attribute> attributes;
    private boolean isMarked;
    private Set<Integer> jobletID;

    public Vertex(String uri, String types) {
        this.uri = uri;
        this.types = types;
    }
}
