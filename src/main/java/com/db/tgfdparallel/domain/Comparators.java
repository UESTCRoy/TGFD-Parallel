package com.db.tgfdparallel.domain;

import java.util.Comparator;

public class Comparators {
    public static final Comparator<RelationshipEdge> edgeComparator = (o1, o2) -> {
        if (o1.getLabel().equals(o2.getLabel())) return 0;
        if (o1.getLabel().equals("*") || o2.getLabel().equals("*")) return 0;
        return 1;
    };

    public static final Comparator<Vertex> vertexComparator = (v1, v2) -> {
        if (v1 == null || v2 == null) return 1; // handle null values to avoid NullPointerException
        return v1.getType().equals(v2.getType()) ? 0 : 1; // Compare only the types of the vertices
    };

}
