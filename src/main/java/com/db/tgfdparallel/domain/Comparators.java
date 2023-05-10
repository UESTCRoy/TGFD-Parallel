package com.db.tgfdparallel.domain;

import java.util.Comparator;

public class Comparators {
    public static final Comparator<RelationshipEdge> edgeComparator = (o1, o2) -> o1.getLabel().equals("*") || o2.getLabel().equals("*") || o1.getLabel().equals(o2.getLabel()) ? 0 : 1;

    public static final Comparator<Vertex> vertexComparator = (v1, v2) -> v1.isMapped(v2) ? 0 : 1;
}
