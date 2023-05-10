package com.db.tgfdparallel.domain;

import lombok.Data;
import org.jgrapht.Graph;

import java.util.ArrayList;

@Data
public class Job {
    private int ID;
    private int diameter;
    private Vertex centerNode;
    private int fragmentID;
    private ArrayList<RelationshipEdge> edges;
    private Graph<Vertex, RelationshipEdge> subgraph;
    private PatternTreeNode patternTreeNode;

    public Job(int ID, int diameter, Vertex centerNode, int fragmentID, ArrayList<RelationshipEdge> edges, PatternTreeNode patternTreeNode) {
        this.ID = ID;
        this.diameter = diameter;
        this.centerNode = centerNode;
        this.fragmentID = fragmentID;
        this.edges = edges;
        this.patternTreeNode = patternTreeNode;
    }

    public Job(Vertex centerNode, PatternTreeNode patternTreeNode) {
        this.centerNode = centerNode;
        this.patternTreeNode = patternTreeNode;
    }
}
