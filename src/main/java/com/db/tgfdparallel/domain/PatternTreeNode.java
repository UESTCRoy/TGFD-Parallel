package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.ArrayList;

@Data
public class PatternTreeNode {
    private VF2PatternGraph pattern;
    private Double patternSupport;
    private PatternTreeNode parentNode;
    private ArrayList<PatternTreeNode> subgraphParents;
    private PatternTreeNode centerVertexParent;
//    private String edgeString;
//    private boolean isPruned;
}
