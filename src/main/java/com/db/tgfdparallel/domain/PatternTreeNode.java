package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@Data
@NoArgsConstructor
public class PatternTreeNode {
    private VF2PatternGraph pattern;
    private Double patternSupport;
    private PatternTreeNode parentNode;
    private ArrayList<PatternTreeNode> subgraphParents;
    private PatternTreeNode centerVertexParent;
    private String edgeString;
//    private boolean isPruned;

    public PatternTreeNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String edgeString) {
        this.pattern = pattern;
        this.parentNode = parentNode;
        this.edgeString = edgeString;
    }

}
