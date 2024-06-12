package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.jgrapht.Graph;

import java.util.List;

@Data
public class Job {
    private Vertex centerNode;
    private PatternTreeNode patternTreeNode;

    public Job(Vertex centerNode, PatternTreeNode patternTreeNode) {
        this.centerNode = centerNode;
        this.patternTreeNode = patternTreeNode;
    }
}
