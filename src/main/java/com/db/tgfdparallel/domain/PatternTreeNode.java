package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class PatternTreeNode implements Serializable {
    @EqualsAndHashCode.Include
    private VF2PatternGraph pattern;
    private Double patternSupport;
    private PatternTreeNode parentNode;
    private List<PatternTreeNode> subgraphParents;
    private PatternTreeNode centerVertexParent;
    private String edgeString;
    private boolean isPruned;
    private List<AttributeDependency> minimalDependencies;
    private List<AttributeDependency> minimalConstantDependencies;

    public PatternTreeNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String edgeString) {
        this.pattern = pattern;
        this.parentNode = parentNode;
        this.edgeString = edgeString;
        this.subgraphParents = new ArrayList<>();
        this.minimalDependencies = new ArrayList<>();
        this.minimalConstantDependencies = new ArrayList<>();
    }

    public PatternTreeNode() {
        this.subgraphParents = new ArrayList<>();
        this.minimalDependencies = new ArrayList<>();
        this.minimalConstantDependencies = new ArrayList<>();
    }

    @Override
    public String toString() {
        return "PatternTreeNode{" +
                "pattern=" + pattern.getPattern() +
                ", patternSupport=" + patternSupport +
                '}';
    }
}
