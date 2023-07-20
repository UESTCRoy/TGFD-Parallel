package com.db.tgfdparallel.domain;

import java.util.ArrayList;
import java.util.List;

public class MappingTree {
    private List<List<MappingTreeNode>> tree;

    public MappingTree() {
        setTree(new ArrayList<>());
    }

    public void addLevel() {
        getTree().add(new ArrayList<>());
    }

    public MappingTreeNode createNodeAtLevel(int level, Vertex dataVertex, String patternVertexType, MappingTreeNode parentNode) {
        MappingTreeNode node = new MappingTreeNode(dataVertex, patternVertexType, parentNode);
        getTree().get(level).add(node);
        return node;
    }

    public void createNodeAtLevel(int level, MappingTreeNode mappingTreeNode) {
        getTree().get(level).add(mappingTreeNode);
    }

    public List<MappingTreeNode> getLevel(int i) {
        return getTree().get(i);
    }

    public List<List<MappingTreeNode>> getTree() {
        return tree;
    }

    public void setTree(List<List<MappingTreeNode>> tree) {
        this.tree = tree;
    }
}
