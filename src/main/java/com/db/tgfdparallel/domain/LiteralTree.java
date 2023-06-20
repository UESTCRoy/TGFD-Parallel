package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class LiteralTree {
    private List<List<LiteralTreeNode>> tree;

    public LiteralTree() {
        this.tree = new ArrayList<>();
    }
}
