package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
public class PatternTree {
    private List<List<PatternTreeNode>> tree;

    public PatternTree() {
        this.tree = new ArrayList<>();
        this.tree.add(new ArrayList<>());
    }
}
