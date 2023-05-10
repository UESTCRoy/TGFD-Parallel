package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.List;

@Data
public class PatternTree {
    public List<List<PatternTreeNode>> tree;
}
