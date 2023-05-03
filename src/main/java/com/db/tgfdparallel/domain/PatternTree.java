package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.ArrayList;

@Data
public class PatternTree {
    public ArrayList<ArrayList<PatternTreeNode>> tree;
}
