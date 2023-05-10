package com.db.tgfdparallel.domain;

import lombok.Data;

@Data
public class VSpawnPattern {
    private PatternTreeNode oldPattern;
    private PatternTreeNode newPattern;
}
