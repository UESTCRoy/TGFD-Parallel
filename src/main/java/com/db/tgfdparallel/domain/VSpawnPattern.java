package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class VSpawnPattern {
    private PatternTreeNode oldPattern;
    private PatternTreeNode newPattern;
}
