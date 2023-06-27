package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LiteralTreeNode {
    private LiteralTreeNode parent;
    private ConstantLiteral literal;
    private boolean isPruned;

    public LiteralTreeNode(LiteralTreeNode parent, ConstantLiteral literal) {
        this.parent = parent;
        this.literal = literal;
    }
}
