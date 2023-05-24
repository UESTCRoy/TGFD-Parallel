package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@Data
@NoArgsConstructor
public class LiteralTreeNode {
    private LiteralTreeNode parent;
    private ConstantLiteral literal;
    ArrayList<LiteralTreeNode> children;
    private boolean isPruned;

    public LiteralTreeNode(LiteralTreeNode parent, ConstantLiteral literal) {
        this.parent = parent;
        this.literal = literal;
    }
}
