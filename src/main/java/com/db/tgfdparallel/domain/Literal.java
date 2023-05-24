package com.db.tgfdparallel.domain;

import lombok.Data;

@Data
public class Literal {
    private enum LiteralType {
        Constant,
        Variable
    }

    private LiteralType type;
}
