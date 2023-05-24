package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VariableLiteral extends Literal {
    private String vertexType;
    private String attrName;
}
