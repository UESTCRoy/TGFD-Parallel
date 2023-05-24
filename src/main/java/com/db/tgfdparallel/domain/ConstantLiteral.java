package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class ConstantLiteral extends Literal {
    private String vertexType;
    private String attrName;
    private String attrValue;
}
