package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConstantLiteral {
    private String vertexType;
    private String attrName;
    private String attrValue;
}
