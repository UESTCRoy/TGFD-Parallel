package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class VariableLiteral extends Literal implements Comparable<VariableLiteral>, Serializable {
    private String vertexType;
    private String attrName;

    @Override
    public int compareTo(VariableLiteral other) {
        int vertexTypeComparison = this.vertexType.compareTo(other.vertexType);
        if (vertexTypeComparison != 0) {
            return vertexTypeComparison;
        } else {
            return this.attrName.compareTo(other.attrName);
        }
    }
}
