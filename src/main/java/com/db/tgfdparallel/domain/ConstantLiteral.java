package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class ConstantLiteral extends Literal implements Comparable<ConstantLiteral>, Serializable {
    private String vertexType;
    private String attrName;
    private String attrValue;

    @Override
    public int compareTo(ConstantLiteral other) {
        int vertexTypeComparison = this.vertexType.compareTo(other.vertexType);
        if (vertexTypeComparison != 0) {
            return vertexTypeComparison;
        }

        int attrNameComparison = this.attrName.compareTo(other.attrName);
        if (attrNameComparison != 0) {
            return attrNameComparison;
        }

        return this.attrValue.compareTo(other.attrValue);
    }
}
