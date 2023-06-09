package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Attribute implements Serializable {
    @EqualsAndHashCode.Include
    private String attrName;
    private String attrValue;
    private boolean isNull;

    public Attribute(String attrName, String attrValue) {
        this.attrName = attrName;
        this.attrValue = attrValue;
    }

    public Attribute(String attrName) {
        this.attrName = attrName;
    }
}
