package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Attribute {
    private String attrName;
    private String attrValue;
    private boolean isNull;

    public Attribute(String attrName, String attrValue) {
        this.attrName = attrName;
        this.attrValue = attrValue;
    }
}
