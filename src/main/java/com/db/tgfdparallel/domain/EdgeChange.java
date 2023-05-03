package com.db.tgfdparallel.domain;

import lombok.Data;

@Data
public class EdgeChange extends Change {
    private String label;
    private String srcURI;
    private String dstURI;

    public EdgeChange(int id, ChangeType type, String srcURI, String dstURI, String label) {
        super(id, type);
        this.srcURI = srcURI;
        this.dstURI = dstURI;
        this.label = label;
    }
}
