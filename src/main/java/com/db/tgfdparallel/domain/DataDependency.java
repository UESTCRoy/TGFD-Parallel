package com.db.tgfdparallel.domain;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class DataDependency implements Serializable {
    private List<Literal> x;
    private List<Literal> y;

    public DataDependency() {
        this.x = new ArrayList<>();
        this.y = new ArrayList<>();
    }

    public DataDependency(List<Literal> x, List<Literal> y) {
        this.x = x;
        this.y = y;
    }
}
