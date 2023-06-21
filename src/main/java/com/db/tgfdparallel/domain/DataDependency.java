package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DataDependency {
    private List<Literal> x;
    private List<Literal> y;

    public DataDependency() {
        this.x = new ArrayList<>();
        this.y = new ArrayList<>();
    }
}
