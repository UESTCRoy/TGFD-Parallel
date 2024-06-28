package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode
public class DataDependency implements Serializable {
    private List<Literal> x;
    private List<Literal> y;

    public DataDependency() {
        this.x = new ArrayList<>();
        this.y = new ArrayList<>();
    }

    @Override
    public String toString() {
        return "DataDependency{" +
                "x=" + x +
                ", y=" + y +
                '}';
    }
}
