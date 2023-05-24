package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.List;

@Data
public class DataDependency {
    private List<Literal> x;
    private List<Literal> y;
}
