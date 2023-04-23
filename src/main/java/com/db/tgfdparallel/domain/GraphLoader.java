package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphLoader {
    private int graphSize;
    private VF2DataGraph graph;
    private Set<String> validTypes;
    private Set<String> validAttributes;
    private Set<String> types;
}
