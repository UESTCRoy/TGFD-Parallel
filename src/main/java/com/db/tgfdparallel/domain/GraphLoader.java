package com.db.tgfdparallel.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphLoader {
    private VF2DataGraph graph;
    private Set<String> types;
//    private int graphSize;
//    private Set<String> validTypes;
//    private Set<String> validAttributes;
}
