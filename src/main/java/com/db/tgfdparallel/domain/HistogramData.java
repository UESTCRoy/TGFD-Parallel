package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class HistogramData {
    private Map<String, Integer> vertexTypesHistogram;
    private Map<String, Set<String>> vertexTypesToAttributesMap;
    private Map<String, Set<String>> attrDistributionMap;
    private Map<String, List<Integer>> vertexTypesToInDegreesMap;
    private Map<String, Integer> edgeTypesHistogram;
}
