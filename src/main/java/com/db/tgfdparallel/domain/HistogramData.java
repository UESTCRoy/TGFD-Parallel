package com.db.tgfdparallel.domain;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class HistogramData {
    private Map<String, Integer> vertexTypesHistogram = new HashMap<>();
    private Map<String, Set<String>> vertexTypesToAttributesMap = new HashMap<>();
    private Map<String, Set<String>> attrDistributionMap = new HashMap<>();
    private Map<String, List<Integer>> vertexTypesToInDegreesMap = new HashMap<>();
    private Map<String, Integer> edgeTypesHistogram = new HashMap<>();
}
