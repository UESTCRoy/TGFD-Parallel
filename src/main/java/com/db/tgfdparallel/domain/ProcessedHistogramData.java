package com.db.tgfdparallel.domain;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class ProcessedHistogramData implements Serializable {
    private Map<String, Integer> vertexHistogram;
    private Set<String> activeAttributesSet;
    private Map<String, Set<String>> vertexTypesToActiveAttributesMap;
    private List<FrequencyStatistics> sortedFrequentEdgesHistogram;
    private List<FrequencyStatistics> sortedVertexHistogram;
}
