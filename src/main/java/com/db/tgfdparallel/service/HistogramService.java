package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class HistogramService {
    private static final Logger logger = LoggerFactory.getLogger(HistogramService.class);
    private final AppConfig config;
    private final GraphService graphService;

    @Autowired
    public HistogramService(AppConfig config, GraphService graphService) {
        this.config = config;
        this.graphService = graphService;
    }

    public ProcessedHistogramData computeHistogram(GraphLoader firstLoader, List<List<Change>> changesData) {
        HistogramData data = new HistogramData();

        logger.info("--------The snapshot(1)--------");
        readGraphInfo(firstLoader, data);

        for (int i = 0; i < config.getTimestamp() - 1; i++) {
            logger.info("--------The snapshot(" + (i + 2) + ")--------");
            graphService.updateEntireGraph(firstLoader.getGraph(), changesData.get(i));
            readGraphInfo(firstLoader, data);
        }

        // TODO: finish the following method
        ProcessedHistogramData histogramData = performingRecordKeeping(data);

        return histogramData;
    }

    public void readGraphInfo(GraphLoader loader, HistogramData data) {
        int edgeCount = loader.getGraph().getGraph().edgeSet().size();
        logger.info("The number of edges " + edgeCount);
        logger.info("The number of nodes " + loader.getGraphSize());

        Map<String, Integer> vertexTypesHistogram = new HashMap<>();
        Map<String, Set<String>> vertexTypesToAttributesMap = new HashMap<>();
        Map<String, Set<String>> attrDistributionMap = new HashMap<>();
        Map<String, List<Integer>> vertexTypesToInDegreesMap = new HashMap<>();
        Map<String, Integer> edgeTypesHistogram = new HashMap<>();

        // Load the graph's vertices and attributes
        for (Vertex v : loader.getGraph().getGraph().vertexSet()) {
            String vertexType = v.getTypes();
            vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
            Set<String> attributeNames = v.getAttributes().stream()
                    .map(Attribute::getAttrName)
                    .filter(name -> !name.equals("uri"))
                    .peek(attrName -> attrDistributionMap.computeIfAbsent(attrName, k -> new HashSet<>()).add(vertexType))
                    .collect(Collectors.toSet());
            vertexTypesToAttributesMap.computeIfAbsent(vertexType, k -> new HashSet<>()).addAll(attributeNames);
            vertexTypesToInDegreesMap.computeIfAbsent(vertexType, k -> new ArrayList<>()).add(loader.getGraph().getGraph().incomingEdgesOf(v).size());
        }

        for (List<Integer> inDegrees : vertexTypesToInDegreesMap.values()) {
            inDegrees.sort(Comparator.naturalOrder());
        }

        // Load the graph's edges
        for (RelationshipEdge edge : loader.getGraph().getGraph().edgeSet()) {
            Vertex sourceVertex = edge.getSource();
            String predicateName = edge.getLabel();
            Vertex objectVertex = edge.getTarget();

            String uniqueEdge = sourceVertex.getTypes() + " " + predicateName + " " + objectVertex.getTypes();
            edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
        }

        data.setVertexTypesHistogram(vertexTypesHistogram);
        data.setVertexTypesToAttributesMap(vertexTypesToAttributesMap);
        data.setAttrDistributionMap(attrDistributionMap);
        data.setVertexTypesToInDegreesMap(vertexTypesToInDegreesMap);
        data.setEdgeTypesHistogram(edgeTypesHistogram);
    }

    //TODO: Sort the histogram data, and compare with the original data

    public ProcessedHistogramData performingRecordKeeping(HistogramData data) {
        Map<String, Integer> vertexHistogram = new HashMap<>();
        Set<String> activeAttributesSet = setActiveAttributeSet(data.getAttrDistributionMap());
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = vertexTypesToActiveAttributesMap(data.getVertexTypesToAttributesMap(), activeAttributesSet);
        List<FrequencyStatistics> sortedFrequentEdgesHistogram = setSortedFrequentEdgeHistogram(data.getEdgeTypesHistogram(), vertexTypesToActiveAttributesMap, data.getVertexTypesHistogram(), vertexHistogram);
        List<FrequencyStatistics> sortedVertexHistogram = setSortedFrequentVerticesUsingFrequentEdges(sortedFrequentEdgesHistogram, data.getVertexTypesHistogram(), vertexHistogram);

        ProcessedHistogramData histogramData = new ProcessedHistogramData();
        histogramData.setVertexHistogram(vertexHistogram);
        histogramData.setSortedFrequentEdgesHistogram(sortedFrequentEdgesHistogram);
        histogramData.setSortedVertexHistogram(sortedVertexHistogram);
        histogramData.setActiveAttributesSet(activeAttributesSet);
        histogramData.setVertexTypesToActiveAttributesMap(vertexTypesToActiveAttributesMap);

        return histogramData;
    }

    /**
     * Sets the active attribute set to the most widely distributed attributes in the graph.
     * Return activeAttributes
     */
    public Set<String> setActiveAttributeSet(Map<String, Set<String>> attrDistributionMap) {
        List<Map.Entry<String, Set<String>>> sortedAttrDistribution = new ArrayList<>(attrDistributionMap.entrySet());
        sortedAttrDistribution.sort((entry1, entry2) -> entry2.getValue().size() - entry1.getValue().size());

        int gamma = config.getActiveAttributes();

        int exclusiveIndex = Math.min(gamma, sortedAttrDistribution.size());
        Set<String> activeAttributes = new HashSet<>();

        for (int i = 0; i < exclusiveIndex; i++) {
            Map.Entry<String, Set<String>> entry = sortedAttrDistribution.get(i);
            activeAttributes.add(entry.getKey());
        }

        return activeAttributes;
    }

    /**
     * Creates a mapping of vertex types to active attributes in the graph.
     * Return vertexTypesToActiveAttributesMap
     */
    public Map<String, Set<String>> vertexTypesToActiveAttributesMap(Map<String, Set<String>> vertexTypesToAttributesMap, Set<String> activeAttributesSet) {
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = new HashMap<>();

        for (String vertexType : vertexTypesToAttributesMap.keySet()) {
            Set<String> attrNameSet = vertexTypesToAttributesMap.get(vertexType);

            // Filters non-active attributes and adds them to the map
            vertexTypesToActiveAttributesMap.put(
                    vertexType,
                    attrNameSet.stream()
                            .filter(activeAttributesSet::contains)
                            .collect(Collectors.toSet())
            );
        }

        return vertexTypesToActiveAttributesMap;
    }

    /**
     * Sets the sorted frequent edge histogram for the graph.
     * Return vertexHistogram & sortedFrequentEdgesHistogram
     */
    public List<FrequencyStatistics> setSortedFrequentEdgeHistogram(Map<String, Integer> edgeTypesHistogram,
                                                                           Map<String, Set<String>> vertexTypesToActiveAttributesMap,
                                                                           Map<String, Integer> vertexTypesHistogram,
                                                                           Map<String, Integer> vertexHistogram) {
        List<FrequencyStatistics> finalEdgesHist = edgeTypesHistogram.entrySet().stream()
                .filter(entry -> {
                    String[] edgeString = entry.getKey().split(" ");
                    String sourceType = edgeString[0];
                    String targetType = edgeString[2];
                    vertexHistogram.put(sourceType, vertexTypesHistogram.get(sourceType));
                    vertexHistogram.put(targetType, vertexTypesHistogram.get(targetType));

                    return vertexTypesToActiveAttributesMap.get(sourceType).size() > 0
                            && vertexTypesToActiveAttributesMap.get(targetType).size() > 0;
                })
                .sorted((o1, o2) -> o2.getValue() - o1.getValue())
                .map(entry -> new FrequencyStatistics(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        int exclusiveIndex = Math.min(finalEdgesHist.size(), config.getFrequentEdges());
        return new ArrayList<>(finalEdgesHist.subList(0, exclusiveIndex));
    }

    /**
     * Sets the sorted frequent vertices using frequent edges for the graph.
     * Return sortedVertexTypesHistogram
     */
    public List<FrequencyStatistics> setSortedFrequentVerticesUsingFrequentEdges(List<FrequencyStatistics> sortedFrequentEdgesHistogram, Map<String, Integer> vertexTypesHistogram, Map<String, Integer> vertexHistogram) {
        Set<String> relevantFrequentVertexTypes = sortedFrequentEdgesHistogram.stream()
                .flatMap(entry -> {
                    String[] edgeString = entry.getType().split(" ");
                    return Stream.of(edgeString[0], edgeString[2]);
                })
                .collect(Collectors.toSet());

        List<FrequencyStatistics> sortedVertexTypesHistogram = vertexTypesHistogram.entrySet().stream()
                .filter(entry -> relevantFrequentVertexTypes.contains(entry.getKey()))
                .sorted((o1, o2) -> o2.getValue() - o1.getValue())
                .map(entry -> new FrequencyStatistics(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        sortedVertexTypesHistogram.forEach(entry -> vertexHistogram.put(entry.getType(), entry.getFrequency()));
        return sortedVertexTypesHistogram;
    }

}
