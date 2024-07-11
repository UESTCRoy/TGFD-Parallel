package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.utils.DeepCopyUtil;
import org.jgrapht.Graph;
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

    public ProcessedHistogramData computeHistogram(VF2DataGraph dataGraph, List<List<Change>> changesData) {
        HistogramData data = new HistogramData();

        logger.info("--------The snapshot(1)--------");
        readGraphInfo(dataGraph.getGraph(), data);

        // We use deep copy here
        for (int i = 0; i < config.getTimestamp() - 1; i++) {
            logger.info("--------The snapshot(" + (i + 2) + ")--------");
            VF2DataGraph nextDataGraph = DeepCopyUtil.deepCopy(dataGraph);
            graphService.updateEntireGraph(nextDataGraph, changesData.get(i));
            readGraphInfo(nextDataGraph.getGraph(), data);
        }

        return performingRecordKeeping(data);
    }

    public ProcessedHistogramData computeHistogramAllSnapshot(List<Graph<Vertex, RelationshipEdge>> allSnapshotGraph) {
        HistogramData data = new HistogramData();
        for (Graph<Vertex, RelationshipEdge> graph : allSnapshotGraph) {
            readGraphInfo(graph, data);
        }
        return performingRecordKeeping(data);
    }

    public void readGraphInfo(Graph<Vertex, RelationshipEdge> graph, HistogramData data) {
        int edgeCount = graph.edgeSet().size();
        logger.info("The number of edges " + edgeCount);

        Map<String, Integer> vertexTypesHistogram = new HashMap<>();
        Map<String, Set<String>> vertexTypesToAttributesMap = new HashMap<>();
        Map<String, Set<String>> attrDistributionMap = new HashMap<>();
        Map<String, Integer> edgeTypesHistogram = new HashMap<>();

        // Load the graph's vertices and attributes
        for (Vertex v : graph.vertexSet()) {
            String vertexType = v.getType();
            vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
            Set<String> attributeNames = v.getAttributes().stream()
                    .map(Attribute::getAttrName)
                    .filter(name -> !name.equals("uri"))
                    .peek(attrName -> attrDistributionMap.computeIfAbsent(attrName, k -> new HashSet<>()).add(vertexType))
                    .collect(Collectors.toSet());
            vertexTypesToAttributesMap.computeIfAbsent(vertexType, k -> new HashSet<>()).addAll(attributeNames);
        }

        // Load the graph's edges
        for (RelationshipEdge edge : graph.edgeSet()) {
            String predicateName = edge.getLabel();
            String sourceVertexType = edge.getSource().getType();
            String objectVertexType = edge.getTarget().getType();

            String uniqueEdge = sourceVertexType + " " + predicateName + " " + objectVertexType;
            edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
        }

        data.setVertexTypesHistogram(vertexTypesHistogram);
        data.setVertexTypesToAttributesMap(vertexTypesToAttributesMap);
        data.setAttrDistributionMap(attrDistributionMap);
        data.setEdgeTypesHistogram(edgeTypesHistogram);
    }

    //TODO: Sort the histogram data, and compare with the original data
    public ProcessedHistogramData performingRecordKeeping(HistogramData data) {
        Set<String> activeAttributesSet = setActiveAttributeSet(data.getAttrDistributionMap());
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = vertexTypesToActiveAttributesMap(data.getVertexTypesToAttributesMap(), activeAttributesSet);
        List<FrequencyStatistics> sortedFrequentEdgesHistogram = setSortedFrequentEdgeHistogram(data.getEdgeTypesHistogram());
        List<FrequencyStatistics> sortedVertexHistogram = setSortedFrequentVerticesUsingFrequentEdges(sortedFrequentEdgesHistogram, data.getVertexTypesHistogram());

        Map<String, Set<String>> seenMap = new HashMap<>();
        List<String> frequentEdgesData = sortedFrequentEdgesHistogram.stream()
                .map(FrequencyStatistics::getType)
                .filter(edge -> {
                    String[] parts = edge.split(" ");
                    String sourceVertexType = parts[0];
                    String targetVertexType = parts[2];
                    String label = edge.split(" ")[1];

                    if (seenMap.containsKey(sourceVertexType)) {
                        if (seenMap.get(sourceVertexType).contains(targetVertexType)) {
                            logger.info("The edge is already seen, skip this edge: {}", edge);
                            return false;
                        }
                    }

                    // Check if source and target vertex types are the same
                    if (sourceVertexType.equals(targetVertexType)) {
                        logger.info("The sourceVertexType and targetVertexType are the same, skip this edge: {}", edge);
                        return false;
                    }

                    // Check if both source and target vertex types have active attributes
                    if (!vertexTypesToActiveAttributesMap.containsKey(sourceVertexType) || !vertexTypesToActiveAttributesMap.containsKey(targetVertexType)) {
                        logger.info("The sourceVertexType or targetVertexType doesn't have active attributes, skip this edge: {}", edge);
                        return false;
                    }

                    seenMap.computeIfAbsent(sourceVertexType, k -> new HashSet<>()).add(targetVertexType);

                    return true;
                })
                .collect(Collectors.toList());

        ProcessedHistogramData histogramData = new ProcessedHistogramData();
        histogramData.setSortedFrequentEdgesHistogram(frequentEdgesData);
        histogramData.setSortedVertexHistogram(sortedVertexHistogram);
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

            // Filters non-active attributes
            Set<String> activeAttributes = attrNameSet.stream()
                    .filter(activeAttributesSet::contains)
                    .collect(Collectors.toSet());

            // Only adds them to the map if there is at least one active attribute
            if (!activeAttributes.isEmpty()) {
                vertexTypesToActiveAttributesMap.put(vertexType, activeAttributes);
            }
        }

        return vertexTypesToActiveAttributesMap;
    }

    /**
     * Sets the sorted frequent edge histogram for the graph.
     * Return vertexHistogram & sortedFrequentEdgesHistogram
     */
    public List<FrequencyStatistics> setSortedFrequentEdgeHistogram(Map<String, Integer> edgeTypesHistogram) {
        List<FrequencyStatistics> finalEdgesHist = edgeTypesHistogram.entrySet().stream()
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
    public List<FrequencyStatistics> setSortedFrequentVerticesUsingFrequentEdges(List<FrequencyStatistics> sortedFrequentEdgesHistogram,
                                                                                 Map<String, Integer> vertexTypesHistogram) {
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

        return sortedVertexTypesHistogram;
    }


}
