package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.utils.MathUtil;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class FastMatchService {
    private static final Logger logger = LoggerFactory.getLogger(FastMatchService.class);
    private final AppConfig config;

    @Autowired
    public FastMatchService(AppConfig config) {
        this.config = config;
    }

    // Single Edge Pattern
    public void findAllMatchesOfSingleEdgePatternInSnapshotUsingCenterVertex(Graph<Vertex, RelationshipEdge> patternGraph, String centerVertexType,
                                                                             Graph<Vertex, RelationshipEdge> realGraph, Vertex centerVertex, int timestamp,
                                                                             Set<Set<ConstantLiteral>> matchesAroundCenterVertex, Map<String, List<Integer>> ptnEntityURIs,
                                                                             Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        RelationshipEdge patternEdge = patternGraph.edgeSet().stream().findFirst().orElse(null);
        String edgeLabel = patternEdge.getLabel();
        String sourceType = patternEdge.getSource().getType();
        String targetType = patternEdge.getTarget().getType();

        Stream<RelationshipEdge> edgeStream = getRelevantEdges(centerVertex, centerVertexType, sourceType, targetType, edgeLabel, realGraph);
        Set<RelationshipEdge> edgeSet = edgeStream.collect(Collectors.toSet());

        extractMatches(centerVertex.getUri(), patternGraph, edgeSet, matchesAroundCenterVertex, timestamp, ptnEntityURIs, vertexTypesToActiveAttributesMap);
    }

    private Stream<RelationshipEdge> getRelevantEdges(Vertex centerVertex, String centerVertexType, String sourceType, String targetType, String edgeLabel, Graph<Vertex, RelationshipEdge> realGraph) {
        if (centerVertexType.equals(sourceType)) {
            return realGraph.outgoingEdgesOf(centerVertex).stream().filter(e -> edgeLabel.equals(e.getLabel()) && e.getTarget().getType().equals(targetType));
        } else {
            return realGraph.incomingEdgesOf(centerVertex).stream().filter(e -> edgeLabel.equals(e.getLabel()) && e.getSource().getType().equals(sourceType));
        }
    }

    private void extractMatches(String entityURI, Graph<Vertex, RelationshipEdge> patternGraph, Set<RelationshipEdge> edgeSet, Set<Set<ConstantLiteral>> matchesAroundCenterVertex,
                                int timestamp, Map<String, List<Integer>> entityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Set<String> vertexTypes = patternGraph.vertexSet().stream().map(Vertex::getType).collect(Collectors.toSet());

        for (RelationshipEdge edge : edgeSet) {
            Set<ConstantLiteral> literalsInMatch = new HashSet<>();
            extractMatch(edge.getSource(), edge.getTarget(), literalsInMatch, vertexTypesToActiveAttributesMap);

            if (literalsInMatch.size() < patternGraph.vertexSet().size()) {
                continue;
            }

            Set<String> matchedVertexTypes = literalsInMatch.stream().map(ConstantLiteral::getVertexType).collect(Collectors.toSet());
            if (!vertexTypes.containsAll(matchedVertexTypes)) {
                continue;
            }


            if (entityURI != null) {
                List<Integer> counts = entityURIs.computeIfAbsent(entityURI, k -> new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0)));
                counts.set(timestamp, counts.get(timestamp) + 1);
            }
            matchesAroundCenterVertex.add(literalsInMatch);
        }
    }

    private void extractMatch(Vertex currentSourceVertex, Vertex currentTargetVertex, Set<ConstantLiteral> match, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        List<Vertex> vertices = Arrays.asList(currentSourceVertex, currentTargetVertex);

        for (Vertex currentMatchedVertex : vertices) {
            if (currentMatchedVertex != null) {
                extractAttributes(match, currentMatchedVertex, vertexTypesToActiveAttributesMap);
            }
        }
    }

    private void extractAttributes(Set<ConstantLiteral> match, Vertex currentMatchedVertex, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String matchedVertexType = currentMatchedVertex.getType();

        Map<String, Attribute> vertexAllAttributesMap = currentMatchedVertex.getAttributes().stream()
                .collect(Collectors.toMap(Attribute::getAttrName, Function.identity()));
        Set<String> activeAttributeNames = vertexTypesToActiveAttributesMap.getOrDefault(matchedVertexType, new HashSet<>());

        for (String attrName : activeAttributeNames) {
            Attribute matchedAttribute = vertexAllAttributesMap.getOrDefault(attrName, null);
            if (matchedAttribute == null) continue;

            String matchedAttrValue = matchedAttribute.getAttrValue();
            match.add(new ConstantLiteral(matchedVertexType, attrName, matchedAttrValue));
        }
    }

    // Double Edge Pattern
    public void findAllMatchesOfK2PatternInSnapshotUsingCenterVertex(VF2PatternGraph pattern, String centerVertexType,
                                                                     Graph<Vertex, RelationshipEdge> realGraph, Vertex centerVertex, int timestamp,
                                                                     Set<Set<ConstantLiteral>> matchesAroundCenterVertex, Map<String, List<Integer>> ptnEntityURIs,
                                                                     Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Graph<Vertex, RelationshipEdge> patternGraph = pattern.getPattern();
        Set<Vertex> patternVertexSet = patternGraph.vertexSet();

        Map<String, Set<Vertex>> patternVertexToDataVerticesMap = getPatternVertexToDataVerticesMap(pattern, realGraph, centerVertex);
        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(centerVertex, vertexTypesToActiveAttributesMap.get(centerVertexType));

        List<Map.Entry<String, Set<Vertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());
        Map.Entry<String, Set<Vertex>> nonCenterVertexEntry1 = vertexSets.get(0);
        Map.Entry<String, Set<Vertex>> nonCenterVertexEntry2 = vertexSets.get(1);

        for (Vertex nonCenterDataVertex1 : nonCenterVertexEntry1.getValue()) {
            Map<String, Attribute> nonCenterDataVertex1Attributes = getAttributesMap(nonCenterDataVertex1);

            for (Vertex nonCenterDataVertex2 : nonCenterVertexEntry2.getValue()) {
                HashSet<ConstantLiteral> match = new HashSet<>(centerDataVertexLiterals);
                Map<String, Attribute> nonCenterDataVertex2Attributes = getAttributesMap(nonCenterDataVertex2);
                addAttributesToMatch(nonCenterDataVertex1.getType(), nonCenterDataVertex1Attributes, vertexTypesToActiveAttributesMap.get(nonCenterVertexEntry1.getKey()), match);
                addAttributesToMatch(nonCenterDataVertex2.getType(), nonCenterDataVertex2Attributes, vertexTypesToActiveAttributesMap.get(nonCenterVertexEntry2.getKey()), match);

                if (match.size() > patternVertexSet.size()) {
                    matchesAroundCenterVertex.add(match);
                }
            }
        }

        updateEntityURIs(centerVertex, matchesAroundCenterVertex, ptnEntityURIs, timestamp);
    }

    private Map<String, Set<Vertex>> getPatternVertexToDataVerticesMap(VF2PatternGraph pattern, Graph<Vertex, RelationshipEdge> realGraph, Vertex centerVertex) {
        Map<String, Set<Vertex>> patternVertexToDataVerticesMap = new HashMap<>();
        Graph<Vertex, RelationshipEdge> patternGraph = pattern.getPattern();
        Vertex centerPatternVertex = pattern.getCenterVertex();

        for (RelationshipEdge patternEdge : patternGraph.incomingEdgesOf(centerPatternVertex)) {
            Vertex nonCenterPatternVertex = patternEdge.getSource();
            patternVertexToDataVerticesMap.put(nonCenterPatternVertex.getType(), new HashSet<>());
            for (RelationshipEdge dataEdge : realGraph.incomingEdgesOf(centerVertex)) {
                if (dataEdge.getLabel().equals(patternEdge.getLabel())
                        && dataEdge.getSource().getType().equals(nonCenterPatternVertex.getType())) {
                    patternVertexToDataVerticesMap.get(nonCenterPatternVertex.getType()).add(dataEdge.getSource());
                }
            }
        }
        for (RelationshipEdge patternEdge : patternGraph.outgoingEdgesOf(centerPatternVertex)) {
            Vertex nonCenterPatternVertex = patternEdge.getTarget();
            patternVertexToDataVerticesMap.put(nonCenterPatternVertex.getType(), new HashSet<>());
            for (RelationshipEdge dataEdge : realGraph.outgoingEdgesOf(centerVertex)) {
                if (dataEdge.getLabel().equals(patternEdge.getLabel())
                        && dataEdge.getTarget().getType().equals(nonCenterPatternVertex.getType())) {
                    patternVertexToDataVerticesMap.get(nonCenterPatternVertex.getType()).add(dataEdge.getTarget());
                }
            }
        }
        return patternVertexToDataVerticesMap;
    }

    private Set<ConstantLiteral> getCenterDataVertexLiterals(Vertex centerVertex, Set<String> activeAttributeNames) {
        Set<ConstantLiteral> centerDataVertexLiterals = new HashSet<>();
        String centerVertexType = centerVertex.getType();
        Map<String, Attribute> vertexAllAttributesMap = centerVertex.getAttributes().stream()
                .collect(Collectors.toMap(Attribute::getAttrName, Function.identity()));

        for (String attrName : activeAttributeNames) {
            Attribute matchedAttribute = vertexAllAttributesMap.getOrDefault(attrName, null);
            if (matchedAttribute == null) continue;
            String matchedAttrValue = matchedAttribute.getAttrValue();
            centerDataVertexLiterals.add(new ConstantLiteral(centerVertexType, attrName, matchedAttrValue));
        }
        return centerDataVertexLiterals;
    }

    private Map<String, Attribute> getAttributesMap(Vertex vertex) {
        return vertex.getAttributes().stream().collect(Collectors.toMap(Attribute::getAttrName, Function.identity()));
    }

    private void addAttributesToMatch(String vertexType, Map<String, Attribute> attributes, Set<String> activeAttributes, Set<ConstantLiteral> matchSet) {
        for (String attrName : activeAttributes) {
            Attribute attribute = attributes.get(attrName);
            if (attribute != null) {
                matchSet.add(new ConstantLiteral(vertexType, attrName, attribute.getAttrValue()));
            }
        }
    }

    private void updateEntityURIs(Vertex vertex, Set<Set<ConstantLiteral>> matches, Map<String, List<Integer>> entityURIs, int timestamp) {
        if (!matches.isEmpty()) {
            String entityURI = vertex.getUri();
            List<Integer> counts = entityURIs.computeIfAbsent(entityURI, k -> new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0)));
            counts.set(timestamp, counts.get(timestamp) + 1);
        }
    }

    // Star
    public void findAllMatchesOfStarPatternInSnapshotUsingCenterVertex(VF2PatternGraph pattern, String centerVertexType,
                                                                       Graph<Vertex, RelationshipEdge> realGraph, Vertex centerVertex, int timestamp,
                                                                       Set<Set<ConstantLiteral>> matchesAroundCenterVertex, Map<String, List<Integer>> ptnEntityURIs,
                                                                       Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Map<String, Set<Vertex>> patternVertexToDataVerticesMap = getPatternVertexToDataVerticesMap(pattern, realGraph, centerVertex);
        List<Set<Vertex>> listOfDataVertexSets = new ArrayList<>(patternVertexToDataVerticesMap.values());

        Set<List<Vertex>> allCombinations = MathUtil.cartesianProduct(listOfDataVertexSets);
        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(centerVertex, vertexTypesToActiveAttributesMap.get(centerVertexType));

        for (List<Vertex> combination : allCombinations) {
            Set<ConstantLiteral> match = new HashSet<>(centerDataVertexLiterals);

            for (int i = 0; i < combination.size(); i++) {
                Vertex vertex = combination.get(i);
                Map<String, Attribute> attributesMap = getAttributesMap(vertex);
                addAttributesToMatch(vertex.getType(), attributesMap, vertexTypesToActiveAttributesMap.get(vertex.getType()), match);
            }

            if (match.size() > pattern.getPattern().vertexSet().size()) { // Implement this method to check the validity of a match
                matchesAroundCenterVertex.add(match);
                updateEntityURIs(centerVertex, matchesAroundCenterVertex, ptnEntityURIs, timestamp);
            }
        }
    }

    // Line and Cyclic
    public void findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(VF2PatternGraph pattern, Graph<Vertex, RelationshipEdge> realGraph, Vertex centerVertex, int timestamp,
                                                                        Set<Set<ConstantLiteral>> matchesAroundCenterVertex, Map<String, List<Integer>> ptnEntityURIs,
                                                                        Map<String, Set<String>> vertexTypesToActiveAttributesMap, boolean isCyclic) {
        List<List<Vertex>> currentMatches = new ArrayList<>();
        currentMatches.add(Arrays.asList(centerVertex));

        List<RelationshipEdge> edgesInPattern = new ArrayList<>(pattern.getPattern().edgeSet());

        for (int i = 0; i < edgesInPattern.size(); i++) {
            RelationshipEdge edge = edgesInPattern.get(i);
            List<List<Vertex>> newMatches = new ArrayList<>();

            for (List<Vertex> connectedVertices : currentMatches) {
                Vertex lastVertex = connectedVertices.get(connectedVertices.size() - 1);
                Set<Vertex> nextVertices = findMatchingVertices(realGraph, lastVertex, edge, centerVertex, isCyclic);

                for (Vertex vertex : nextVertices) {
                    List<Vertex> newConnectedVertices = new ArrayList<>(connectedVertices);
                    newConnectedVertices.add(vertex);
                    newMatches.add(newConnectedVertices);
                }
            }

            currentMatches = newMatches;
        }

        extractValidMatches(pattern.getPattern(), currentMatches, centerVertex, timestamp, matchesAroundCenterVertex, ptnEntityURIs, vertexTypesToActiveAttributesMap);
    }

    private Set<Vertex> findMatchingVertices(Graph<Vertex, RelationshipEdge> realGraph, Vertex currentVertex, RelationshipEdge patternEdge, Vertex startVertex, boolean isCyclic) {
        Set<Vertex> matchingVertices = new HashSet<>();
        // Assuming getConnectedEdges returns relevant edges for a vertex that could form part of the pattern
        Set<RelationshipEdge> candidateEdges = realGraph.edgesOf(currentVertex);

        for (RelationshipEdge candidateEdge : candidateEdges) {
            if (edgeMatchesPattern(candidateEdge, patternEdge)) {
                Vertex nextVertex = candidateEdge.getTarget().equals(currentVertex) ? candidateEdge.getSource() : candidateEdge.getTarget();
                if (!isCyclic || !nextVertex.equals(startVertex)) { // Check for cyclic conditions
                    matchingVertices.add(nextVertex);
                }
            }
        }
        return matchingVertices;
    }

    private boolean edgeMatchesPattern(RelationshipEdge dataEdge, RelationshipEdge patternEdge) {
        return dataEdge.getLabel().equals(patternEdge.getLabel()) &&
                dataEdge.getSource().getType().equals(patternEdge.getSource().getType()) &&
                dataEdge.getTarget().getType().equals(patternEdge.getTarget().getType());
    }

    private void extractValidMatches(Graph<Vertex, RelationshipEdge> patternGraph, List<List<Vertex>> currentMatches, Vertex centerDataVertex, int year,
                                     Set<Set<ConstantLiteral>> matchesAroundCenterVertex, Map<String, List<Integer>> entityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Set<Vertex> patternVertexSet = patternGraph.vertexSet();
        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(centerDataVertex, vertexTypesToActiveAttributesMap.get(centerDataVertex.getType()));

        for (List<Vertex> vertexList : currentMatches) {
            Set<ConstantLiteral> match = new HashSet<>(centerDataVertexLiterals);

            // Convert match to literals and validate match size upfront
            for (Vertex vertex : vertexList) {
                Map<String, Attribute> attributesMap = getAttributesMap(vertex);
                addAttributesToMatch(vertex.getType(), attributesMap, vertexTypesToActiveAttributesMap.get(vertex.getType()), match);
            }

            // Only consider matches that at least cover all pattern vertices once
            if (match.size() > patternVertexSet.size()) {
                matchesAroundCenterVertex.add(match);
                updateEntityURIs(centerDataVertex, matchesAroundCenterVertex, entityURIs, year);
            }
        }
    }

}
