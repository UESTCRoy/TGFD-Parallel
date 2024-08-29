package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.*;
import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@EnableAsync
public class AsyncService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncService.class);
    private final GraphService graphService;
    private final PatternService patternService;
    private final TGFDService tgfdService;
    private final DependencyService dependencyService;
    private final FastMatchService fastMatchService;

    @Autowired
    public AsyncService(GraphService graphService, PatternService patternService, TGFDService tgfdService, DependencyService dependencyService,
                        FastMatchService fastMatchService) {
        this.graphService = graphService;
        this.patternService = patternService;
        this.tgfdService = tgfdService;
        this.dependencyService = dependencyService;
        this.fastMatchService = fastMatchService;
    }

    @Async
    public CompletableFuture<Integer> runSnapshotAsync(int snapshotID, PatternTreeNode newPattern, GraphLoader loader,
                                                       List<Set<ConstantLiteral>> matchesOnTimestamps, int level, Map<String, List<Integer>> entityURIs,
                                                       Map<String, List<Integer>> ptnEntityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        long startTime = System.currentTimeMillis();
        Integer result = runFastMatchSnapshot(snapshotID, newPattern, loader, matchesOnTimestamps, level, entityURIs, ptnEntityURIs, vertexTypesToActiveAttributesMap);
        long endTime = System.currentTimeMillis();
        logger.info("Async task for snapshot {} started and completed in {} ms, result: {}", snapshotID, (endTime - startTime), result);
        return CompletableFuture.completedFuture(result);
    }

    public int runSnapshot(int snapshotID, PatternTreeNode newPattern, GraphLoader loader,
                           Set<Set<ConstantLiteral>> matchesOnTimestamps, int level, Map<String, List<Integer>> entityURIs,
                           Map<String, List<Integer>> ptnEntityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        String centerVertexType = newPattern.getPattern().getCenterVertex().getType();
        level = Math.min(level, 2);

        Set<String> validTypes = newPattern.getPattern().getPattern().vertexSet().stream()
                .map(Vertex::getType)
                .collect(Collectors.toSet());

        PatternType patternType = patternService.assignPatternType(newPattern.getPattern());
        int diameter = (patternType == PatternType.Line || patternType == PatternType.Circle || patternType == PatternType.Complex) ? 2 : 1;
        logger.info("Pattern shape: {} and diameter is {}", patternType, diameter);

        graph.vertexSet().stream()
                .filter(vertex -> vertex.getType().equals(centerVertexType))
                .filter(vertex -> entityURIs.containsKey(vertex.getUri()))
                .filter(vertex -> entityURIs.get(vertex.getUri()).get(snapshotID) > 0)
                .forEach(centerVertex -> {
                    long subGraphStartTime = System.currentTimeMillis();
                    Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, centerVertex, diameter, validTypes);
                    long subGraphEndTime = System.currentTimeMillis();
                    long subGraphDuration = subGraphEndTime - subGraphStartTime;
                    if (subGraphDuration > 10000) {
                        logger.info("Subgraph creation for snapshot {} took {} ms", snapshotID, subGraphDuration);
                    }

                    long isomorphismStartTime = System.currentTimeMillis();
                    VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = graphService.checkIsomorphism(subgraph, newPattern.getPattern(), false);
                    long isomorphismEndTime = System.currentTimeMillis();
                    long isomorphismDuration = isomorphismEndTime - isomorphismStartTime;
                    if (isomorphismDuration > 10000) {
                        logger.info("Isomorphism check for snapshot {} took {} ms", snapshotID, isomorphismDuration);
                    }

                    if (results.isomorphismExists()) {
                        Set<Set<ConstantLiteral>> matches = new HashSet<>();
                        long extractMatchesStartTime = System.currentTimeMillis();
                        patternService.extractMatches(results.getMappings(), matches, newPattern, ptnEntityURIs, snapshotID, vertexTypesToActiveAttributesMap);
                        long extractMatchesEndTime = System.currentTimeMillis();
                        long extractDuration = extractMatchesEndTime - extractMatchesStartTime;
//                        if (extractDuration > 10000) {
//                            logger.info("Snapshot {}: Extraction took {} ms, Matches: {}", snapshotID, extractDuration, matches.size());
//                        }

                        matchesOnTimestamps.addAll(matches);
                    }
                });

        return matchesOnTimestamps.size();
    }

    public int runFastMatchSnapshot(int snapshotID, PatternTreeNode newPattern, GraphLoader loader,
                                    List<Set<ConstantLiteral>> matchesOnTimestamps, int level, Map<String, List<Integer>> entityURIs,
                                    Map<String, List<Integer>> ptnEntityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        String centerVertexType = newPattern.getPattern().getCenterVertex().getType();

        Set<String> validTypes = newPattern.getPattern().getPattern().vertexSet().stream()
                .map(Vertex::getType)
                .filter(type -> !type.equals(centerVertexType))
                .collect(Collectors.toSet());

        PatternType patternType = patternService.assignPatternType(newPattern.getPattern());
        int diameter;
        switch (patternType) {
            case Line:
            case Circle:
                diameter = 2;
                break;
            case Complex:
                diameter = 3;
                break;
            default:
                diameter = 1;
                break;
        }
        logger.info("Pattern shape: {} and diameter is {}", patternType, diameter);

        graph.vertexSet().stream()
                .filter(vertex -> vertex.getType().equals(centerVertexType) && entityURIs.containsKey(vertex.getUri()) && entityURIs.get(vertex.getUri()).get(snapshotID) > 0)
                .forEach(centerVertex -> {
                    Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, centerVertex, diameter, validTypes);
                    Set<String> realGraphVertexTypes = subgraph.vertexSet().stream().map(Vertex::getType).collect(Collectors.toSet());
                    if (realGraphVertexTypes.containsAll(validTypes)) {
                        fastMatchService.findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(newPattern.getPattern(), subgraph, centerVertex, snapshotID,
                                matchesOnTimestamps, ptnEntityURIs, vertexTypesToActiveAttributesMap);
                    }
                });

        return matchesOnTimestamps.size();
    }

    @Async
    public CompletableFuture<List<List<TGFD>>> findTGFDsAsync(PatternTreeNode patternTreeNode, AttributeDependency newPath,
                                                              List<List<Set<ConstantLiteral>>> matchesPerTimestamps, Map<Integer, Integer> dependencyNumberMap) {
        long startTime = System.currentTimeMillis();
        List<List<TGFD>> result = findTGFDs(patternTreeNode, newPath, matchesPerTimestamps, dependencyNumberMap);
        long endTime = System.currentTimeMillis();
//        logger.info("Async task for finding TGFDs for dependency {} completed in {} ms", newPath, (endTime - startTime));
        return CompletableFuture.completedFuture(result);
    }

    public List<List<TGFD>> findTGFDs(PatternTreeNode patternTreeNode, AttributeDependency newPath, List<List<Set<ConstantLiteral>>> matchesPerTimestamps,
                                      Map<Integer, Integer> dependencyNumberMap) {
        List<List<TGFD>> result = new ArrayList<>();
        result.add(new ArrayList<>());
        result.add(new ArrayList<>());

        Map<List<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entities = dependencyService.findEntities(newPath, matchesPerTimestamps);
        List<Pair> candidatePairs = new ArrayList<>();

        int dependencyKey = tgfdService.generateDependencyKey(newPath);
        dependencyNumberMap.put(dependencyKey, entities.size());

        Set<TGFD> constantTGFD = tgfdService.discoverConstantTGFD(patternTreeNode, newPath.getRhs(), entities, candidatePairs, dependencyKey);
        result.get(0).addAll(constantTGFD);

        if (!candidatePairs.isEmpty()) {
            Set<TGFD> generalTGFD = tgfdService.discoverGeneralTGFD(patternTreeNode, newPath, candidatePairs);
            result.get(1).addAll(generalTGFD);
        }

        return result;
    }
}
