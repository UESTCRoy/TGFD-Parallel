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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@EnableAsync
public class AsyncService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncService.class);
    private final GraphService graphService;
    private final PatternService patternService;

    @Autowired
    public AsyncService(GraphService graphService, PatternService patternService) {
        this.graphService = graphService;
        this.patternService = patternService;
    }

    @Async
    public CompletableFuture<Integer> runSnapshotAsync(int snapshotID, PatternTreeNode newPattern, GraphLoader loader,
                                                       Set<Set<ConstantLiteral>> matchesOnTimestamps, int level, Map<String, List<Integer>> entityURIs,
                                                       Map<String, List<Integer>> ptnEntityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        logger.info("Starting async task for snapshot {}", snapshotID);
        Integer result = runSnapshot(snapshotID, newPattern, loader, matchesOnTimestamps, level, entityURIs, ptnEntityURIs, vertexTypesToActiveAttributesMap);
        logger.info("Completed async task for snapshot {}, result: {}", snapshotID, result);
        return CompletableFuture.completedFuture(result);
    }

    public int runSnapshot(int snapshotID, PatternTreeNode newPattern, GraphLoader loader, Set<Set<ConstantLiteral>> matchesOnTimestamps, int level,
                           Map<String, List<Integer>> entityURIs, Map<String, List<Integer>> ptnEntityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        String centerVertexType = newPattern.getPattern().getCenterVertex().getType();
        level = Math.min(level, 2);

        Set<String> validTypes = newPattern.getPattern().getPattern().vertexSet().stream()
                .map(Vertex::getType)
                .collect(Collectors.toSet());

        int finalLevel = level;
        graph.vertexSet().stream()
                .filter(vertex -> vertex.getType().equals(centerVertexType))
                .filter(vertex -> entityURIs.containsKey(vertex.getUri()))
                .filter(vertex -> entityURIs.get(vertex.getUri()).get(snapshotID) > 0)
                .forEach(centerVertex -> {
                    Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, centerVertex, 1, validTypes);
                    VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results =
                            graphService.checkIsomorphism(subgraph, newPattern.getPattern(), false);

                    if (results.isomorphismExists()) {
                        Set<Set<ConstantLiteral>> matches = new HashSet<>();
                        patternService.extractMatches(results.getMappings(), matches, newPattern, ptnEntityURIs, snapshotID, vertexTypesToActiveAttributesMap);
                        matchesOnTimestamps.addAll(matches);
                    }
                });
        return matchesOnTimestamps.size();
    }
}
