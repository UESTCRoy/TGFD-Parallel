package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.*;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
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

    public int runSnapshot(int snapshotID, PatternTreeNode newPattern, GraphLoader loader,
                           Set<Set<ConstantLiteral>> matchesOnTimestamps, int level,
                           Map<String, List<Integer>> entityURIs, Map<String, List<Integer>> ptnEntityURIs,
                           Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        String centerVertexType = newPattern.getPattern().getCenterVertex().getType();
        level = Math.min(level, 2);

        Set<String> validTypes = newPattern.getPattern().getPattern().vertexSet().stream()
                .map(Vertex::getType)
                .collect(Collectors.toSet());

        List<Iterator<GraphMapping<Vertex, RelationshipEdge>>> allMappings = new ArrayList<>();

        graph.vertexSet().stream()
                .filter(vertex -> vertex.getType().equals(centerVertexType))
                .filter(vertex -> entityURIs.containsKey(vertex.getUri()))
                .filter(vertex -> entityURIs.get(vertex.getUri()).get(snapshotID) > 0)
                .forEach(centerVertex -> {
                    Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, centerVertex, 1, validTypes);

                    long isomorphismStartTime = System.currentTimeMillis();
                    VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = graphService.checkIsomorphism(subgraph, newPattern.getPattern(), false);
                    long isomorphismEndTime = System.currentTimeMillis();
                    long isomorphismDuration = isomorphismEndTime - isomorphismStartTime;
                    if (isomorphismDuration > 10000) {
                        logger.info("Isomorphism check for snapshot {} took {} ms", snapshotID, isomorphismDuration);
                    }

                    if (results.isomorphismExists()) {
                        allMappings.add(results.getMappings());
                        Set<Set<ConstantLiteral>> matches = new HashSet<>();
                        long extractMatchesStartTime = System.currentTimeMillis();
                        patternService.extractMatches(results.getMappings(), matches, newPattern, ptnEntityURIs, snapshotID, vertexTypesToActiveAttributesMap);
                        long extractMatchesEndTime = System.currentTimeMillis();
                        long extractDuration = extractMatchesEndTime - extractMatchesStartTime;
                        if (extractDuration > 10000) {
                            logger.info("Extracting matches for snapshot {} took {} ms", snapshotID, extractMatchesEndTime - extractMatchesStartTime);
                            logger.info("Matches: {}", matches.size());
                            long currentMappingCount = countMappings(results.getMappings());
                            logger.info("Mappings: {}", currentMappingCount);
                        }

                        matchesOnTimestamps.addAll(matches);
                    }
                });

        long totalMappings = allMappings.stream().mapToLong(this::countMappings).sum();
        logger.info("Total number of isomorphisms for all snapshots: {}", totalMappings);

        return matchesOnTimestamps.size();
    }

    private long countMappings(Iterator<GraphMapping<Vertex, RelationshipEdge>> mappings) {
        long count = 0;
        while (mappings.hasNext()) {
            mappings.next();
            count++;
        }
        return count;
    }
}
