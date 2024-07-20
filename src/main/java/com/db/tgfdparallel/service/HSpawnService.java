package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.*;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class HSpawnService {
    private static final Logger logger = LoggerFactory.getLogger(HSpawnService.class);
    private final PatternService patternService;
    private final DependencyService dependencyService;
    private final AsyncService asyncService;

    @Autowired
    public HSpawnService(PatternService patternService, DependencyService dependencyService, AsyncService asyncService) {
        this.patternService = patternService;
        this.dependencyService = dependencyService;
        this.asyncService = asyncService;
    }

    public List<List<TGFD>> performHSPawn(Map<String, Set<String>> vertexTypesToActiveAttributesMap, PatternTreeNode patternTreeNode,
                                          List<Set<Set<ConstantLiteral>>> matchesPerTimestamps, Map<Integer, Integer> dependencyNumberMap) {
        Graph<Vertex, RelationshipEdge> graph = patternTreeNode.getPattern().getPattern();
        List<ConstantLiteral> activeAttributesInPattern = new ArrayList<>(patternService.getActiveAttributesInPattern(graph.vertexSet(), false, vertexTypesToActiveAttributesMap));
        int hSpawnLimit = graph.vertexSet().size();

        List<AttributeDependency> newPaths = dependencyService.generateAllPaths(activeAttributesInPattern, hSpawnLimit);
        List<AttributeDependency> allMinimalDependenciesOnThisPath = patternService.getAllMinimalDependenciesOnThisPath(patternTreeNode);
        newPaths.removeIf(newPath -> dependencyService.isSuperSetOfPath(newPath, allMinimalDependenciesOnThisPath));


        List<List<TGFD>> combinedResult = new ArrayList<>();
        combinedResult.add(new ArrayList<>()); // constant TGFDs
        combinedResult.add(new ArrayList<>()); // general TGFDs

        for (AttributeDependency newPath : newPaths) {
            List<List<TGFD>> res = asyncService.findTGFDs(patternTreeNode, newPath, matchesPerTimestamps, dependencyNumberMap);
            combinedResult.get(0).addAll(res.get(0));
            combinedResult.get(1).addAll(res.get(1));
        }

        return combinedResult;
    }
}
