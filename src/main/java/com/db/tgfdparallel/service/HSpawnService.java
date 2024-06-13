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
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class HSpawnService {
    private static final Logger logger = LoggerFactory.getLogger(HSpawnService.class);
    private final AppConfig config;
    private final PatternService patternService;
    private final TGFDService tgfdService;
    private final DependencyService dependencyService;

    @Autowired
    public HSpawnService(AppConfig config, PatternService patternService, TGFDService tgfdService, DependencyService dependencyService) {
        this.config = config;
        this.patternService = patternService;
        this.tgfdService = tgfdService;
        this.dependencyService = dependencyService;
    }

    // TODO: 改造成异步的方式
    public List<List<TGFD>> performHSPawn(Map<String, Set<String>> vertexTypesToActiveAttributesMap, PatternTreeNode patternTreeNode,
                                          List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        List<List<TGFD>> result = Collections.nCopies(2, new ArrayList<TGFD>())
                .stream()
                .map(ArrayList::new)
                .collect(Collectors.toList());
        Graph<Vertex, RelationshipEdge> graph = patternTreeNode.getPattern().getPattern();
        // TODO: 如果pth中的vertex 并没有active attribute，我们是否应该过滤掉？
        List<ConstantLiteral> activeAttributesInPattern = new ArrayList<>(patternService.getActiveAttributesInPattern(graph.vertexSet(), false, vertexTypesToActiveAttributesMap));
        LiteralTree literalTree = new LiteralTree();

        logger.info("Active attributes in pattern: {}", activeAttributesInPattern);

        int hSpawnLimit = graph.vertexSet().size();

        List<LiteralTreeNode> firstLevelLiteral = activeAttributesInPattern.stream().map(x -> new LiteralTreeNode(null, x)).collect(Collectors.toList());
        literalTree.getTree().add(firstLevelLiteral);

        for (int i = 1; i < hSpawnLimit; i++) {
            List<LiteralTreeNode> currentLiteralLevel = new ArrayList<>();
            List<LiteralTreeNode> literalTreePreviousLevel = literalTree.getTree().get(i - 1);
            if (literalTreePreviousLevel.isEmpty()) {
                logger.info(("Previous level of literal tree is empty. Nothing to expand. End HSpawn"));
                break;
            }

            Set<AttributeDependency> visitedPaths = new HashSet<>();

            for (LiteralTreeNode previousLiteral : literalTreePreviousLevel) {
                List<ConstantLiteral> parentsPathToRoot = getPathToRoot(previousLiteral);

                if (previousLiteral.isPruned()) {
                    logger.info("Could not expand pruned literal path.");
                    continue;
                }

                for (ConstantLiteral constantLiteral : activeAttributesInPattern) {
                    if (isUsedVertexType(constantLiteral.getVertexType(), parentsPathToRoot)) {
                        continue;
                    }

                    if (parentsPathToRoot.contains(constantLiteral)) {
                        logger.info("Skip. Literal already exists in path.");
                        continue;
                    }

                    AttributeDependency newPath = new AttributeDependency(parentsPathToRoot, constantLiteral);
                    if (visitedPaths.contains(newPath)) {
                        continue;
                    }

                    List<AttributeDependency> allMinimalDependenciesOnThisPath = patternService.getAllMinimalDependenciesOnThisPath(patternTreeNode);
                    if (dependencyService.isSuperSetOfPath(newPath, allMinimalDependenciesOnThisPath)) {
                        continue;
                    }

                    LiteralTreeNode node = new LiteralTreeNode(previousLiteral, constantLiteral);
                    currentLiteralLevel.add(node);

                    if (patternService.literalPathIsMissingTypesInPattern(getPathToRoot(node), graph.vertexSet())) {
//                        logger.info("Skip Delta Discovery. Literal path does not involve all pattern vertices.");
                        continue;
                    }

                    visitedPaths.add(newPath);

                    VF2PatternGraph patternGraphCopy = DeepCopyUtil.deepCopy(patternTreeNode.getPattern());
                    addDependencyAttributesToPattern(patternGraphCopy, newPath);
                    Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entities = findEntities(newPath, matchesPerTimestamps);
                    List<Pair> candidatePairs = new ArrayList<>();

                    List<TGFD> constantTGFD = tgfdService.discoverConstantTGFD(patternTreeNode, newPath.getRhs(), entities, candidatePairs);
                    logger.info("There are {} constant TGFDs discovered for dependency {}", constantTGFD.size(), newPath);
                    result.get(0).addAll(constantTGFD);

                    if (!candidatePairs.isEmpty()) {
                        List<TGFD> generalTGFD = tgfdService.discoverGeneralTGFD(patternTreeNode, patternTreeNode.getPatternSupport(),
                                newPath, candidatePairs, entities.size());
                        result.get(1).addAll(generalTGFD);
                    }
                }
            }
            literalTree.getTree().add(currentLiteralLevel);
        }

        return result;
    }

    public List<ConstantLiteral> getPathToRoot(LiteralTreeNode currentNode) {
        List<ConstantLiteral> literalPath = new ArrayList<>();
        for (LiteralTreeNode node = currentNode; node != null; node = node.getParent()) {
            literalPath.add(node.getLiteral());
        }
        Collections.reverse(literalPath);
        return literalPath;
    }

    public void addDependencyAttributesToPattern(VF2PatternGraph patternForDependency, AttributeDependency literalPath) {
        Set<ConstantLiteral> attributesSetForDependency = new HashSet<>(literalPath.getLhs());
        attributesSetForDependency.add(literalPath.getRhs());

        Map<String, ConstantLiteral> attributeMap = attributesSetForDependency.stream()
                .collect(Collectors.toMap(ConstantLiteral::getVertexType, Function.identity()));

        for (Vertex v : patternForDependency.getPattern().vertexSet()) {
            String vType = v.getType();
            ConstantLiteral constantLiteral = attributeMap.getOrDefault(vType, null);
            if (constantLiteral != null) {
                Attribute attribute = new Attribute(constantLiteral.getAttrName());
                v.getAttributes().add(attribute);
            }
        }
    }

    public Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> findEntities(AttributeDependency dependency,
                                                                                                   List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        long startTime = System.currentTimeMillis();

        Map<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entitiesWithRHSvalues = new HashMap<>();
        Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entitiesWithSortedRHSvalues = new HashMap<>();
        String yVertexType = dependency.getRhs().getVertexType();
        String yAttrName = dependency.getRhs().getAttrName();
        Set<ConstantLiteral> xAttributes = dependency.getLhs();

        for (int timestamp = 0; timestamp < matchesPerTimestamps.size(); timestamp++) {
            Set<Set<ConstantLiteral>> matchesInOneTimeStamp = matchesPerTimestamps.get(timestamp);
            if (!matchesInOneTimeStamp.isEmpty()) {
                for (Set<ConstantLiteral> match : matchesInOneTimeStamp) {
                    if (match.size() < dependency.getLhs().size() + 1) {
                        continue;
                    }

                    Set<ConstantLiteral> entity = new HashSet<>();
                    ConstantLiteral rhs = null;
                    for (ConstantLiteral literalInMatch : match) {
                        if (literalInMatch.getVertexType().equals(yVertexType) && literalInMatch.getAttrName().equals(yAttrName)) {
                            rhs = literalInMatch;
                            continue;
                        }
                        for (ConstantLiteral attribute : xAttributes) {
                            if (literalInMatch.getVertexType().equals(attribute.getVertexType()) && literalInMatch.getAttrName().equals(attribute.getAttrName()))
                                entity.add(literalInMatch);
                        }
                    }
                    if (entity.size() < xAttributes.size() || rhs == null) {
                        continue;
                    }

//                    entitiesWithRHSvalues.get(entity).get(rhs).set(timestamp, entitiesWithRHSvalues.get(entity).get(rhs).get(timestamp) + 1);
                    Map<ConstantLiteral, List<Integer>> entityRHSvalues = entitiesWithRHSvalues.getOrDefault(entity, new HashMap<>());
                    List<Integer> rhsValues = entityRHSvalues.getOrDefault(rhs, new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0)));
                    rhsValues.set(timestamp, rhsValues.get(timestamp) + 1);
                    entityRHSvalues.put(rhs, rhsValues);
                    entitiesWithRHSvalues.put(entity, entityRHSvalues);
                }
            }
        }

        Comparator<Map.Entry<ConstantLiteral, List<Integer>>> comparator =
                (o1, o2) ->
                        o2.getValue()
                                .stream()
                                .reduce(0, Integer::sum) -
                                o1.getValue()
                                        .stream()
                                        .reduce(0, Integer::sum);

        for (Map.Entry<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entityEntry : entitiesWithRHSvalues.entrySet()) {
            Map<ConstantLiteral, List<Integer>> rhsMapOfEntity = entityEntry.getValue();
            List<Map.Entry<ConstantLiteral, List<Integer>>> sortedRhsMapOfEntity = rhsMapOfEntity.entrySet()
                    .stream()
                    .sorted(comparator)
                    .collect(Collectors.toList());
            entitiesWithSortedRHSvalues.put(entityEntry.getKey(), sortedRhsMapOfEntity);
        }
        long endTime = System.currentTimeMillis();
        logger.info("Time to find entities {}: {}", dependency, (endTime - startTime));

        return entitiesWithSortedRHSvalues;
    }

    private boolean isUsedVertexType(String vertexType, List<ConstantLiteral> parentsPathToRoot) {
        for (ConstantLiteral literal : parentsPathToRoot) {
            if (literal.getVertexType().equals(vertexType)) {
                return true;
            }
        }
        return false;
    }
}
