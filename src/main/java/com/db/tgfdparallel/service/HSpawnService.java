package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
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

    @Autowired
    public HSpawnService(AppConfig config, PatternService patternService, TGFDService tgfdService) {
        this.config = config;
        this.patternService = patternService;
        this.tgfdService = tgfdService;
    }

    public List<List<TGFD>> performHSPawn(Map<String, Set<String>> vertexTypesToActiveAttributesMap, PatternTreeNode patternTreeNode,
                                          List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        List<List<TGFD>> result = new ArrayList<>(2);
        Graph<Vertex, RelationshipEdge> graph = patternTreeNode.getPattern().getPattern();

        List<ConstantLiteral> activeAttributesInPattern = new ArrayList<>(patternService.getActiveAttributesInPattern(graph.vertexSet(), false, vertexTypesToActiveAttributesMap));
        LiteralTree literalTree = new LiteralTree();

        // TODO: 设置多少hspawn limit?
//        if (Util.onlyInterestingTGFDs) {
//            hSpawnLimit = Math.max(patternTreeNode.getGraph().vertexSet().size(), Util.maxNumOfLiterals);
//        } else {
//            hSpawnLimit = Util.maxNumOfLiterals;
//        }
        int hSpawnLimit = graph.vertexSet().size();

        List<LiteralTreeNode> firstLevelLiteral = activeAttributesInPattern.stream().map(x -> new LiteralTreeNode(null, x)).collect(Collectors.toList());
        literalTree.getTree().get(0).addAll(firstLevelLiteral);

        for (int i = 1; i < hSpawnLimit; i++) {
            List<LiteralTreeNode> currentLiteralLevel = new ArrayList<>();
            List<LiteralTreeNode> literalTreePreviousLevel = literalTree.getTree().get(i - 1);
            if (literalTreePreviousLevel.isEmpty()) {
                logger.info(("Previous level of literal tree is empty. Nothing to expand. End HSpawn"));
                break;
            }

            Set<AttributeDependency> visitedPaths = new HashSet<>();
            List<TGFD> currentLevelConstantTGFDs = new ArrayList<>();
            List<TGFD> currentLevelGeneralTGFDs = new ArrayList<>();

            for (LiteralTreeNode previousLiteral : literalTreePreviousLevel) {
                List<ConstantLiteral> parentsPathToRoot = getPathToRoot(previousLiteral);

                // TODO: 啥是pruned
                if (previousLiteral.isPruned()) {
                    logger.info("Could not expand pruned literal path.");
                    continue;
                }

                for (ConstantLiteral constantLiteral : activeAttributesInPattern) {
                    //TODO: check
//                    if (Util.onlyInterestingTGFDs && j < patternTreeNode.getGraph().vertexSet().size()) { // Ensures all vertices are involved in dependency
//                        if (Util.isUsedVertexType(literal.getVertexType(), parentsPathToRoot))
//                            continue;
//                    }

                    if (parentsPathToRoot.contains(constantLiteral)) {
                        logger.info("Skip. Literal already exists in path.");
                        continue;
                    }

                    AttributeDependency newPath = new AttributeDependency(parentsPathToRoot, constantLiteral);
                    if (visitedPaths.contains(newPath)) { // TODO: Is this relevant anymore?
                        logger.info("Skip. Duplicate literal path.");
                        continue;
                    }

                    //TODO: Check
//                    boolean isSuperSetPath = false;
//                    if (Util.hasSupportPruning && newPath.isSuperSetOfPath(patternTreeNode.getZeroEntityDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
//                        System.out.println("Skip. Candidate literal path is a superset of zero-entity dependency.");
//                        isSuperSetPath = true;
//                    } else if (Util.hasMinimalityPruning && newPath.isSuperSetOfPath(patternTreeNode.getAllMinimalDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have already have a general dependency
//                        System.out.println("Skip. Candidate literal path is a superset of minimal dependency.");
//                        isSuperSetPath = true;
//                    }

                    visitedPaths.add(newPath);
                    LiteralTreeNode node = new LiteralTreeNode(previousLiteral, constantLiteral);
                    currentLiteralLevel.add(node);

                    //TODO: Check
//                    if (Util.onlyInterestingTGFDs) { // Ensures all vertices are involved in dependency
//                        if (Util.literalPathIsMissingTypesInPattern(literalTreeNode.getPathToRoot(), patternTreeNode.getGraph().vertexSet())) {
//                            System.out.println("Skip Delta Discovery. Literal path does not involve all pattern vertices.");
//                            continue;
//                        }
//                    }
                    addDependencyAttributesToPattern(patternTreeNode.getPattern(), newPath);
                    Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entities = findEntities(newPath, matchesPerTimestamps);
                    Map<Pair, List<TreeSet<Pair>>> deltaToPairsMap = new HashMap<>();

                    List<TGFD> constantTGFD = tgfdService.discoverConstantTGFD(patternTreeNode, newPath.getRhs(), entities, deltaToPairsMap);
                    result.get(0).addAll(constantTGFD);

                    if (!deltaToPairsMap.isEmpty()) {
                        List<TGFD> generalTGFD = tgfdService.discoverGeneralTGFD(patternTreeNode, patternTreeNode.getPatternSupport(),
                                newPath, entities.size(), deltaToPairsMap, node);
                        result.get(1).addAll(generalTGFD);
                    }
                }
            }
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
            for (String vType : v.getTypes()) {
                ConstantLiteral constantLiteral = attributeMap.getOrDefault(vType, null);
                if (constantLiteral != null) {
                    Attribute attribute = new Attribute(constantLiteral.getAttrName());
                    v.getAttributes().add(attribute);
                }
            }
        }
    }

    public Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> findEntities(AttributeDependency attributes,
                                                                                                   List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        Map<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entitiesWithRHSvalues = new HashMap<>();
        Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entitiesWithSortedRHSvalues = new HashMap<>();
        String yVertexType = attributes.getRhs().getVertexType();
        String yAttrName = attributes.getRhs().getAttrName();
        Set<ConstantLiteral> xAttributes = attributes.getLhs();

        for (int timestamp = 0; timestamp < matchesPerTimestamps.size(); timestamp++) {
            Set<Set<ConstantLiteral>> matchesInOneTimeStamp = matchesPerTimestamps.get(timestamp);
            if (!matchesInOneTimeStamp.isEmpty()) {
                for (Set<ConstantLiteral> match : matchesInOneTimeStamp) {
                    if (match.size() < attributes.getLhs().size() + 1)
                        continue;

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
                    if (entity.size() < xAttributes.size() || rhs == null)
                        continue;

//                    entitiesWithRHSvalues.get(entity).get(rhs).set(timestamp, entitiesWithRHSvalues.get(entity).get(rhs).get(timestamp) + 1);
                    Map<ConstantLiteral, List<Integer>> entityRHSvalues = entitiesWithRHSvalues.getOrDefault(entity, new HashMap<>());
                    List<Integer> rhsValues = entityRHSvalues.getOrDefault(rhs, new ArrayList<>(config.getTimestamp()));
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
        return entitiesWithSortedRHSvalues;
    }
}
