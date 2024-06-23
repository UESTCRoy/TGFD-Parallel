package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.utils.DeepCopyUtil;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class PatternService {

    private static final Logger logger = LoggerFactory.getLogger(PatternService.class);
    private final AppConfig config;
    private final GraphService graphService;

    public PatternService(GraphService graphService, AppConfig config) {
        this.graphService = graphService;
        this.config = config;
    }

    public List<PatternTreeNode> vSpawnSinglePatternTreeNode(List<FrequencyStatistics> sortedVertexHistogram, PatternTree patternTree) {
        addLevel(patternTree);
        List<PatternTreeNode> singleNodePatternTreeNodes = new ArrayList<>();

        logger.info("VSpawn Level 0");

        for (int i = 0; i < sortedVertexHistogram.size(); i++) {
            long vSpawnTime = System.currentTimeMillis();
            String patternVertexType = sortedVertexHistogram.get(i).getType();

            logger.info("Vertex type: " + patternVertexType);
            Vertex vertex = new Vertex(patternVertexType);
            Graph<Vertex, RelationshipEdge> graph = new DefaultDirectedGraph<>(RelationshipEdge.class);
            graph.addVertex(vertex);
            VF2PatternGraph candidatePattern = new VF2PatternGraph(graph, patternVertexType, vertex);

            PatternTreeNode patternTreeNode = new PatternTreeNode();
            patternTreeNode.setPattern(candidatePattern);

            singleNodePatternTreeNodes.add(patternTreeNode);
            patternTree.getTree().get(0).add(patternTreeNode);

            long finalVspawnTime = System.currentTimeMillis() - vSpawnTime;
            logger.info("VSpawn Time: " + finalVspawnTime + " ms");
        }

        logger.info("GenTree Level 0" + " size: " + patternTree.getTree().get(0).size());

        for (PatternTreeNode node : patternTree.getTree().get(0)) {
            logger.info("Pattern Type: " + node.getPattern().getCenterVertexType());
        }

        return singleNodePatternTreeNodes;
    }

    public void addLevel(PatternTree tree) {
        tree.getTree().add(new ArrayList<>());
    }

    public void addVertex(VF2PatternGraph candidatePattern, Vertex vertex) {
        candidatePattern.getPattern().addVertex(vertex);
    }

    public void singleNodePatternInitialization(VF2DataGraph dataGraph, int snapshotID,
                                                Map<String, PatternTreeNode> singlePatternTreeNodesMap,
                                                Map<String, Map<String, List<Integer>>> entityURIsByPTN) {
        // We start from the singleNodeVertex, so the initial diameter is set to 0.
        Graph<Vertex, RelationshipEdge> graph = dataGraph.getGraph();

        for (Map.Entry<String, PatternTreeNode> entry : singlePatternTreeNodesMap.entrySet()) {
            String ptnType = entry.getKey();
            Map<String, List<Integer>> entityURIs = entityURIsByPTN.get(ptnType);

            graph.vertexSet().stream()
                    .filter(vertex -> vertex.getType().equals(ptnType))
                    .forEach(vertex -> {
                        entityURIs.computeIfAbsent(vertex.getUri(), k -> new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0)))
                                .set(snapshotID, entityURIs.get(vertex.getUri()).get(snapshotID) + 1);
                    });
        }
    }

    public void extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, Set<Set<ConstantLiteral>> matches,
                               PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp,
                               Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
//        int numOfMatches = 0;
        while (iterator.hasNext()) {
            GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
            Set<ConstantLiteral> literalsInMatch = new HashSet<>();
            String entityURI = extractMatch(result, patternTreeNode, literalsInMatch, vertexTypesToActiveAttributesMap);

            if (literalsInMatch.size() >= patternTreeNode.getPattern().getPattern().vertexSet().size()) {
//                numOfMatches++;
                if (entityURI != null) {
                    List<Integer> counts = entityURIs.computeIfAbsent(entityURI, k -> new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0)));
                    counts.set(timestamp, counts.get(timestamp) + 1);
                }
                matches.add(literalsInMatch);
            }
        }
    }

    public String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode,
                               Set<ConstantLiteral> match, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String entityURI = "";
        for (Vertex v : patternTreeNode.getPattern().getPattern().vertexSet()) {
            Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
            if (currentMatchedVertex != null) {
                String tempEntityURI = extractAttributes(patternTreeNode, match, currentMatchedVertex, vertexTypesToActiveAttributesMap);
                if (tempEntityURI != null) {
                    entityURI = tempEntityURI;
                }
            }
        }
        return entityURI;
    }

    public String extractAttributes(PatternTreeNode patternTreeNode, Set<ConstantLiteral> match, Vertex currentMatchedVertex,
                                    Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String entityURI = null;
        String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
        String matchedVertexType = currentMatchedVertex.getType();

        Map<String, Attribute> vertexAllAttributesMap = currentMatchedVertex.getAttributes().stream()
                .collect(Collectors.toMap(Attribute::getAttrName, Function.identity()));
        Set<String> activeAttributeNames = vertexTypesToActiveAttributesMap.getOrDefault(matchedVertexType, new HashSet<>());

        for (String attrName : activeAttributeNames) {
            Attribute matchedAttribute = vertexAllAttributesMap.getOrDefault(attrName, null);
            if (matchedAttribute == null) continue;

            if (matchedVertexType.equals(centerVertexType)) {
                entityURI = matchedAttribute.getAttrValue();
            }

            String matchedAttrValue = matchedAttribute.getAttrValue();
            match.add(new ConstantLiteral(matchedVertexType, attrName, matchedAttrValue));
        }

        return entityURI;
    }

    public Set<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI,
                                                             Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        return vertexSet.stream()
                .flatMap(vertex -> {
                    Set<String> activeAttributes = vertexTypesToActiveAttributesMap.getOrDefault(vertex.getType(), Collections.emptySet());
                    return activeAttributes.stream().map(attrName -> new ConstantLiteral(vertex.getType(), attrName, null));
                })
                .collect(Collectors.toSet());
    }


    public void calculateTotalSupport(Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN,
                                      Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN,
                                      Map<String, Integer> vertexHistogram) {
        for (PatternTreeNode ptn : matchesPerTimestampsByPTN.keySet()) {
            int numberOfMatchesFound = matchesPerTimestampsByPTN.get(ptn).stream()
                    .mapToInt(Set::size)
                    .sum();
            logger.info("Total number of matches found across all snapshots:" + numberOfMatchesFound);

            double S = vertexHistogram.get(ptn.getPattern().getCenterVertexType());
            double patternSupport = calculatePatternSupport(entityURIsByPTN.get(ptn), S, config.getTimestamp());
            ptn.setPatternSupport(patternSupport);
        }
    }

    public double calculatePatternSupport(Map<String, List<Integer>> entityURIs, double S, int T) {
        int numOfPossiblePairs = 0;
        for (Map.Entry<String, List<Integer>> entityUriEntry : entityURIs.entrySet()) {
            int numberOfAcrossMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 0).count();
            int k = 2;
            if (numberOfAcrossMatchesOfEntity >= k)
                numOfPossiblePairs += CombinatoricsUtils.binomialCoefficient(numberOfAcrossMatchesOfEntity, k);

            int numberOfWithinMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 1).count();
            numOfPossiblePairs += numberOfWithinMatchesOfEntity;
        }
        return calculateSupport(numOfPossiblePairs, S, T);
    }

    public double calculateSupport(double numerator, double S, int T) {
        double denominator = S * CombinatoricsUtils.binomialCoefficient(T + 1, 2);
        if (numerator > denominator) {
            return 0.0;
        }
//            throw new IllegalArgumentException("numerator > denominator");
        return numerator / denominator;
    }

    public List<VSpawnPattern> vSpawnGenerator(List<String> edgeData, PatternTree patternTree, int level) {
        List<VSpawnPattern> vSpawnPatternList = new ArrayList<>();
        List<PatternTreeNode> nodes = patternTree.getTree().get(level);

        // TODO: Set pattern Pruned
        for (PatternTreeNode ptn : nodes) {
            if (ptn.isPruned()) {
                continue;
            }
            for (String edge : edgeData) {
                VSpawnPattern pattern = new VSpawnPattern();

                String[] parts = edge.split(" ");
                String sourceVertexType = parts[0];
                String label = parts[1];
                String targetVertexType = parts[2];

                // filter out duplicate edges
                if (checkEdgeProperties(ptn.getPattern(), label, sourceVertexType, targetVertexType)) {
                    logger.info("Duplicate edge. Skipping edge: " + edge);
                    continue;
                }

                Vertex sourceVertex = isDuplicateVertex(ptn.getPattern(), sourceVertexType);
                Vertex targetVertex = isDuplicateVertex(ptn.getPattern(), targetVertexType);
                if (sourceVertex == null && targetVertex == null) {
                    logger.info("Source and target vertices are not in the pattern. Skipping edge: " + edge);
                    continue;
                } else if (sourceVertex != null && targetVertex != null) {
                    logger.info("We do not support multiple edges between existing vertices. Skipping edge: " + edge);
                    continue;
                }

                VF2PatternGraph newPattern = DeepCopyUtil.deepCopy(ptn.getPattern());
                Graph<Vertex, RelationshipEdge> graph = newPattern.getPattern();

                if (sourceVertex == null) {
                    sourceVertex = new Vertex(sourceVertexType);
                    addVertex(newPattern, sourceVertex);
                }
                if (targetVertex == null) {
                    targetVertex = new Vertex(targetVertexType);
                    addVertex(newPattern, targetVertex);
                }
                RelationshipEdge newEdge = new RelationshipEdge(label);
                addEdge(graph, sourceVertex, targetVertex, newEdge);

                if (isIsomorphicPattern(newPattern, vSpawnPatternList)) {
                    logger.info("Skip. Candidate pattern is an isomorph of existing pattern");
                    continue;
                }

                if (isSuperGraphOfPrunedPattern(newPattern, patternTree)) {
                    logger.info("Skip. Candidate pattern is a supergraph of pruned pattern");
                    continue;
                }

                // TODO: What this method does?
                PatternTreeNode patternTreeNode = initializeNewNode(newPattern, ptn, edge, nodes, level);
                pattern.setOldPattern(ptn);
                pattern.setNewPattern(patternTreeNode);
                vSpawnPatternList.add(pattern);
            }
        }

        return vSpawnPatternList;
    }

    public boolean checkEdgeProperties(VF2PatternGraph pattern, String edgeType, String sourceType, String targetType) {
        for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
            boolean sourceMatches = edge.getSource().getType().equals(sourceType);
            boolean targetMatches = edge.getTarget().getType().equals(targetType);

            if (edge.getLabel().equalsIgnoreCase(edgeType) && sourceMatches && targetMatches) {
                return true;
            }
        }
        return false;
    }


    private Vertex isDuplicateVertex(VF2PatternGraph newPattern, String vertexType) {
        return newPattern.getPattern().vertexSet().stream()
                .filter(v -> v.getType().equals(vertexType))
                .findFirst()
                .orElse(null);
    }

    private boolean isIsomorphicPattern(VF2PatternGraph newPattern, List<VSpawnPattern> vSpawnPatternList) {
        Set<String> newEdges = newPattern.getPattern().edgeSet().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());

        for (VSpawnPattern vSpawnPattern : vSpawnPatternList) {
            Set<String> existingEdges = vSpawnPattern.getNewPattern().getPattern().getPattern().edgeSet().stream()
                    .map(Object::toString)
                    .collect(Collectors.toSet());
            if (newEdges.containsAll(existingEdges)) {
                return true;
            }
        }
        return false;
    }

    public VF2PatternGraph copyGraph(Graph<Vertex, RelationshipEdge> graph) {
        VF2PatternGraph newPattern = new VF2PatternGraph();
        Map<Vertex, Vertex> vertexMap = new HashMap<>();

        // Copy vertices and create a mapping from the original vertices to the new ones
        for (Vertex v : graph.vertexSet()) {
            Vertex newVertex = graphService.copyVertex(v);
            addVertex(newPattern, newVertex);
            vertexMap.put(v, newVertex);
        }

        // Copy edges using the mapping to find the corresponding source and target vertices
        for (RelationshipEdge e : graph.edgeSet()) {
            Vertex source = vertexMap.get(e.getSource());
            Vertex target = vertexMap.get(e.getTarget());
            addEdge(graph, source, target, new RelationshipEdge(e.getLabel()));
        }

        return newPattern;
    }

    public void addEdge(Graph<Vertex, RelationshipEdge> graph, Vertex v1, Vertex v2, RelationshipEdge edge) {
        graph.addEdge(v1, v2, edge);
    }

    public PatternTreeNode initializeNewNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String candidateEdgeString, List<PatternTreeNode> nodes, int level) {
        PatternTreeNode node = new PatternTreeNode(pattern, parentNode, candidateEdgeString);
        if (level == 0) {
            findFirstLevelSubgraphParents(node, nodes);
        } else {
            findSubgraphParents(node, nodes);
        }
        findCenterVertexParent(node, nodes);
        return node;
    }

    public void findFirstLevelSubgraphParents(PatternTreeNode node, List<PatternTreeNode> nodes) {
        Set<String> newPatternVertices = node.getPattern().getPattern().vertexSet().stream()
                .map(Vertex::getType)
                .collect(Collectors.toSet());

        for (PatternTreeNode otherPatternNode : nodes) {
            Set<String> otherTypes = otherPatternNode.getPattern().getPattern().vertexSet().stream()
                    .map(Vertex::getType)
                    .collect(Collectors.toSet());

            if (newPatternVertices.containsAll(otherTypes)) {
                node.getSubgraphParents().add(otherPatternNode);
            }
        }
    }

    public void findSubgraphParents(PatternTreeNode node, List<PatternTreeNode> nodes) {
        Set<String> newPatternEdges = node.getPattern().getPattern().edgeSet().stream()
                .map(RelationshipEdge::toString)
                .collect(Collectors.toSet());

        for (PatternTreeNode otherPatternNode : nodes) {
            Set<String> otherPatternEdges = otherPatternNode.getPattern().getPattern().edgeSet().stream()
                    .map(RelationshipEdge::toString)
                    .collect(Collectors.toSet());

            if (newPatternEdges.containsAll(otherPatternEdges)) {
                node.getSubgraphParents().add(otherPatternNode);
            }
        }
    }

    public void findCenterVertexParent(PatternTreeNode node, List<PatternTreeNode> nodes) {
        Set<String> newPatternEdges = node.getPattern().getPattern().edgeSet().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());

        for (PatternTreeNode otherPatternNode : nodes) {
            if (newPatternEdges.equals(otherPatternNode.getPattern().getPattern().edgeSet().stream().map(Object::toString).collect(Collectors.toSet()))
                    && otherPatternNode.getPattern().getCenterVertexType().equals(node.getPattern().getCenterVertexType())) {
                printParent(node, otherPatternNode);
                node.setCenterVertexParent(otherPatternNode);
                return;
            }
        }

        nodes.stream()
                .filter(otherPatternNode -> otherPatternNode.getPattern().getCenterVertexType().equals(node.getPattern().getCenterVertexType()))
                .findFirst()
                .ifPresent(parent -> {
                    printParent(node, parent);
                    node.setCenterVertexParent(parent);
                });
    }

    public void addMinimalDependency(PatternTreeNode node, AttributeDependency dependency) {
        if (node.getMinimalDependencies() == null) {
            node.setMinimalDependencies(new ArrayList<>());
        }
        node.getMinimalDependencies().add(dependency);
    }

    public void addMinimalConstantDependency(PatternTreeNode node, AttributeDependency dependency) {
        if (node.getMinimalConstantDependencies() == null) {
            node.setMinimalConstantDependencies(new ArrayList<>());
        }
        node.getMinimalConstantDependencies().add(dependency);
    }


    public List<AttributeDependency> getAllMinimalConstantDependenciesOnThisPath(PatternTreeNode currPatternTreeNode) {
        return getAllDependenciesOnThisPath(currPatternTreeNode, true);
    }

    public List<AttributeDependency> getAllMinimalDependenciesOnThisPath(PatternTreeNode currPatternTreeNode) {
        return getAllDependenciesOnThisPath(currPatternTreeNode, false);
    }

    private List<AttributeDependency> getAllDependenciesOnThisPath(PatternTreeNode currPatternTreeNode, boolean constant) {
        Set<AttributeDependency> dependencies = new HashSet<>();
        collectDependencies(currPatternTreeNode, dependencies, constant);
        return new ArrayList<>(dependencies);
    }

    private void collectDependencies(PatternTreeNode node, Set<AttributeDependency> dependencies, boolean constant) {
        if (constant) {
            dependencies.addAll(node.getMinimalConstantDependencies());
        } else {
            dependencies.addAll(node.getMinimalDependencies());
        }

        for (PatternTreeNode parent : node.getSubgraphParents()) {
            collectDependencies(parent, dependencies, constant);
        }
    }

    public boolean literalPathIsMissingTypesInPattern(List<ConstantLiteral> parentsPathToRoot, Set<Vertex> patternVertexSet) {
        Set<String> literalTypes = new HashSet<>();
        for (ConstantLiteral literal : parentsPathToRoot) {
            literalTypes.add(literal.getVertexType());
        }

        for (Vertex vertex : patternVertexSet) {
            if (!literalTypes.contains(vertex.getType())) {
                return true;
            }
        }

        return false;
    }

    public boolean isSuperGraphOfPrunedPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
        Set<String> newPatternEdges = newPattern.getPattern().edgeSet().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());

        for (int i = patternTree.getTree().size() - 1; i >= 0; i--) {
            for (PatternTreeNode treeNode : patternTree.getTree().get(i)) {
                if (treeNode.isPruned() && treeNode.getPattern().getCenterVertexType().equals(newPattern.getCenterVertexType())) {
                    Set<String> otherPatternEdges = treeNode.getPattern().getPattern().edgeSet().stream()
                            .map(Object::toString)
                            .collect(Collectors.toSet());

                    if (newPatternEdges.containsAll(otherPatternEdges)) {
                        System.out.println("Candidate pattern: " + newPattern + " is a supergraph of pruned subgraph pattern: " + treeNode.getPattern());
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void printParent(PatternTreeNode node, PatternTreeNode otherPatternNode) {
        logger.info("New pattern: " + node.getPattern());
        if (otherPatternNode.getPattern().getPattern().edgeSet().size() == 0) {
            logger.info("is a child of center vertex parent pattern: " + otherPatternNode.getPattern().getPattern().vertexSet());
        } else {
            logger.info("is a child of center vertex parent pattern: " + otherPatternNode.getPattern());
        }
    }

}
