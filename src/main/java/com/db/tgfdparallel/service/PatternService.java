package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
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

    public List<PatternTreeNode> vSpawnSinglePatternTreeNode(ProcessedHistogramData histogramData, PatternTree patternTree) {
        addLevel(patternTree);
        List<PatternTreeNode> singleNodePatternTreeNodes = new ArrayList<>();
        List<FrequencyStatistics> sortedVertexHistogram = histogramData.getSortedVertexHistogram();

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

    public void singleNodePatternInitialization(Graph<Vertex, RelationshipEdge> graph,
                                                int snapshotID,
                                                Map<String, Set<String>> vertexTypesToActiveAttributesMap,
                                                Map<String, PatternTreeNode> singlePatternTreeNodesMap,
                                                Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN,
                                                Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN,
                                                Map<Integer, List<Job>> assignedJobsBySnapshot) {
        // TODO: how should we set the diameter?
        int diameter = 2;
        Set<String> singlePatternTypes = singlePatternTreeNodesMap.keySet();
        assignedJobsBySnapshot.put(snapshotID, new ArrayList<>());

        for (PatternTreeNode ptn : singlePatternTreeNodesMap.values()) {
            for (Vertex vertex : graph.vertexSet()) {
                // 判断singleNodePattern与vertex的types是否有交集
                if (!Collections.disjoint(singlePatternTypes, vertex.getTypes())) {
                    Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, vertex, diameter);

                    Set<Set<ConstantLiteral>> matches = new HashSet<>();
                    VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = graphService.checkIsomorphism(subgraph, ptn.getPattern(), false);

                    int numOfMatchesInTimestamp = 0;
                    if (results.isomorphismExists()) {
                        numOfMatchesInTimestamp = extractMatches(results.getMappings(), matches, ptn, entityURIsByPTN.get(ptn), snapshotID, vertexTypesToActiveAttributesMap);
                    }

                    Job job = new Job(vertex, ptn);
                    job.setSubgraph(subgraph);
                    assignedJobsBySnapshot.get(snapshotID).add(job);

                    matchesPerTimestampsByPTN.get(ptn).get(snapshotID).addAll(matches);
                }
            }
        }
    }

    public int extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, Set<Set<ConstantLiteral>> matches,
                              PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp,
                              Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        int numOfMatches = 0;
        while (iterator.hasNext()) {
            GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
            Set<ConstantLiteral> literalsInMatch = new HashSet<>();
            String entityURI = extractMatch(result, patternTreeNode, literalsInMatch, vertexTypesToActiveAttributesMap);

            boolean isValidMatch = literalsInMatch.size() >= patternTreeNode.getPattern().getPattern().vertexSet().size();

            if (isValidMatch) {
                numOfMatches++;
                if (entityURI != null) {
                    List<Integer> emptyArray = new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0));
                    entityURIs.putIfAbsent(entityURI, emptyArray);
                    entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp) + 1);
                }
                matches.add(literalsInMatch);
            }
        }
        return numOfMatches;
    }

    public String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode,
                               Set<ConstantLiteral> match, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String entityURI = null;
        for (Vertex v : patternTreeNode.getPattern().getPattern().vertexSet()) {
            Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
            if (currentMatchedVertex == null) continue;
            String tempEntityURI = extractAttributes(patternTreeNode, match, currentMatchedVertex, vertexTypesToActiveAttributesMap);
            if (entityURI == null) {
                entityURI = tempEntityURI;
            }
        }
        return entityURI;
    }

    public String extractAttributes(PatternTreeNode patternTreeNode, Set<ConstantLiteral> match, Vertex currentMatchedVertex,
                                    Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String entityURI = null;
        String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
        Set<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternTreeNode.getPattern().getPattern().vertexSet(),
                true, vertexTypesToActiveAttributesMap);
        Set<String> vertexAllAttributesName = currentMatchedVertex.getAttributes().stream().map(Attribute::getAttrName).collect(Collectors.toSet());

        for (String matchedVertexType : currentMatchedVertex.getTypes()) {
            for (ConstantLiteral activeAttribute : activeAttributes) {
                if (!matchedVertexType.equals(activeAttribute.getVertexType())) continue;
                for (String matchedAttrName : vertexAllAttributesName) {
                    if (matchedVertexType.equals(centerVertexType) && matchedAttrName.equals("uri")) {
                        entityURI = currentMatchedVertex.getAttributes().stream().filter(attr -> attr.getAttrName().equals("uri")).findFirst().get().getAttrValue();
                    }
                    if (!activeAttribute.getAttrName().equals(matchedAttrName)) continue;
                    String matchedAttrValue = currentMatchedVertex.getAttributes().stream().filter(attr -> attr.getAttrName().equals(matchedAttrName)).findFirst().get().getAttrValue();
                    ConstantLiteral xLiteral = new ConstantLiteral(matchedVertexType, matchedAttrName, matchedAttrValue);
                    match.add(xLiteral);
                }
            }
        }
        return entityURI;
    }

    public Set<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI,
                                                             Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Map<String, Set<String>> patternVerticesAttributes = new HashMap<>();
        for (Vertex vertex : vertexSet) {
            for (String vertexType : vertex.getTypes()) {
                patternVerticesAttributes.put(vertexType, new HashSet<>());
                Set<String> attrNameSet = vertexTypesToActiveAttributesMap.get(vertexType);
                if (attrNameSet != null) {
                    patternVerticesAttributes.get(vertexType).addAll(attrNameSet);
                }
            }
        }
        Set<ConstantLiteral> literals = new HashSet<>();
        for (String vertexType : patternVerticesAttributes.keySet()) {
            if (considerURI) literals.add(new ConstantLiteral(vertexType, "uri", null));
            for (String attrName : patternVerticesAttributes.get(vertexType)) {
                ConstantLiteral literal = new ConstantLiteral(vertexType, attrName, null);
                literals.add(literal);
            }
        }
        return literals;
    }

    public void calculateTotalSupport(Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN,
                                      Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN,
                                      Map<String, Integer> vertexHistogram) {
        for (PatternTreeNode ptn : matchesPerTimestampsByPTN.keySet()) {
            int numberOfMatchesFound = matchesPerTimestampsByPTN.get(ptn).stream()
                    .mapToInt(Set::size)
                    .sum();
            System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);

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
        if (numerator > denominator)
            throw new IllegalArgumentException("numerator > denominator");
        return numerator / denominator;
    }

    public List<VSpawnPattern> vSpawnGenerator(Map<String, Set<String>> vertexTypesToActiveAttributesMap, List<String> edgeData, PatternTree patternTree, int level) {
        List<VSpawnPattern> vSpawnPatternList = new ArrayList<>();
        List<PatternTreeNode> nodes = patternTree.getTree().get(level);

        // TODO: Set pattern Pruned?
        for (PatternTreeNode ptn : nodes) {
            for (String edge : edgeData) {
                VSpawnPattern pattern = null;
                // TODO: if ptn is pruned, we skip it.
                String sourceVertexType = edge.split(" ")[0];
                String targetVertexType = edge.split(" ")[2];
                String label = edge.split(" ")[1];

                if (sourceVertexType.equals(targetVertexType)) {
                    logger.info("Source vertex type is equal to target vertex type. Skipping edge: " + edge);
                    continue;
                }

                if (vertexTypesToActiveAttributesMap.get(targetVertexType).size() == 0) {
                    logger.info("Target vertex type has no active attributes. Skipping edge: " + edge);
                    continue;
                }

                if (isDuplicateEdge(ptn.getPattern(), label, sourceVertexType, targetVertexType)) {
                    logger.info("Duplicate edge. Skipping edge: " + edge);
                    continue;
                }

                if (isMultipleEdge(ptn.getPattern(), sourceVertexType, targetVertexType)) {
                    logger.info("Multiple edge. Skipping edge: " + edge);
                    continue;
                }

                Vertex sourceVertex = isDuplicateVertex(ptn.getPattern(), sourceVertexType);
                Vertex targetVertex = isDuplicateVertex(ptn.getPattern(), targetVertexType);
                if (sourceVertex == null && targetVertex == null) {
                    logger.info("Source and target vertices are not in the pattern. Skipping edge: " + edge);
                    continue;
                }

                for (Vertex v : ptn.getPattern().getPattern().vertexSet()) {
                    logger.info("Looking to add candidate edge to vertex: " + v.getTypes());
                    if (v.isMarked()) {
                        logger.info("Vertex is marked. Skipping edge: " + edge);
                        continue;
                    }

                    if (!sourceVertexType.equals(v.getTypes()) && !targetVertexType.equals(v.getTypes())) {
                        logger.info("Vertex is not source or target. Skipping edge: " + edge);
                        continue;
                    }

                    pattern.setOldPattern(ptn);
                    Graph<Vertex, RelationshipEdge> graph = ptn.getPattern().getPattern();
                    VF2PatternGraph newPattern = copyGraph(graph);
                    if (targetVertex == null) {
                        targetVertex = new Vertex(targetVertexType);
                        addVertex(newPattern, targetVertex);
                    } else {
                        // TODO: 这步的意义？
                        for (Vertex vertex : newPattern.getPattern().vertexSet()) {
                            if (vertex.getTypes().contains(targetVertexType)) {
                                targetVertex.setMarked(true);
                                break;
                            }
                        }
                    }

                    RelationshipEdge newEdge = new RelationshipEdge(label);

                    if (sourceVertex == null) {
                        sourceVertex = new Vertex(sourceVertexType);
                        addVertex(newPattern, sourceVertex);
                    } else {
                        for (Vertex vertex : newPattern.getPattern().vertexSet()) {
                            if (vertex.getTypes().contains(sourceVertexType)) {
                                sourceVertex.setMarked(true);
                                break;
                            }
                        }
                    }

                    addEdge(graph, sourceVertex, targetVertex, newEdge);

                    // TODO: 待优化：比较new pattern与之前pattern是否isomorphism，判断是否有存在必要
//                    if (!isIsomorphicPattern(newPattern, Util.patternTree)) {
//                        pv.setMarked(true);
//                        System.out.println("Skip. Candidate pattern is an isomorph of existing pattern");
//                        continue;
//                    }

                    // TODO: Mark这里的用处

                    PatternTreeNode patternTreeNode = initializeNewNode(newPattern, ptn, edge, nodes);
                    pattern.setNewPattern(patternTreeNode);
                    vSpawnPatternList.add(pattern);
                }
            }
        }

        return vSpawnPatternList;
    }

    public boolean isDuplicateEdge(VF2PatternGraph pattern, String edgeType, String sourceType, String targetType) {
        for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
            if (edge.getLabel().equalsIgnoreCase(edgeType)) {
                if (edge.getSource().getTypes().contains(sourceType) && edge.getTarget().getTypes().contains(targetType)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isMultipleEdge(VF2PatternGraph pattern, String sourceType, String targetType) {
        for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
            if (edge.getSource().getTypes().contains(sourceType) && edge.getTarget().getTypes().contains(targetType)) {
                return true;
            } else if (edge.getSource().getTypes().contains(targetType) && edge.getTarget().getTypes().contains(sourceType)) {
                return true;
            }
        }
        return false;
    }

    private Vertex isDuplicateVertex(VF2PatternGraph newPattern, String vertexType) {
        return (Vertex) newPattern.getPattern().vertexSet().stream()
                .filter(v -> v.getTypes().contains(vertexType))
                .findFirst()
                .orElse(null);
    }

    public VF2PatternGraph copyGraph(Graph<Vertex, RelationshipEdge> graph) {
        VF2PatternGraph newPattern = new VF2PatternGraph();
        Map<Vertex, Vertex> vertexMap = new HashMap<>();

        // Copy vertices and create a mapping from the original vertices to the new ones
        for (Vertex v : graph.vertexSet()) {
//            Vertex newVertex = v.copy();
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

    public PatternTreeNode initializeNewNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String candidateEdgeString, List<PatternTreeNode> nodes) {
        PatternTreeNode node = new PatternTreeNode(pattern, parentNode, candidateEdgeString);
        findSubgraphParents(node, nodes);
        findCenterVertexParent(node, nodes);
        return node;
    }

    public void findSubgraphParents(PatternTreeNode node, List<PatternTreeNode> nodes) {
        // TODO: 有没有必要给level0 赋subgraph?
        List<String> newPatternEdges = node.getPattern().getPattern().edgeSet().stream()
                .map(RelationshipEdge::toString)
                .collect(Collectors.toList());

        for (PatternTreeNode otherPatternNode : nodes) {
            List<String> otherPatternEdges = otherPatternNode.getPattern().getPattern().edgeSet().stream()
                    .map(RelationshipEdge::toString)
                    .collect(Collectors.toList());

            if (newPatternEdges.containsAll(otherPatternEdges)) {
                StringBuilder sb = new StringBuilder("New pattern: ")
                        .append(node.getPattern())
                        .append(" is a child of subgraph parent pattern: ");
                if (otherPatternNode.getPattern().getPattern().edgeSet().isEmpty()) {
                    sb.append(otherPatternNode.getPattern().getPattern().vertexSet());
                } else {
                    sb.append(otherPatternNode.getPattern());
                }
                logger.info(sb.toString());
                node.getSubgraphParents().add(otherPatternNode);
            }
        }
    }

    public void findCenterVertexParent(PatternTreeNode node, List<PatternTreeNode> nodes) {
        System.out.println("Finding center vertex parent...");
        Set<String> newPatternEdges = node.getPattern().getPattern().edgeSet().stream().map(Object::toString).collect(Collectors.toSet());
        List<PatternTreeNode> almostParents = new ArrayList<>();
        for (PatternTreeNode otherPatternNode : nodes) {
            Set<String> otherPatternEdges = otherPatternNode.getPattern().getPattern().edgeSet().stream().map(Object::toString).collect(Collectors.toSet());
            if (newPatternEdges.containsAll(otherPatternEdges)) {
                almostParents.add(otherPatternNode);
                if (otherPatternNode.getPattern().getCenterVertexType().equals(node.getPattern().getCenterVertexType())) {
                    printParent(node, otherPatternNode);
                    node.setCenterVertexParent(otherPatternNode);
                    return;
                }
            }
        }
//        if (node.getCenterVertexParent() == null) {
//            for (PatternTreeNode otherPatternNode : this.getTree().get(0)) {
//                if (otherPatternNode.getPattern().getCenterVertexType().equals(node.getPattern().getCenterVertexType())) {
//                    printParent(node, otherPatternNode);
//                    node.setCenterVertexParent(otherPatternNode);
//                    return;
//                }
//            }
//        }
    }

    private void printParent(PatternTreeNode node, PatternTreeNode otherPatternNode) {
        System.out.println("New pattern: " + node.getPattern());
        if (otherPatternNode.getPattern().getPattern().edgeSet().size() == 0) {
            System.out.println("is a child of center vertex parent pattern: " + otherPatternNode.getPattern().getPattern().vertexSet());
        } else {
            System.out.println("is a child of center vertex parent pattern: " + otherPatternNode.getPattern());
        }
    }

}
