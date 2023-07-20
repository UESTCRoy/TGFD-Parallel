package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class FastMatchService {
    private Graph<Vertex, RelationshipEdge> graph;
    private int level;
    private VF2PatternGraph pattern;
    private Map<String, Set<String>> vertexTypesToActiveAttributesMap;
    private int timestamp;
    private PatternTreeNode ptn;
    private Set<Set<ConstantLiteral>> matches;
    private Map<String, List<Integer>> entityURIs;
    private Vertex centerNode;
    private final AppConfig config;
    private final GraphService graphService;
    public FastMatchService(GraphService graphService, AppConfig config) {
        this.graphService = graphService;
        this.config = config;
    }

    public void init(Graph<Vertex, RelationshipEdge> graph, int level, VF2PatternGraph pattern, Map<String, Set<String>> vertexTypesToActiveAttributesMap, int timestamp, PatternTreeNode ptn, Map<String, List<Integer>> entityURIs, Vertex centerNode){
        this.graph = graph;
        this.level = level;
        this.pattern = pattern;
        this.vertexTypesToActiveAttributesMap = vertexTypesToActiveAttributesMap;
        this.timestamp = timestamp;
        this.ptn = ptn;
        this.matches = new HashSet<>();
        this.entityURIs = entityURIs;
        this.centerNode = centerNode;
    }

    public Set<Set<ConstantLiteral>> getMatches() {
        return matches;
    }

    public int getNumOfMatches() {
        return matches.size();
    }

    public void findMatches() {
        if (this.pattern.getPatternType() == PatternType.SingleNode)
            findAllMatchesOfK0patternInSnapshotUsingCenterVertices();
        else
            findMatchesUsingEntityURIs();
    }

    private void findAllMatchesOfK0patternInSnapshotUsingCenterVertices() {
        String patternVertexType = this.pattern.getCenterVertexType();
        for (Vertex v : this.graph.vertexSet()) {
            if (v.getTypes().contains(patternVertexType)) {
                Set<ConstantLiteral> match = new HashSet<>();
                String entityURI = extractAttributes(ptn,match,v,this.vertexTypesToActiveAttributesMap);
                if (match.size() < this.pattern.getPattern().vertexSet().size()) continue;
                if (entityURI != null) {
                    List<Integer> emptyArray = new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0));
                    this.entityURIs.putIfAbsent(entityURI, emptyArray);
                    this.entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp) + 1);
                }
                this.matches.add(match);
            }
        }
    }

    private void findMatchesUsingEntityURIs() {
//        Map<String, List<Integer>> existingEntityURIs = getExistingEntityURIs(graph, t);
//        for (Map.Entry<String, List<Integer>> entry: existingEntityURIs.entrySet()) {
//            if (entry.getValue().get(t) > 0) {
//                String centerVertexUri = entry.getKey();
//                Vertex centerVertex = graph.getGraph().getNode(centerVertexUri);
//                findMatchesAroundThisCenterVertex(centerVertex);
//            }
//        }
        findMatchesAroundThisCenterVertex(this.centerNode);
    }//TODO: figure out here

    private void findMatchesAroundThisCenterVertex(Vertex centerVertex) {
        PatternType patternType = this.pattern.getPatternType();
        switch (patternType) {
            case SingleEdge: findAllMatchesOfK1patternInSnapshotUsingCenterVertex(centerVertex);  break;
            case DoubleEdge: findAllMatchesOfK2PatternInSnapshotUsingCenterVertex(centerVertex); break;
            case Star: findAllMatchesOfStarPatternInSnapshotUsingCenterVertex(centerVertex); break;
            case Line: findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(centerVertex, false);  break;
            case Circle: findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(centerVertex, true); break;
            case Complex: findAllMatchesOfPatternInThisSnapshotUsingCenterVertex(centerVertex); break;
            default: throw new IllegalArgumentException("Unrecognized pattern type");
        }
    }

    private void findAllMatchesOfPatternInThisSnapshotUsingCenterVertex(Vertex centerVertex) {
        Set<String> validTypes = this.pattern.getPattern().vertexSet().stream().flatMap(v -> v.getTypes().stream()).collect(Collectors.toSet());
        Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(this.graph, centerVertex, this.level, validTypes);
        VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = graphService.checkIsomorphism(subgraph, this.pattern, false);
        if (results.isomorphismExists())
            extractMatches(results.getMappings());
    }

    private void findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(Vertex startDataVertex, boolean isCyclic) {
        Vertex startVertex = this.pattern.getCenterVertex();
        String startVertexType = startVertex.getTypes().iterator().next();

        Vertex currentPatternVertex = startVertex;
        Set<RelationshipEdge> visited = new HashSet<>();
        List<RelationshipEdge> patternEdgePath = new ArrayList<>();
        System.out.println("Pattern edge path:");
        while (visited.size() < this.pattern.getPattern().edgeSet().size()) {
            for (RelationshipEdge patternEdge : this.pattern.getPattern().edgesOf(currentPatternVertex)) {
                if (!visited.contains(patternEdge)) {
                    boolean outgoing = patternEdge.getSource().equals(currentPatternVertex);
                    currentPatternVertex = outgoing ? patternEdge.getTarget() : patternEdge.getSource();
                    patternEdgePath.add(patternEdge);
                    System.out.println(patternEdge);
                    visited.add(patternEdge);
                    if (isCyclic) break;
                }
            }
        }

        MappingTree mappingTree = new MappingTree();
        mappingTree.addLevel();
        System.out.println("Added level to MappingTree. MappingTree levels: " + mappingTree.getTree().size());
        MappingTreeNode rootMappingTreeNode = new MappingTreeNode(startDataVertex, startVertexType, null);
        mappingTree.createNodeAtLevel(0, rootMappingTreeNode);
        System.out.println("Added node to MappingTree level "+mappingTree.getTree().size()+".");
        System.out.println("MappingTree level "+mappingTree.getTree().size()+" size = "+mappingTree.getTree().get(0).size());

        for (int index = 0; index < this.pattern.getPattern().edgeSet().size(); index++) {
            String currentPatternEdgeLabel = patternEdgePath.get(index).getLabel();
            Vertex currentPatternSourceVertex = patternEdgePath.get(index).getSource();
            String currentPatternSourceVertexLabel = currentPatternSourceVertex.getTypes().iterator().next();
            Vertex currentPatternTargetVertex = patternEdgePath.get(index).getTarget();
            String currentPatternTargetVertexLabel = currentPatternTargetVertex.getTypes().iterator().next();
            if (mappingTree.getLevel(index).size() == 0) {
                break;
            } else {
                mappingTree.addLevel();
            }
            for (MappingTreeNode currentMappingTreeNode: mappingTree.getLevel(index)) {
                Vertex currentDataVertex = currentMappingTreeNode.getDataVertex();
                for (RelationshipEdge dataEdge : this.graph.outgoingEdgesOf(currentDataVertex)) {
                    if (dataEdge.getLabel().equals(currentPatternEdgeLabel)
                            && dataEdge.getSource().getTypes().contains(currentPatternSourceVertexLabel)
                            && dataEdge.getTarget().getTypes().contains(currentPatternTargetVertexLabel)) {
                        Vertex otherVertex = dataEdge.getTarget();
                        MappingTreeNode newMappingTreeNode = new MappingTreeNode(otherVertex, currentPatternTargetVertexLabel, currentMappingTreeNode);
                        if (isCyclic && index == this.pattern.getPattern().edgeSet().size()-1) {
                            if (!newMappingTreeNode.getDataVertex().getUri().equals(startDataVertex.getUri())) {
                                newMappingTreeNode.setPruned(true);
                            }
                        }
                        mappingTree.createNodeAtLevel(index+1, newMappingTreeNode);
                        System.out.println("Added node to MappingTree level "+mappingTree.getTree().size()+".");
                        System.out.println("MappingTree level "+mappingTree.getTree().size()+" size = "+mappingTree.getTree().get(index+1).size());
                    }
                }
                for (RelationshipEdge dataEdge : this.graph.incomingEdgesOf(currentDataVertex)) {
                    if (dataEdge.getLabel().equals(currentPatternEdgeLabel)
                            && dataEdge.getSource().getTypes().contains(currentPatternSourceVertexLabel)
                            && dataEdge.getTarget().getTypes().contains(currentPatternTargetVertexLabel)) {
                        Vertex otherVertex = dataEdge.getSource();
                        MappingTreeNode newMappingTreeNode = new MappingTreeNode(otherVertex, currentPatternSourceVertexLabel, currentMappingTreeNode);
                        if (isCyclic && index == this.pattern.getPattern().edgeSet().size()-1) {
                            if (!newMappingTreeNode.getDataVertex().getUri().equals(startDataVertex.getUri())) {
                                newMappingTreeNode.setPruned(true);
                            }
                        }
                        mappingTree.createNodeAtLevel(index+1, newMappingTreeNode);
                        System.out.println("Added node to MappingTree level "+mappingTree.getTree().size()+".");
                        System.out.println("MappingTree level "+mappingTree.getTree().size()+" size = "+mappingTree.getTree().get(index+1).size());
                    }
                }
            }
        }

        if (mappingTree.getTree().size() == this.pattern.getPattern().vertexSet().size()) {
            extractMatchesFromMappingTree(startDataVertex, mappingTree);
        }
    }

    private void findAllMatchesOfStarPatternInSnapshotUsingCenterVertex(Vertex centerDataVertex) {
        Map<Vertex, Set<Vertex>> patternVertexToDataVerticesMap = getPatternVertexToDataVerticesMap(centerDataVertex);
        ArrayList<Map.Entry<Vertex, Set<Vertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());
        MappingTree mappingTree = new MappingTree();

        for (int i = 0; i < patternVertexToDataVerticesMap.keySet().size(); i++) {
            if (i == 0) {
                mappingTree.addLevel();
                for (Vertex dv: vertexSets.get(i).getValue()) {
                    mappingTree.createNodeAtLevel(i, dv, vertexSets.get(i).getKey().getTypes().iterator().next(), null);
                }
            } else {
                List<MappingTreeNode> mappingTreeNodeList = mappingTree.getLevel(i-1);
                mappingTree.addLevel();
                for (MappingTreeNode parentNode: mappingTreeNodeList) {
                    for (Vertex dv: vertexSets.get(i).getValue()) {
                        mappingTree.createNodeAtLevel(i, dv, vertexSets.get(i).getKey().getTypes().iterator().next(), parentNode);
                    }
                }
            }
        }
        extractMatchesFromMappingTree(centerDataVertex, mappingTree);
    }

    private void extractMatchesFromMappingTree(Vertex centerDataVertex, MappingTree mappingTree) {
        Set<Vertex> patternVertexSet = this.pattern.getPattern().vertexSet();
        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(centerDataVertex);
        Set<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(this.pattern.getPattern().vertexSet(), true, this.vertexTypesToActiveAttributesMap);
        for (MappingTreeNode leafNode: mappingTree.getLevel(mappingTree.getTree().size()-1).stream().filter(mappingTreeNode -> !mappingTreeNode.isPruned()).collect(Collectors.toList())) {
            Set<MappingTreeNode> mapping = leafNode.getPathToRoot();
            HashSet<ConstantLiteral> match = new HashSet<>(centerDataVertexLiterals);
            for (MappingTreeNode mappingTreeNode: mapping) {
                Vertex dv = mappingTreeNode.getDataVertex();
                String patternVertexType = mappingTreeNode.getPatternVertexType();
                for (Attribute matchedAttr: dv.getAttributes()) {
                    for (ConstantLiteral activeAttribute : activeAttributes) {
                        if (!patternVertexType.equals(activeAttribute.getVertexType())) continue;
                        if (!matchedAttr.getAttrName().equals(activeAttribute.getAttrName())) continue;
                        ConstantLiteral literal = new ConstantLiteral(patternVertexType, matchedAttr.getAttrName(), matchedAttr.getAttrValue());
                        match.add(literal);
                    }
                }
            }
            if (match.size() <= patternVertexSet.size()) continue;
            this.matches.add(match);
        }
        String entityURI = centerDataVertex.getUri();
        if (this.matches.size() > 0) { // equivalent to entityURI != null
            List<Integer> emptyArray = new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0));
            this.entityURIs.putIfAbsent(entityURI, emptyArray);
            this.entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp) + this.matches.size());
        }
    }

    private void findAllMatchesOfK1patternInSnapshotUsingCenterVertex(Vertex dataVertex) {
        String centerVertexType = this.pattern.getCenterVertexType();
        Set<String> edgeLabels = this.pattern.getPattern().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
        String sourceType = this.pattern.getPattern().edgeSet().iterator().next().getSource().getTypes().iterator().next();
        String targetType = this.pattern.getPattern().edgeSet().iterator().next().getTarget().getTypes().iterator().next();
        Set<RelationshipEdge> edgeSet;
        if (centerVertexType.equals(sourceType)) {
            edgeSet = this.graph.outgoingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getTarget().getTypes().contains(targetType)).collect(Collectors.toSet());
        } else {
            edgeSet = this.graph.incomingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getSource().getTypes().contains(sourceType)).collect(Collectors.toSet());
        }
        extractMatches(edgeSet);
    }

    // Use this for k=2 instead of findAllMatchesOfStarPattern to avoid overhead of creating a MappingTree
    private void findAllMatchesOfK2PatternInSnapshotUsingCenterVertex(Vertex dataVertex) {
        Set<Vertex> patternVertexSet = this.pattern.getPattern().vertexSet();
        Map<Vertex, Set<Vertex>> patternVertexToDataVerticesMap = getPatternVertexToDataVerticesMap(dataVertex);
        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(dataVertex);
        ArrayList<Map.Entry<Vertex, Set<Vertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());
        Set<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(this.pattern.getPattern().vertexSet(), true, this.vertexTypesToActiveAttributesMap);

        Map.Entry<Vertex, Set<Vertex>> nonCenterVertexEntry1 = vertexSets.get(0);
        String nonCenterPatternVertex1Type = nonCenterVertexEntry1.getKey().getTypes().iterator().next();
        for (Vertex nonCenterDataVertex1: nonCenterVertexEntry1.getValue()) {
            Map.Entry<Vertex, Set<Vertex>> nonCenterVertexEntry2 = vertexSets.get(1);
            String nonCenterPatternVertex2Type = nonCenterVertexEntry2.getKey().getTypes().iterator().next();
            for (Vertex nonCenterDataVertex2: nonCenterVertexEntry2.getValue()) {
                HashSet<ConstantLiteral> match = new HashSet<>();
                for (Attribute matchedAttr: nonCenterDataVertex1.getAttributes()) {
                    for (ConstantLiteral activeAttribute : activeAttributes) {
                        if (!nonCenterPatternVertex1Type.equals(activeAttribute.getVertexType())) continue;
                        if (!matchedAttr.getAttrName().equals(activeAttribute.getAttrName())) continue;
                        ConstantLiteral literal = new ConstantLiteral(nonCenterPatternVertex1Type, matchedAttr.getAttrName(), matchedAttr.getAttrValue());
                        match.add(literal);
                    }
                }
                for (Attribute matchedAttr: nonCenterDataVertex2.getAttributes()) {
                    for (ConstantLiteral activeAttribute : activeAttributes) {
                        if (!nonCenterPatternVertex2Type.equals(activeAttribute.getVertexType())) continue;
                        if (!matchedAttr.getAttrName().equals(activeAttribute.getAttrName())) continue;
                        ConstantLiteral literal = new ConstantLiteral(nonCenterPatternVertex2Type, matchedAttr.getAttrName(), matchedAttr.getAttrValue());
                        match.add(literal);
                    }
                }
                match.addAll(centerDataVertexLiterals);
                if (match.size() <= patternVertexSet.size()) continue;
                this.matches.add(match);
            }
        }
        String entityURI = dataVertex.getUri();
        if (this.matches.size() > 0) { // equivalent to entityURI != null
            List<Integer> emptyArray = new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0));
            this.entityURIs.putIfAbsent(entityURI, emptyArray);
            this.entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp) + this.matches.size());
        }
    }

    private Map<Vertex, Set<Vertex>> getPatternVertexToDataVerticesMap(Vertex dataVertex) {
        Map<Vertex, Set<Vertex>> patternVertexToDataVerticesMap = new HashMap<>();
        Vertex centerPatternVertex = this.pattern.getCenterVertex();
        for (RelationshipEdge patternEdge: this.pattern.getPattern().incomingEdgesOf(centerPatternVertex)) {
            Vertex nonCenterPatternVertex = patternEdge.getSource();
            patternVertexToDataVerticesMap.put(nonCenterPatternVertex, new HashSet<>());
            for (RelationshipEdge dataEdge: this.graph.incomingEdgesOf(dataVertex)) {
                if (dataEdge.getLabel().equals(patternEdge.getLabel())
                        && dataEdge.getSource().getTypes().contains(nonCenterPatternVertex.getTypes().iterator().next())) {
                    patternVertexToDataVerticesMap.get(nonCenterPatternVertex).add(dataEdge.getSource());
                }
            }
        }
        for (RelationshipEdge patternEdge: this.pattern.getPattern().outgoingEdgesOf(centerPatternVertex)) {
            Vertex nonCenterPatternVertex = patternEdge.getTarget();
            patternVertexToDataVerticesMap.put(nonCenterPatternVertex, new HashSet<>());
            for (RelationshipEdge dataEdge: this.graph.outgoingEdgesOf(dataVertex)) {
                if (dataEdge.getLabel().equals(patternEdge.getLabel())
                        && dataEdge.getTarget().getTypes().contains(nonCenterPatternVertex.getTypes().iterator().next())) {
                    patternVertexToDataVerticesMap.get(nonCenterPatternVertex).add(dataEdge.getTarget());
                }
            }
        }
        return patternVertexToDataVerticesMap;
    }

    private Set<ConstantLiteral> getCenterDataVertexLiterals(Vertex dataVertex) {
        Set<ConstantLiteral> centerDataVertexLiterals = new HashSet<>();
        String centerVertexType = this.pattern.getCenterVertexType();
        Set<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(this.pattern.getPattern().vertexSet(), true, this.vertexTypesToActiveAttributesMap);
        for (Attribute matchedAttr: dataVertex.getAttributes()) {
            for (ConstantLiteral activeAttribute : activeAttributes) {
                if (!centerVertexType.equals(activeAttribute.getVertexType())) continue;
                if (!matchedAttr.getAttrName().equals(activeAttribute.getAttrName())) continue;
                ConstantLiteral literal = new ConstantLiteral(centerVertexType, matchedAttr.getAttrName(), matchedAttr.getAttrValue());
                centerDataVertexLiterals.add(literal);
            }
        }
        return centerDataVertexLiterals;
    }

    private void extractMatches(Set<RelationshipEdge> edgeSet) {
        String patternEdgeLabel = this.pattern.getPattern().edgeSet().iterator().next().getLabel();
        String sourceVertexType = this.pattern.getPattern().edgeSet().iterator().next().getSource().getTypes().iterator().next();
        String targetVertexType = this.pattern.getPattern().edgeSet().iterator().next().getTarget().getTypes().iterator().next();
        for (RelationshipEdge edge: edgeSet) {
            String matchedEdgeLabel = edge.getLabel();
            Set<String> matchedSourceVertexType = edge.getSource().getTypes();
            Set<String> matchedTargetVertexType = edge.getTarget().getTypes();
            if (matchedEdgeLabel.equals(patternEdgeLabel) && matchedSourceVertexType.contains(sourceVertexType) && matchedTargetVertexType.contains(targetVertexType)) {
                Set<ConstantLiteral> literalsInMatch = new HashSet<>();
                String entityURI = extractMatch(edge.getSource(), sourceVertexType, edge.getTarget(), targetVertexType, literalsInMatch);
                if (literalsInMatch.size() < this.pattern.getPattern().vertexSet().size()) continue;
                if (entityURI != null) {
                    List<Integer> emptyArray = new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0));
                    this.entityURIs.putIfAbsent(entityURI, emptyArray);
                    this.entityURIs.get(entityURI).set(this.timestamp, this.entityURIs.get(entityURI).get(this.timestamp) + 1);
                }
                this.matches.add(literalsInMatch);
            }
        }
    }

    private String extractMatch(Vertex currentSourceVertex, String sourceVertexType, Vertex currentTargetVertex, String targetVertexType, Set<ConstantLiteral> match) {
        String entityURI = null;
        List<String> patternVerticesTypes = Arrays.asList(sourceVertexType, targetVertexType);
        List<Vertex> vertices = Arrays.asList(currentSourceVertex, currentTargetVertex);
        for (int index = 0; index < vertices.size(); index++) {
            Vertex currentMatchedVertex = vertices.get(index);
            String patternVertexType = patternVerticesTypes.get(index);
            if (entityURI == null) {
                entityURI = extractAttributes(patternVertexType, match, currentMatchedVertex);
            } else {
                extractAttributes(patternVertexType, match, currentMatchedVertex);
            }
        }
        return entityURI;
    }

    private void extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator) {
        while (iterator.hasNext()) {
            GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
            Set<ConstantLiteral> literalsInMatch = new HashSet<>();
            String entityURI = extractMatch(result, this.ptn, literalsInMatch, this.vertexTypesToActiveAttributesMap);
            if (literalsInMatch.size() < this.pattern.getPattern().vertexSet().size()) continue;
            if (entityURI != null) {
                List<Integer> emptyArray = new ArrayList<>(Collections.nCopies(config.getTimestamp(), 0));
                this.entityURIs.putIfAbsent(entityURI, emptyArray);
                this.entityURIs.get(entityURI).set(this.timestamp, this.entityURIs.get(entityURI).get(this.timestamp) + 1);
            }
            this.matches.add(literalsInMatch);
        }
    }

    private String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode, Set<ConstantLiteral> match, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String entityURI = null;
        for (Vertex v : this.pattern.getPattern().vertexSet()) {
            Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
            if (currentMatchedVertex != null) {
                String tempEntityURI = extractAttributes(patternTreeNode, match, currentMatchedVertex, vertexTypesToActiveAttributesMap);
                if (entityURI == null && tempEntityURI != null) {
                    entityURI = tempEntityURI;
                } else if (entityURI == null) {
                    entityURI = currentMatchedVertex.getUri();
                }
            }
        }
        return entityURI;
    }

    public String extractAttributes(PatternTreeNode patternTreeNode, Set<ConstantLiteral> match, Vertex currentMatchedVertex, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        String entityURI = null;
        String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
        Set<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternTreeNode.getPattern().getPattern().vertexSet(), true, vertexTypesToActiveAttributesMap);
        Map<String, Attribute> vertexAllAttributesMap = currentMatchedVertex.getAttributes().stream().collect(Collectors.toMap(Attribute::getAttrName, Function.identity()));
        for (String matchedVertexType : currentMatchedVertex.getTypes()) {
            for (ConstantLiteral activeAttribute : activeAttributes) {
                if (!matchedVertexType.equals(activeAttribute.getVertexType())) continue;
                Attribute matchedAttribute = vertexAllAttributesMap.getOrDefault(activeAttribute.getAttrName(), null);
                if (matchedAttribute == null) continue;
                if (matchedVertexType.equals(centerVertexType) && matchedAttribute.getAttrName().equals("uri")) {
                    entityURI = matchedAttribute.getAttrValue();
                }
                ConstantLiteral xLiteral = new ConstantLiteral(matchedVertexType, activeAttribute.getAttrName(), matchedAttribute.getAttrValue());
                match.add(xLiteral);
            }
        }
        return entityURI;
    }

    private String extractAttributes(String patternVertexType, Set<ConstantLiteral> match, Vertex currentMatchedVertex) {
        String entityURI = null;
        String centerVertexType = this.pattern.getCenterVertexType();
        Set<String> matchedVertexTypes = currentMatchedVertex.getTypes();
        Set<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(this.pattern.getPattern().vertexSet(), true, this.vertexTypesToActiveAttributesMap);
        for (ConstantLiteral activeAttribute : activeAttributes) {
            if (!matchedVertexTypes.contains(activeAttribute.getVertexType())) continue;
            for (Attribute matchedAttr : currentMatchedVertex.getAttributes()) {
                if (!activeAttribute.getAttrName().equals(matchedAttr.getAttrName())) continue;
                if (matchedVertexTypes.contains(centerVertexType) && matchedAttr.getAttrName().equals("uri")) {
                    entityURI = matchedAttr.getAttrValue();
                }
                ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttr.getAttrName(), matchedAttr.getAttrValue());
                match.add(xLiteral);
            }
        }
        return entityURI;
    }

    public Set<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Map<String, Set<String>> patternVerticesAttributes = new HashMap<>();

        for (Vertex vertex : vertexSet) {
            for (String vertexType : vertex.getTypes()) {
                Set<String> attrNameSet = vertexTypesToActiveAttributesMap.getOrDefault(vertexType, new HashSet<>());
                patternVerticesAttributes.putIfAbsent(vertexType, new HashSet<>(attrNameSet));
            }
        }

        Set<ConstantLiteral> literals = new HashSet<>();

        for (Map.Entry<String, Set<String>> entry : patternVerticesAttributes.entrySet()) {
            String vertexType = entry.getKey();
            if (considerURI) literals.add(new ConstantLiteral(vertexType, "uri", null));

            for (String attrName : entry.getValue()) {
                literals.add(new ConstantLiteral(vertexType, attrName, null));
            }
        }
        return literals;
    }
}
