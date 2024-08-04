package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class DependencyService {
    private static final Logger logger = LoggerFactory.getLogger(DependencyService.class);

    private final AppConfig config;

    @Autowired
    public DependencyService(AppConfig config) {
        this.config = config;
    }

    public boolean isSuperSetOfPath(AttributeDependency dependency, List<AttributeDependency> prunedDependencies) {
        for (AttributeDependency prunedPath : prunedDependencies) {
            if (prunedPath == null) {
//                logger.error("A pruned path in the list is null");
                continue;
            }
            if (dependency.getRhs().equals(prunedPath.getRhs()) && dependency.getLhs().containsAll(prunedPath.getLhs())) {
//                logger.info("Candidate path {} is a superset of pruned path {}", dependency, prunedPath);
                return true;
            }
        }
        return false;
    }

    public boolean isSuperSetOfPathAndSubsetOfDelta(AttributeDependency dependency, List<AttributeDependency> minimalDependenciesOnThisPath) {
        for (AttributeDependency prunedPath : minimalDependenciesOnThisPath) {
            if (dependency.getRhs().equals(prunedPath.getRhs()) && dependency.getLhs().containsAll(prunedPath.getLhs())) {
//                logger.info("Candidate path {} is a superset of pruned path {}", dependency, prunedPath);
                if (subsetOf(dependency.getDelta(), prunedPath.getDelta())) {
//                    logger.info("Candidate path delta {}\n with pruned path delta {}.", dependency.getDelta(), prunedPath.getDelta());
                    return true;
                }
            }
        }
        return false;
    }

    private boolean subsetOf(Delta delta, Delta prunedPathDelta) {
        return delta.getMin().getYears() >= prunedPathDelta.getMin().getYears() && delta.getMax().getYears() <= prunedPathDelta.getMax().getYears();
    }

//    private boolean subsetOf(Delta delta, Delta prunedPathDelta) {
//        // Extracting years and months for the minimum period comparison
//        int deltaMinYears = delta.getMin().getYears();
//        int deltaMinMonths = delta.getMin().getMonths();
//        int prunedDeltaMinYears = prunedPathDelta.getMin().getYears();
//        int prunedDeltaMinMonths = prunedPathDelta.getMin().getMonths();
//
//        // Extracting years and months for the maximum period comparison
//        int deltaMaxYears = delta.getMax().getYears();
//        int deltaMaxMonths = delta.getMax().getMonths();
//        int prunedDeltaMaxYears = prunedPathDelta.getMax().getYears();
//        int prunedDeltaMaxMonths = prunedPathDelta.getMax().getMonths();
//
//        // Combine years and months into total months for easier comparison
//        int deltaMinTotalMonths = deltaMinYears * 12 + deltaMinMonths;
//        int prunedDeltaMinTotalMonths = prunedDeltaMinYears * 12 + prunedDeltaMinMonths;
//        int deltaMaxTotalMonths = deltaMaxYears * 12 + deltaMaxMonths;
//        int prunedDeltaMaxTotalMonths = prunedDeltaMaxYears * 12 + prunedDeltaMaxMonths;
//
//        // Compare the total months for min and max
//        return deltaMinTotalMonths >= prunedDeltaMinTotalMonths && deltaMaxTotalMonths <= prunedDeltaMaxTotalMonths;
//    }

    public List<AttributeDependency> generateAllPaths(List<ConstantLiteral> activeAttributesInPattern, int limit) {
        // Step 1: Generate all unique vertex type combinations
        List<List<ConstantLiteral>> uniqueCombinations = new ArrayList<>();
        getCombinations(activeAttributesInPattern, limit, uniqueCombinations, new ArrayList<>(), 0);

        // Step 2: Create AttributeDependency from each combination
        List<AttributeDependency> dependencies = new ArrayList<>();
        for (List<ConstantLiteral> combination : uniqueCombinations) {
            dependencies.addAll(createAttributeDependencies(combination));
        }

        return dependencies;
    }

    private void getCombinations(List<ConstantLiteral> literals, int limit,
                                 List<List<ConstantLiteral>> uniqueCombinations, List<ConstantLiteral> current, int start) {
        if (current.size() == limit) {
            uniqueCombinations.add(new ArrayList<>(current));
            return;
        }

        Set<String> usedTypes = new HashSet<>(current.stream().map(ConstantLiteral::getVertexType).collect(Collectors.toList()));

        for (int i = start; i < literals.size(); i++) {
            ConstantLiteral literal = literals.get(i);
            if (!usedTypes.contains(literal.getVertexType())) {
                current.add(literal);
                getCombinations(literals, limit, uniqueCombinations, current, i + 1);
                current.remove(current.size() - 1);
            }
        }
    }

    private List<AttributeDependency> createAttributeDependencies(List<ConstantLiteral> literals) {
        List<AttributeDependency> dependencies = new ArrayList<>();
        for (int i = 0; i < literals.size(); i++) {
            List<ConstantLiteral> lhs = new ArrayList<>(literals);
            ConstantLiteral rhs = literals.get(i);
            lhs.remove(rhs);
            dependencies.add(new AttributeDependency(lhs, rhs));
        }
        return dependencies;
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
                                                                                                   List<List<Set<ConstantLiteral>>> matchesPerTimestamps) {
        long startTime = System.currentTimeMillis();

        Map<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entitiesWithRHSvalues = new HashMap<>();
        Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entitiesWithSortedRHSvalues = new HashMap<>();
        String yVertexType = dependency.getRhs().getVertexType();
        String yAttrName = dependency.getRhs().getAttrName();
        Set<ConstantLiteral> xAttributes = dependency.getLhs();

        for (int timestamp = 0; timestamp < matchesPerTimestamps.size(); timestamp++) {
            List<Set<ConstantLiteral>> matchesInOneTimeStamp = matchesPerTimestamps.get(timestamp);
            if (!matchesInOneTimeStamp.isEmpty()) {
                for (Set<ConstantLiteral> match : matchesInOneTimeStamp) {
                    if (match == null) {
                        continue;
                    }
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
//        logger.info("Time to find entities {}: {}", dependency, (endTime - startTime));

        return entitiesWithSortedRHSvalues;
    }
}
