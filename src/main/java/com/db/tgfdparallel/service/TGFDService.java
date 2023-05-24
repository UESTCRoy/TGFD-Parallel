package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Period;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class TGFDService {
    private static final Logger logger = LoggerFactory.getLogger(TGFDService.class);
    private final AppConfig config;
    private final PatternService patternService;

    @Autowired
    public TGFDService(AppConfig config, PatternService patternService) {
        this.config = config;
        this.patternService = patternService;
    }

    public List<TGFD> discoverConstantTGFD(PatternTreeNode patternNode, ConstantLiteral yLiteral,
                                           Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entities,
                                           Map<Pair, List<TreeSet<Pair>>> deltaToPairsMap) {
        List<TGFD> tgfds = new ArrayList<>();

        String yVertexType = yLiteral.getVertexType();
        String yAttrName = yLiteral.getAttrName();

        for (Map.Entry<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
            VF2PatternGraph newPattern = patternService.copyGraph(patternNode.getPattern().getPattern());
            DataDependency newDependency = new DataDependency();
            AttributeDependency constantPath = new AttributeDependency();
            String attrValue = entityEntry.getValue().get(0).getKey().getAttrValue();

            Map<String, ConstantLiteral> vertexTypeToLiteral = new HashMap<>();
            for (ConstantLiteral xLiteral : entityEntry.getKey()) {
                vertexTypeToLiteral.put(xLiteral.getVertexType(), xLiteral);
            }

            for (Vertex v : newPattern.getPattern().vertexSet()) {
                String vType = v.getTypes();
                if (vType.equalsIgnoreCase(yVertexType)) {
                    v.getAttributes().add(new Attribute(yAttrName));
                    if (newDependency.getY().size() == 0) {
                        ConstantLiteral newY = new ConstantLiteral(yVertexType, yAttrName, attrValue);
                        newDependency.getY().add(newY);
                    }
                }

                ConstantLiteral xLiteral = vertexTypeToLiteral.get(vType);
                if (xLiteral != null) {
                    v.getAttributes().add(new Attribute(xLiteral.getAttrName(), xLiteral.getAttrValue()));
                    ConstantLiteral newXLiteral = new ConstantLiteral(vType, xLiteral.getAttrName(), xLiteral.getAttrValue());
                    newDependency.getX().add(newXLiteral);
                    constantPath.getLhs().add(newXLiteral);
                }
            }
            constantPath.setRhs(new ConstantLiteral(yVertexType, yAttrName, attrValue));

            List<Map.Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq = entityEntry.getValue();
            List<Pair> candidateDeltas = new ArrayList<>();

            // 处理Positive与Negative的情况
            if (rhsAttrValuesTimestampsSortedByFreq.size() == 1) {

            } else if (rhsAttrValuesTimestampsSortedByFreq.size() > 1) {

            }

            // Compute TGFD support
            Delta candidateTGFDdelta;
            double candidateTGFDsupport = 0;
            Pair mostSupportedDelta = null;
            TreeSet<Pair> mostSupportedSatisfyingPairs = null;
            for (Pair candidateDelta : candidateDeltas) {
                int minDistance = candidateDelta.getMin();
                int maxDistance = candidateDelta.getMax();
                if (minDistance <= maxDistance) {
                    System.out.println("Calculating support for candidate delta (" + minDistance + "," + maxDistance + ")");
                    List<Integer> timestampCounts = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
                    TreeSet<Pair> satisfyingPairs = new TreeSet<>();
                    for (int index = 0; index < timestampCounts.size(); index++) {
                        int indexCount = timestampCounts.get(index);
                        if (indexCount == 0) continue;
                        if (indexCount > 1 && 0 >= minDistance && 0 <= maxDistance)
                            satisfyingPairs.add(new Pair(index, index));
                        for (int j = index + 1; j < timestampCounts.size(); j++) {
                            int jCount = timestampCounts.get(j);
                            if (jCount > 0) {
                                int distance = j - index;
                                if (distance >= minDistance && distance <= maxDistance) {
                                    satisfyingPairs.add(new Pair(index, j));
                                }
                            }
                        }
                    }
                    System.out.println("Satisfying pairs: " + satisfyingPairs);
                    double candidateSupport = calculateTGFDSupport(satisfyingPairs.size(), entities.size(), config.getTimestamp());
                    if (candidateSupport > candidateTGFDsupport) {
                        candidateTGFDsupport = candidateSupport;
                        mostSupportedDelta = candidateDelta;
                        mostSupportedSatisfyingPairs = satisfyingPairs;
                    }
                }
            }

            if (mostSupportedDelta == null) {
                System.out.println("Could not come up with mostSupportedDelta for entity: " + entityEntry.getKey());
                continue;
            }

            deltaToPairsMap.computeIfAbsent(mostSupportedDelta, k -> new ArrayList<>()).add(mostSupportedSatisfyingPairs);

            int minDistance = mostSupportedDelta.getMin();
            int maxDistance = mostSupportedDelta.getMax();
            candidateTGFDdelta = new Delta(Period.ofYears(minDistance), Period.ofYears(maxDistance), Duration.ofDays(365));
            constantPath.setDelta(candidateTGFDdelta);

            // TODO: Ensures we don't expand constant TGFDs from previous iterations
//            boolean isNotMinimal = false;
//            if (Util.hasMinimalityPruning && constantPath.isSuperSetOfPathAndSubsetOfDelta(patternNode.getAllMinimalConstantDependenciesOnThisPath())) {
//                System.out.println("Candidate constant TGFD " + constantPath + " is a superset of an existing minimal constant TGFD");
//                isNotMinimal = true;
//            }
//            if (isNotMinimal) continue;

            if (candidateTGFDsupport < config.getTgfdTheta()) {
                // TODO: add the NegativeTGFD
//                negativeTGFDs.add(new NegativeTGFD(entityEntry));
                logger.info("Could not satisfy TGFD support threshold for entity: " + entityEntry.getKey());
            } else {
                TGFD entityTGFD = new TGFD(newPattern, candidateTGFDdelta, newDependency, candidateTGFDsupport, patternNode.getPatternSupport());
                tgfds.add(entityTGFD);
//                if (Util.hasMinimalityPruning) patternNode.addMinimalConstantDependency(constantPath);
            }
        }

        return tgfds;
    }

    public List<TGFD> discoverGeneralTGFD(PatternTreeNode patternTreeNode, double patternSupport, AttributeDependency literalPath, int entitiesSize,
                                          Map<Pair, List<TreeSet<Pair>>> deltaToPairsMap, LiteralTreeNode literalTreeNode) {
        List<TGFD> tgfds = new ArrayList<>();

//        int numOfEntitiesWithDeltas = deltaToPairsMap.values().stream().mapToInt(List::size).sum();
//        int numOfPairs = deltaToPairsMap.values().stream().flatMap(List::stream).mapToInt(Set::size).sum();

        Map<Pair, List<Pair>> intersections = new HashMap<>();
//        int[] currMinMax = {0, config.getTimestamp() - 1};
        int currMin = 0;
        int currMax = config.getTimestamp() - 1;
        List<Pair> currSatisfyingAttrValues = new ArrayList<>();
        for (Pair deltaPair: deltaToPairsMap.keySet().stream().sorted().collect(Collectors.toList())) {
            if (Math.max(currMin, deltaPair.getMin()) <= Math.min(currMax, deltaPair.getMax())) {
                currMin = Math.max(currMin, deltaPair.getMin());
                currMax = Math.min(currMax, deltaPair.getMax());
//				currSatisfyingAttrValues.add(satisfyingPairsSet.get(index)); // By axiom 4
                continue;
            }
            for (Map.Entry<Pair, List<TreeSet<Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
                for (TreeSet<Pair> satisfyingPairSet : deltaToPairsEntry.getValue()) {
                    for (Pair satisfyingPair : satisfyingPairSet) {
                        if (satisfyingPair.getMax() - satisfyingPair.getMin() >= currMin && satisfyingPair.getMax() - satisfyingPair.getMin() <= currMax) {
                            currSatisfyingAttrValues.add(new Pair(satisfyingPair.getMin(), satisfyingPair.getMax()));
                        }
                    }
                }
            }
            intersections.putIfAbsent(new Pair(currMin, currMax), currSatisfyingAttrValues);
            currSatisfyingAttrValues = new ArrayList<>();
            currMin = 0;
            currMax = config.getTimestamp() - 1;
            if (Math.max(currMin, deltaPair.getMin()) <= Math.min(currMax, deltaPair.getMax())) {
                currMin = Math.max(currMin, deltaPair.getMin());
                currMax = Math.min(currMax, deltaPair.getMax());
            }
        }
        for (Map.Entry<Pair, List<TreeSet<Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
            for (TreeSet<Pair> satisfyingPairSet : deltaToPairsEntry.getValue()) {
                for (Pair satisfyingPair : satisfyingPairSet) {
                    if (satisfyingPair.getMax() - satisfyingPair.getMin() >= currMin && satisfyingPair.getMax() - satisfyingPair.getMin() <= currMax) {
                        currSatisfyingAttrValues.add(new Pair(satisfyingPair.getMin(), satisfyingPair.getMax()));
                    }
                }
            }
        }
        intersections.putIfAbsent(new Pair(currMin, currMax), currSatisfyingAttrValues);

        List<Map.Entry<Pair, List<Pair>>> sortedIntersections = new ArrayList<>(intersections.entrySet());
        sortedIntersections.sort(Comparator.comparing(o -> o.getValue().size(), Comparator.reverseOrder()));

        sortedIntersections.forEach(intersection -> {
            Pair candidateDelta = intersection.getKey();
            int generalMin = candidateDelta.getMin();
            int generalMax = candidateDelta.getMax();

            int numberOfSatisfyingPairs = intersection.getValue().size();
            double tgfdSupport = calculateTGFDSupport(numberOfSatisfyingPairs, entitiesSize, config.getTimestamp());

            Delta delta = new Delta(Period.ofYears(generalMin), Period.ofYears(generalMax), Duration.ofDays(365));

            DataDependency generalDependency = new DataDependency();
            String yVertexType = literalPath.getRhs().getVertexType();
            String yAttrName = literalPath.getRhs().getAttrName();

            VariableLiteral varY = new VariableLiteral(yVertexType, yAttrName);
            generalDependency.getY().add(varY);
            literalPath.getLhs().stream().map(x -> new VariableLiteral(x.getVertexType(), x.getAttrName())).forEach(generalDependency.getX()::add);

            if (tgfdSupport < config.getTgfdTheta()) {
                logger.info("Support for candidate general TGFD is below support threshold");
            } else {
                TGFD tgfd = new TGFD(patternTreeNode.getPattern(), delta, generalDependency, tgfdSupport, patternSupport);
                tgfds.add(tgfd);
            }
        });

        return tgfds;
    }

    public double calculateTGFDSupport(double numerator, double S, int T) {
        double denominator = S * CombinatoricsUtils.binomialCoefficient(T + 1, 2);
        if (numerator > denominator)
            throw new IllegalArgumentException("numerator > denominator");
        return numerator / denominator;
    }

}
