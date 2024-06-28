package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.utils.DeepCopyUtil;
import com.db.tgfdparallel.utils.MathUtil;
import com.google.common.hash.Hashing;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
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

    public Set<TGFD> discoverConstantTGFD(PatternTreeNode patternNode, ConstantLiteral yLiteral,
                                          Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entities, List<Pair> candidatePairs, int dependencyKey) {
        long startTime = System.currentTimeMillis();

        Set<TGFD> result = new HashSet<>();
        int level = patternNode.getPattern().getPattern().vertexSet().size();
        VF2PatternGraph pattern = patternNode.getPattern();
//        List<AttributeDependency> allMinimalConstantDependenciesOnThisPath = patternService.getAllMinimalConstantDependenciesOnThisPath(patternNode);
        double supportThreshold = config.getTgfdTheta() / config.getWorkers().size();

        for (Map.Entry<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
            Set<ConstantLiteral> xLiterals = entityEntry.getKey();
            List<Map.Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq = entityEntry.getValue();

            long rhsDiscoveryStartTime = System.currentTimeMillis();
            // 不管rhsAttrValuesTimestampsSortedByFreq的size，也计算Delta，对每个都转成TGFD，然后support返回给Coordinator处理

            // TODO: Deal with multiple rhs
            for (Map.Entry<ConstantLiteral, List<Integer>> entry : rhsAttrValuesTimestampsSortedByFreq) {
//                VF2PatternGraph newPattern = DeepCopyUtil.deepCopy(pattern);
                DataDependency newDependency = new DataDependency();
                AttributeDependency constantPath = new AttributeDependency();

                // 处理Dependency
                String yAttrValue = entry.getKey().getAttrValue();
                generateDataDependency(yAttrValue, yLiteral, pattern, newDependency, constantPath, xLiterals);

                // 处理Delta
                List<Integer> values = entry.getValue();
                Pair minMaxPair = getMinMaxPair(values);
                if (minMaxPair == null) {
                    continue;
                }
                int minDistance = minMaxPair.getMin();
                int maxDistance = minMaxPair.getMax();

                long numberOfPairs = MathUtil.countPairs(values);
                double tgfdSupport = calculateTGFDSupport(numberOfPairs, entities.size(), config.getTimestamp());
                if (tgfdSupport < supportThreshold) {
//                    logger.info("TGFD support is less than the threshold. TGFD support: {}  **  Threshold: {}", tgfdSupport, supportThreshold);
                    continue;
                }

                // 处理deltaToPairsMap，为后续生成generalTGFDs
                if (minDistance <= maxDistance) {
                    candidatePairs.add(minMaxPair);
                }

                Delta candidateTGFDdelta = new Delta(Period.ofYears(minDistance), Period.ofYears(maxDistance));
                constantPath.setDelta(candidateTGFDdelta);

//                long constantTGFDKeyStartTime = System.currentTimeMillis();
//                if (dependencyService.isSuperSetOfPathAndSubsetOfDelta(constantPath, allMinimalConstantDependenciesOnThisPath)) {
//                    logger.info("WE CANNOT SKIP THIS STEP!!");
//                    continue;
//                }
//                long constantTGFDKeyEndTime = System.currentTimeMillis();
//                logger.info("Time taken to check super set of path and subset of delta: {} ms", (constantTGFDKeyEndTime - constantTGFDKeyStartTime));

//                patternService.addMinimalConstantDependency(patternNode, constantPath);

                // Coordinator处，delta, support 需要重新计算
                TGFD candidateConstantTGFD = new TGFD(minMaxPair, newDependency, 0.0, level, dependencyKey, (int) numberOfPairs);
                result.add(candidateConstantTGFD);
            }
            long rhsDiscoveryEndTime = System.currentTimeMillis();
        }
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to discover constant TGFDs: {} ms, there are {} entities", (endTime - startTime), entities.size());

        return result;
    }

    public void generateDataDependency(String attrValue, ConstantLiteral yLiteral, VF2PatternGraph newPattern, DataDependency newDependency,
                                       AttributeDependency constantPath, Set<ConstantLiteral> xLiterals) {
        Map<String, ConstantLiteral> vertexTypeToLiteral = new HashMap<>();
        String yVertexType = yLiteral.getVertexType();
        String yAttrName = yLiteral.getAttrName();

        for (ConstantLiteral xLiteral : xLiterals) {
            vertexTypeToLiteral.put(xLiteral.getVertexType(), xLiteral);
        }

        for (Vertex v : newPattern.getPattern().vertexSet()) {
            String vType = v.getType();
            if (vType.equalsIgnoreCase(yVertexType)) {
//                v.getAttributes().add(new Attribute(yAttrName));
                ConstantLiteral newY = new ConstantLiteral(yVertexType, yAttrName, attrValue);
                newDependency.getY().add(newY);
            }

            ConstantLiteral xLiteral = vertexTypeToLiteral.get(vType);
            if (xLiteral != null) {
//                v.getAttributes().add(new Attribute(xLiteral.getAttrName(), xLiteral.getAttrValue()));
                ConstantLiteral newXLiteral = new ConstantLiteral(vType, xLiteral.getAttrName(), xLiteral.getAttrValue());
                newDependency.getX().add(newXLiteral);
                constantPath.getLhs().add(newXLiteral);
            }

        }
        constantPath.setRhs(new ConstantLiteral(yVertexType, yAttrName, attrValue));
    }

    public Set<TGFD> discoverGeneralTGFD(PatternTreeNode patternTreeNode, AttributeDependency literalPath, List<Pair> deltas) {
        long startTime = System.currentTimeMillis();
        Set<TGFD> tgfds = new HashSet<>();
        List<Pair> candidateDeltas = mergeOverlappingPairs(deltas);
        /*
            咱就不在worker计算support了，把相同Literal Path获得的General TGFD的Delta合并
            然后返回给Coordinator进行下一步合并Delta，在进行support的计算
            在Coordinator进行合并，可能需要重新设计hashKey
         */
        for (Pair delta : candidateDeltas) {
            DataDependency generalDependency = new DataDependency();
            String yVertexType = literalPath.getRhs().getVertexType();
            String yAttrName = literalPath.getRhs().getAttrName();

            VariableLiteral varY = new VariableLiteral(yVertexType, yAttrName);
            generalDependency.getY().add(varY);
            literalPath.getLhs().stream().map(x -> new VariableLiteral(x.getVertexType(), x.getAttrName())).forEach(generalDependency.getX()::add);

            int level = patternTreeNode.getPattern().getPattern().vertexSet().size();

            TGFD tgfd = new TGFD(delta, generalDependency, 0.0, level);
            tgfds.add(tgfd);
        }

        if (!tgfds.isEmpty() && literalPath.getLhs().size() > 1) {
            patternService.addMinimalDependency(patternTreeNode, literalPath);
            logger.info("Added minimal dependency: {}", literalPath);
        }
        long endTime = System.currentTimeMillis();
        logger.info("Time taken to discover general TGFDs: {} ms on {}, and there are {} general TGFD", (endTime - startTime), literalPath, tgfds.size());
        return tgfds;
    }

    public double calculateTGFDSupport(double numerator, double S, int T) {
        double denominator = S * CombinatoricsUtils.binomialCoefficient(T + 1, 2);
        if (numerator > denominator) {
            throw new IllegalArgumentException("numerator > denominator");
        }
        return numerator / denominator;
    }

    public int getConstantTGFDKey(DataDependency dependency) {
        List<ConstantLiteral> collect = dependency.getX().stream().map(x -> (ConstantLiteral) x).sorted().collect(Collectors.toList());
        ConstantLiteral literal = (ConstantLiteral) dependency.getY().get(0);
        StringBuilder sb = new StringBuilder();
        // sb得加上rhs的literal的vertexType，因为可能会有settlement->country, settlement->village的情况
        for (ConstantLiteral data : collect) {
            sb.append(data.getVertexType()).append(data.getAttrName()).append(data.getAttrValue());
        }
        sb.append(literal.getVertexType()).append(literal.getAttrName());
        return hashString(sb.toString());
    }

    public int getGeneralTGFDKey(DataDependency dependency) {
        List<VariableLiteral> collect = dependency.getX().stream().map(x -> (VariableLiteral) x).sorted().collect(Collectors.toList());
        VariableLiteral literal = (VariableLiteral) dependency.getY().get(0);
        StringBuilder sb = new StringBuilder();
        for (VariableLiteral data : collect) {
            sb.append(data.getVertexType()).append(data.getAttrName());
        }
        sb.append(literal.getVertexType()).append(literal.getAttrName());
        return hashString(sb.toString());
    }

    private Pair getMinMaxPair(List<Integer> timestampCounts) {
        int minDistance, maxDistance;
        List<Integer> occurIndices = new ArrayList<>();

        for (int index = 0; index < timestampCounts.size(); index++) {
            if (timestampCounts.get(index) > 0) {
                occurIndices.add(index);
            }
        }

        if (occurIndices.isEmpty()) {
            return null;
        }

        minDistance = Integer.MAX_VALUE;
        for (int i = 1; i < occurIndices.size(); i++) {
            minDistance = Math.min(minDistance, occurIndices.get(i) - occurIndices.get(i - 1));
            if (minDistance == 0) {
                break;
            }
        }

        Integer indexOfFirstOccurrence = occurIndices.get(0);
        Integer indexOfFinalOccurrence = occurIndices.get(occurIndices.size() - 1);

        if (indexOfFirstOccurrence.equals(indexOfFinalOccurrence) && timestampCounts.get(indexOfFirstOccurrence) > 1) {
            maxDistance = 0;
        } else {
            maxDistance = indexOfFinalOccurrence - indexOfFirstOccurrence;
        }

        if (minDistance > maxDistance) {
//            logger.info("Not enough timestamped matches found for entity.");
            return null;
        }

        return new Pair(minDistance, maxDistance);
    }

    public List<Pair> mergeOverlappingPairs(List<Pair> inputPairs) {
        if (inputPairs == null || inputPairs.isEmpty()) {
            return new ArrayList<>();
        }

        Collections.sort(inputPairs);

        List<Pair> mergedPairs = new ArrayList<>();
        Pair currentPair = inputPairs.get(0);

        for (int i = 1; i < inputPairs.size(); i++) {
            Pair nextPair = inputPairs.get(i);

            // If overlapping
            if (currentPair.getMax() >= nextPair.getMin()) {
                int newMin = Math.max(currentPair.getMin(), nextPair.getMin());
                int newMax = Math.min(currentPair.getMax(), nextPair.getMax());
                currentPair = new Pair(newMin, newMax);
            } else {
                mergedPairs.add(currentPair);
                currentPair = nextPair;
            }
        }
        mergedPairs.add(currentPair);

        return mergedPairs;
    }

    public boolean checkDeltaOverlapping(Pair pair1, Pair pair2) {
        return pair1.getMax() >= pair2.getMin() && pair1.getMin() <= pair2.getMax();
    }

    public void processConstantTGFD(Map<Integer, Set<TGFD>> constantTGFDMap, Map<Integer,Integer> dependencyMap) {
        int numOfPositiveTGFDs = 0;
        int numOfNegativeTGFDs = 0;

        Iterator<Map.Entry<Integer, Set<TGFD>>> iterator = constantTGFDMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, Set<TGFD>> entry = iterator.next();
            Set<TGFD> constantTGFDs = entry.getValue();
            Integer hashKey = entry.getKey();
            TGFD initialTGFD = constantTGFDs.iterator().next();
            int entities = dependencyMap.getOrDefault(initialTGFD.getDependencyKey(), 0);

            if (constantTGFDs.size() == 1) {
                TGFD tgfd = updateTGFDWithSupport(constantTGFDs.iterator().next(), entities);

                if (tgfd.getTgfdSupport() >= config.getTgfdTheta()) {
                    numOfPositiveTGFDs++;
                    constantTGFDMap.put(hashKey, new HashSet<>(Collections.singleton(tgfd)));
                } else {
                    iterator.remove();
                }
            } else {
                Map<String, Set<TGFD>> tgfdMap = constantTGFDs.stream()
                        .collect(Collectors.groupingBy(tgfd -> {
                            ConstantLiteral constantLiteral = (ConstantLiteral) tgfd.getDependency().getY().get(0);
                            return constantLiteral.getAttrValue();
                        }, Collectors.toSet()));

                // 对于有相同rhs attr value的tgfd进行delta merge
                for (String key : tgfdMap.keySet()) {
                    Set<TGFD> tgfds = tgfdMap.get(key);
                    Set<TGFD> mergeResult = mergeAndRecreateTGFD(tgfds);
                    tgfdMap.put(key, mergeResult);
                }

                Set<TGFD> combinedSet = tgfdMap.values().stream()
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());

                Set<TGFD> constantTGFDResults = updateAndFilterTGFDListWithSupport(combinedSet, entities);
                removeOverlappingDeltas(constantTGFDResults);
                if (constantTGFDResults.isEmpty()) {
                    iterator.remove();
                } else {
                    constantTGFDMap.put(hashKey, constantTGFDResults);
                }

                numOfNegativeTGFDs += constantTGFDs.size() - constantTGFDResults.size();
                numOfPositiveTGFDs += constantTGFDResults.size();
            }
        }

        logger.info("There are {} Positive TGFDs and {} Negative TGFDs", numOfPositiveTGFDs, numOfNegativeTGFDs);
    }

    public void processGeneralTGFD(Map<Integer, Set<TGFD>> generalTGFDMap) {
        int count = 0;
        Iterator<Map.Entry<Integer, Set<TGFD>>> iterator = generalTGFDMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, Set<TGFD>> entry = iterator.next();
            Set<TGFD> tgfds = entry.getValue();
            Integer key = entry.getKey();

            Set<TGFD> combinedList = mergeAndRecreateTGFD(tgfds);
//            Set<TGFD> generalTGFDResults = updateAndFilterTGFDListWithSupport(combinedList);

            count += combinedList.size();

            if (!combinedList.isEmpty()) {
                generalTGFDMap.put(key, combinedList);
            } else {
                iterator.remove();
            }
        }
        logger.info("There are {} General TGFDs", count);
    }

    private Set<TGFD> mergeAndRecreateTGFD(Set<TGFD> tgfds) {
        if (tgfds.isEmpty()) return tgfds;

        TGFD originalTGFD = tgfds.iterator().next();

        List<Pair> deltas = tgfds.stream()
                .map(TGFD::getDelta)
                .collect(Collectors.toList());

        List<Pair> mergedDeltas = mergeOverlappingPairs(deltas);

        // If the merged deltas have less size, it means some deltas were merged
        if (mergedDeltas.size() < deltas.size()) {
            return mergedDeltas.stream()
                    .map(mergedDelta -> new TGFD(
                            mergedDelta,
                            originalTGFD.getDependency(),
                            originalTGFD.getTgfdSupport(),
                            originalTGFD.getLevel()))
                    .collect(Collectors.toSet());
        } else {
            return tgfds;
        }
    }

    private TGFD updateTGFDWithSupport(TGFD tgfd, int entities) {
        Pair delta = tgfd.getDelta();
//        long numberOfDeltas = MathUtil.computeCombinations(delta.getMin(), delta.getMax());
        double tgfdSupport = calculateTGFDSupport(tgfd.getNumberOfPairs(), entities, config.getTimestamp());
        tgfd.setTgfdSupport(tgfdSupport);
        return tgfd;
    }

    private Set<TGFD> updateAndFilterTGFDListWithSupport(Set<TGFD> tgfds, int entities) {
        if (tgfds.isEmpty()) return tgfds;

        tgfds.forEach(tgfd -> {
            Pair delta = tgfd.getDelta();
//            long numberOfDeltas = MathUtil.computeCombinations(delta.getMin(), delta.getMax());
            double tgfdSupport = calculateTGFDSupport(tgfd.getNumberOfPairs(), entities, config.getTimestamp());
            tgfd.setTgfdSupport(tgfdSupport);
        });

        return tgfds.stream()
                .filter(tgfd -> tgfd.getTgfdSupport() >= config.getTgfdTheta())
                .collect(Collectors.toSet());
    }

    private void removeOverlappingDeltas(Set<TGFD> constantTGFDResults) {
        Set<TGFD> toRemove = new HashSet<>();
        List<TGFD> tgfdList = new ArrayList<>(constantTGFDResults);

        for (int i = 0; i < tgfdList.size(); i++) {
            for (int j = i + 1; j < tgfdList.size(); j++) {
                if (checkDeltaOverlapping(tgfdList.get(i).getDelta(), tgfdList.get(j).getDelta())) {
                    toRemove.add(tgfdList.get(i));
                    toRemove.add(tgfdList.get(j));
                }
            }
        }
        constantTGFDResults.removeAll(toRemove);
    }

    public int generateDependencyKey(AttributeDependency dependency) {
        StringBuilder key = new StringBuilder();
        for (ConstantLiteral literal : dependency.getLhs()) {
            key.append(literal.getVertexType()).append(literal.getAttrName());
        }
        key.append(dependency.getRhs().getVertexType()).append(dependency.getRhs().getAttrName());
        return hashString(key.toString());
    }

    public int hashString(String input) {
        return Hashing.murmur3_32().hashString(input, StandardCharsets.UTF_8).asInt();
    }

}
