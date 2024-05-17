package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.utils.DeepCopyUtil;
import com.db.tgfdparallel.utils.MathUtil;
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

    @Autowired
    public TGFDService(AppConfig config) {
        this.config = config;
    }

    public List<TGFD> discoverConstantTGFD(PatternTreeNode patternNode, ConstantLiteral yLiteral,
                                           Map<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entities, List<Pair> candidatePairs) {
        List<TGFD> result = new ArrayList<>();
        int level = patternNode.getPattern().getPattern().vertexSet().size();

        for (Map.Entry<Set<ConstantLiteral>, List<Map.Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
            Set<ConstantLiteral> xLiterals = entityEntry.getKey();
            List<Map.Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq = entityEntry.getValue();

            // 不管rhsAttrValuesTimestampsSortedByFreq的size，也计算Delta，对每个都转成TGFD，然后support返回给Coordinator处理
            for (Map.Entry<ConstantLiteral, List<Integer>> entry : rhsAttrValuesTimestampsSortedByFreq) {
                VF2PatternGraph newPattern = DeepCopyUtil.deepCopy(patternNode.getPattern());
                DataDependency newDependency = new DataDependency();
                AttributeDependency constantPath = new AttributeDependency();

                // 处理Dependency
                String yAttrValue = entry.getKey().getAttrValue();
                generateDataDependency(yAttrValue, yLiteral, newPattern, newDependency, constantPath, xLiterals);

                // 处理Delta
                List<Integer> values = entry.getValue();
                Pair minMaxPair = getMinMaxPair(values);
                if (minMaxPair == null) {
                    continue;
                }
                int minDistance = minMaxPair.getMin();
                int maxDistance = minMaxPair.getMax();

                double supportThreshold = config.getTgfdTheta() / config.getWorkers().size();
                long numberOfPairs = MathUtil.countPairs(values);
                // TODO: 这里并不是用numberOfDeltas, 而是matches的pair数量
                double tgfdSupport = calculateTGFDSupport(numberOfPairs, entities.size(), config.getTimestamp());
                if (tgfdSupport < supportThreshold) {
                    logger.info("TGFD support is less than the threshold. TGFD support: {}  **  Threshold: {}", tgfdSupport, supportThreshold);
                    continue;
                }

                // 处理deltaToPairsMap，为后续生成generalTGFDs
                if (minDistance <= maxDistance) {
                    candidatePairs.add(minMaxPair);
                }

                Delta candidateTGFDdelta = new Delta(Period.ofYears(minDistance), Period.ofYears(maxDistance), Duration.ofDays(365));
                constantPath.setDelta(candidateTGFDdelta);

                // Coordinator处，delta, support 需要重新计算
                TGFD candidateConstantTGFD = new TGFD(patternNode.getPattern(), minMaxPair, newDependency, 0.0,
                        patternNode.getPatternSupport(), level, entities.size());
                result.add(candidateConstantTGFD);
            }
        }

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
                v.getAttributes().add(new Attribute(yAttrName));
                ConstantLiteral newY = new ConstantLiteral(yVertexType, yAttrName, attrValue);
                newDependency.getY().add(newY);
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
    }

    public List<TGFD> discoverGeneralTGFD(PatternTreeNode patternTreeNode, double patternSupport, AttributeDependency literalPath,
                                          List<Pair> deltas, int entitySize) {
        List<TGFD> tgfds = new ArrayList<>();
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

            TGFD tgfd = new TGFD(patternTreeNode.getPattern(), delta, generalDependency, 0.0, patternSupport, level, entitySize);
            tgfds.add(tgfd);
        }

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
        return sb.hashCode();
    }

    public int getGeneralTGFDKey(DataDependency dependency) {
        List<VariableLiteral> collect = dependency.getX().stream().map(x -> (VariableLiteral) x).sorted().collect(Collectors.toList());
        VariableLiteral literal = (VariableLiteral) dependency.getY().get(0);
        StringBuilder sb = new StringBuilder();
        for (VariableLiteral data : collect) {
            sb.append(data.getVertexType()).append(data.getAttrName());
        }
        sb.append(literal.getVertexType()).append(literal.getAttrName());
        return sb.hashCode();
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

    public void processConstantTGFD(Map<Integer, List<TGFD>> constantTGFDMap) {
        int numOfPositiveTGFDs = 0;
        int numOfNegativeTGFDs = 0;

        Iterator<Map.Entry<Integer, List<TGFD>>> iterator = constantTGFDMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, List<TGFD>> entry = iterator.next();
            List<TGFD> constantTGFDsList = entry.getValue();
            Integer hashKey = entry.getKey();

            if (constantTGFDsList.size() == 1) {
                TGFD tgfd = updateTGFDWithSupport(constantTGFDsList.get(0));

                if (tgfd.getTgfdSupport() >= config.getTgfdTheta()) {
                    numOfPositiveTGFDs++;
                    constantTGFDMap.put(hashKey, Collections.singletonList(tgfd));
                } else {
                    iterator.remove();
                }
            } else {
                Map<String, List<TGFD>> tgfdMap = constantTGFDsList.stream()
                        .collect(Collectors.groupingBy(tgfd -> {
                            ConstantLiteral constantLiteral = (ConstantLiteral) tgfd.getDependency().getY().get(0);
                            return constantLiteral.getAttrValue();
                        }));

                for (String key : tgfdMap.keySet()) {
                    List<TGFD> tgfds = tgfdMap.get(key);
                    List<TGFD> mergeResult = mergeAndRecreateTGFD(tgfds);
                    tgfdMap.put(key, mergeResult);
                }

                List<TGFD> combinedList = tgfdMap.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

                List<TGFD> constantTGFDResults = updateAndFilterTGFDListWithSupport(combinedList);
                removeOverlappingDeltas(constantTGFDResults);
                constantTGFDMap.put(hashKey, constantTGFDResults);

                numOfNegativeTGFDs += constantTGFDsList.size() - constantTGFDResults.size();
            }
        }

        logger.info("There are {} Positive TGFDs and {} Negative TGFDs", numOfPositiveTGFDs, numOfNegativeTGFDs);
    }

    public void processGeneralTGFD(Map<Integer, List<TGFD>> generalTGFDMap) {
        int count = 0;
        Iterator<Map.Entry<Integer, List<TGFD>>> iterator = generalTGFDMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, List<TGFD>> entry = iterator.next();
            List<TGFD> tgfds = entry.getValue();
            Integer key = entry.getKey();

            List<TGFD> combinedList = mergeAndRecreateTGFD(tgfds);
            List<TGFD> generalTGFDResults = updateAndFilterTGFDListWithSupport(combinedList);

            count += generalTGFDResults.size();

            if (generalTGFDResults.size() != 0) {
                generalTGFDMap.put(key, generalTGFDResults);
            } else {
                iterator.remove();
            }
        }
        logger.info("There are {} General TGFDs", count);
    }

    private List<TGFD> mergeAndRecreateTGFD(List<TGFD> tgfds) {
        TGFD originalTGFD = tgfds.get(0);

        List<Pair> deltas = tgfds.stream()
                .map(TGFD::getDelta)
                .collect(Collectors.toList());

        List<Pair> mergedDeltas = mergeOverlappingPairs(deltas);

        // If the merged deltas have less size, it means some deltas were merged
        if (mergedDeltas.size() < deltas.size()) {
            return mergedDeltas.stream()
                    .map(mergedDelta -> new TGFD(
                            originalTGFD.getPattern(),
                            mergedDelta,
                            originalTGFD.getDependency(),
                            originalTGFD.getTgfdSupport(),
                            originalTGFD.getPatternSupport(),
                            originalTGFD.getLevel(),
                            originalTGFD.getEntitySize()))
                    .collect(Collectors.toList());
        } else {
            return tgfds;
        }
    }

    private TGFD updateTGFDWithSupport(TGFD tgfd) {
        Pair delta = tgfd.getDelta();
        long numberOfDeltas = MathUtil.computeCombinations(delta.getMin(), delta.getMax());
        double tgfdSupport = calculateTGFDSupport(numberOfDeltas, tgfd.getEntitySize(), config.getTimestamp());
        tgfd.setTgfdSupport(tgfdSupport);
        return tgfd;
    }

    private List<TGFD> updateAndFilterTGFDListWithSupport(List<TGFD> tgfds) {
        int entitySize = tgfds.stream()
                .mapToInt(TGFD::getEntitySize)
                .sum();
        tgfds.forEach(tgfd -> {
            Pair delta = tgfd.getDelta();
            long numberOfDeltas = MathUtil.computeCombinations(delta.getMin(), delta.getMax());
            double tgfdSupport = calculateTGFDSupport(numberOfDeltas, entitySize, config.getTimestamp());
            tgfd.setTgfdSupport(tgfdSupport);
        });

        return tgfds.stream()
                .filter(tgfd -> tgfd.getTgfdSupport() >= config.getTgfdTheta())
                .collect(Collectors.toList());
    }

    private void removeOverlappingDeltas(List<TGFD> constantTGFDResults) {
        Set<TGFD> toRemove = new HashSet<>();
        for (int i = 0; i < constantTGFDResults.size(); i++) {
            for (int j = i + 1; j < constantTGFDResults.size(); j++) {
                if (checkDeltaOverlapping(constantTGFDResults.get(i).getDelta(), constantTGFDResults.get(j).getDelta())) {
                    toRemove.add(constantTGFDResults.get(i));
                    toRemove.add(constantTGFDResults.get(j));
                }
            }
        }
        constantTGFDResults.removeAll(toRemove);
    }

}
