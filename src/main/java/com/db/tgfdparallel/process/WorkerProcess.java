package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.service.*;
import com.db.tgfdparallel.utils.DeepCopyUtil;
import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class WorkerProcess {
    private static final Logger logger = LoggerFactory.getLogger(WorkerProcess.class);
    private final AppConfig config;
    private final ActiveMQService activeMQService;
    private final DataShipperService dataShipperService;
    private final GraphService graphService;
    private final PatternService patternService;
    private final HSpawnService hSpawnService;
    private final TGFDService tgfdService;
    private final S3Service s3Service;

    @Autowired
    public WorkerProcess(AppConfig config, ActiveMQService activeMQService, DataShipperService dataShipperService, GraphService graphService,
                         PatternService patternService, HSpawnService hSpawnService, TGFDService tgfdService, S3Service s3Service) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
        this.graphService = graphService;
        this.patternService = patternService;
        this.hSpawnService = hSpawnService;
        this.tgfdService = tgfdService;
        this.s3Service = s3Service;
    }

    private Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN;
    private Map<String, Map<String, List<Integer>>> entityURIsByPTN; // first key: centerVertexType, second key: entityURI

    public void start() {
        // Send the status to the coordinator
        logger.info("{} send the status to Coordinator at {}", config.getNodeName(), LocalDateTime.now());
        activeMQService.sendStatus();

        // Receive the histogram data from the coordinator
        ProcessedHistogramData histogramData = receiveAndProcessHistogramData();
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = histogramData.getVertexTypesToActiveAttributesMap();
        List<String> edgeData = histogramData.getSortedFrequentEdgesHistogram();

        Map<String, Integer> vertexHistogram = histogramData.getSortedVertexHistogram().stream()
                .collect(Collectors.toMap(FrequencyStatistics::getType, FrequencyStatistics::getFrequency));
        Set<String> vertexTypes = histogramData.getSortedVertexHistogram().stream()
                .map(FrequencyStatistics::getType).collect(Collectors.toSet());

        // Receive the pattern tree from the coordinator
        List<PatternTreeNode> patternTreeNodes = receivePatternTreeNodes();

        // Load the first snapshot
        String dataPath = dataShipperService.workerDataPreparation();
        GraphLoader initialLoader = graphService.loadFirstSnapshot(dataPath, vertexTypes);
        logger.info("Load the first split graph, graph edge size: {}, graph vertex size: {}",
                initialLoader.getGraph().getGraph().edgeSet().size(), initialLoader.getGraph().getGraph().vertexSet().size());

        // By using the change file, generate new loader for each snapshot
        GraphLoader[] loaders = processChangesAndLoadSubsequentSnapshots(initialLoader);

        // Initialize the matchesPerTimestampsByPTN and entityURIsByPTN
        initializePatternDataStructures(patternTreeNodes);

        // run first level matches
        Map<String, PatternTreeNode> patternTreeNodeMap = patternTreeNodes.stream()
                .collect(Collectors.toMap(
                        node -> node.getPattern().getCenterVertexType(),
                        node -> node
                ));

        for (int i = 0; i < config.getTimestamp(); i++) {
            patternService.singleNodePatternInitialization(loaders[i].getGraph(), i, patternTreeNodeMap, entityURIsByPTN);
        }
        evaluatePatternSupport(patternTreeNodes, vertexHistogram);

        Set<TGFD> constantTGFDs = new HashSet<>();
        Set<TGFD> generalTGFDs = new HashSet<>();
        Map<Integer, Set<TGFD>> constantTGFDMap = new HashMap<>();
        Map<Integer, Set<TGFD>> generalTGFDMap = new HashMap<>();

        // Start VSpawn
        PatternTree patternTree = new PatternTree();
        patternTree.getTree().get(0).addAll(patternTreeNodes);
        int level = 0;
        while (level < config.getK()) {
            List<VSpawnPattern> vSpawnPatternList = patternService.vSpawnGenerator(edgeData, patternTree, level);
            if (vSpawnPatternList.isEmpty()) {
                break;
            }
            List<PatternTreeNode> newPatternList = vSpawnPatternList.stream().map(VSpawnPattern::getNewPattern).collect(Collectors.toList());
            patternTree.getTree().add(newPatternList);
            level++;

            for (VSpawnPattern vSpawnedPatterns : vSpawnPatternList) {
                PatternTreeNode newPattern = vSpawnedPatterns.getNewPattern();
                Graph<Vertex, RelationshipEdge> pattern = newPattern.getPattern().getPattern();
                List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>(Collections.nCopies(config.getTimestamp(), new HashSet<>()));

                logger.info("Finding TGFDs at level {} for pattern {}", level, pattern);

//                if (level == 1 && numOfNewJobs < 100 * config.getTimestamp()) {
//                    logger.info("The number of new jobs is too small, skip this pattern");
//                    newPattern.setPruned(true);
//                    continue;
//                }
                String centerVertexType = newPattern.getPattern().getCenterVertexType();
                Map<String, List<Integer>> entityURIs = entityURIsByPTN.get(centerVertexType);

                List<CompletableFuture<Integer>> futures = new ArrayList<>();
                for (int superstep = 0; superstep < config.getTimestamp(); superstep++) {
                    GraphLoader loader = loaders[superstep];
                    Set<Set<ConstantLiteral>> matchesOnTimestamps = matchesPerTimestamps.get(superstep);
                    CompletableFuture<Integer> future = runSnapshotAsync(superstep, newPattern, loader, matchesOnTimestamps, level, entityURIs, vertexTypesToActiveAttributesMap);
                    futures.add(future);
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // 计算new Pattern的support，然后判断与theta的关系，如果support不够，则把ptn设为pruned
                double newPatternSupport = patternService.calculatePatternSupport(entityURIs,
                        vertexHistogram.get(newPattern.getPattern().getCenterVertexType()), config.getTimestamp());
                logger.info("The pattern support for pattern: {} is {}", pattern, newPatternSupport);
                newPattern.setPatternSupport(newPatternSupport);
                if (newPatternSupport < config.getPatternTheta()) {
                    newPattern.setPruned(true);
                    logger.info("The pattern: {} didn't pass the support threshold", pattern);
                    continue;
                }
                matchesPerTimestampsByPTN.put(newPattern, matchesPerTimestamps);

                // 计算新pattern的HSpawn
                List<List<TGFD>> tgfds = hSpawnService.performHSPawn(vertexTypesToActiveAttributesMap, newPattern, matchesPerTimestamps);
                if (tgfds.size() == 2 && level > 1) {
                    constantTGFDs.addAll(tgfds.get(0));
                    generalTGFDs.addAll(tgfds.get(1));
                }
                logger.info("Level: {}, Pattern: {}, Size Constant TGFD: {}, Size General TGFD: {}",
                        level, newPattern.getPattern().getPattern(), constantTGFDs.size(), generalTGFDs.size());
            }
        }
        logger.info("======================================");
        logger.info("The Maximum Level We got is {}", level);
        logger.info("======================================");

        // 生成constant与general的TGFD Map，返回给Coordinator汇总
        int constantCounter = 0;
        for (TGFD data : constantTGFDs) {
            int hashKey = tgfdService.getConstantTGFDKey(data.getDependency());
            constantTGFDMap.computeIfAbsent(hashKey, k -> new HashSet<>()).add(data);
            constantCounter++;
//            if (constantCounter == 50000) {
//                break;
//            }
        }
        int generalCounter = 0;
        for (TGFD data : generalTGFDs) {
            int hashKey = tgfdService.getGeneralTGFDKey(data.getDependency());
            generalTGFDMap.computeIfAbsent(hashKey, k -> new HashSet<>()).add(data);
            generalCounter++;
//            if (generalCounter == 50000) {
//                break;
//            }
        }

        // Send data(Constant TGFDs) back to coordinator
        logger.info("Send {} constant and {} general TGFDs to Coordinator", constantTGFDs.size(), generalTGFDs.size());
        dataShipperService.uploadTGFD(constantTGFDMap, generalTGFDMap);
        logger.info(config.getNodeName() + " Done");
        if (dataShipperService.isAmazonMode()) {
            s3Service.stopInstance();
        }
    }

    private ProcessedHistogramData receiveAndProcessHistogramData() {
        long startTime = System.currentTimeMillis();
        ProcessedHistogramData histogramData = dataShipperService.receiveHistogramData();
        long endTime = System.currentTimeMillis();
        logger.info("Received Histogram From Coordinator, {} ms", endTime - startTime);
        return histogramData;
    }

    private List<PatternTreeNode> receivePatternTreeNodes() {
        long startTime = System.currentTimeMillis();
        List<PatternTreeNode> patternTreeNodes = dataShipperService.receiveSinglePatternNode();
        long endTime = System.currentTimeMillis();
        logger.info("Received singlePatternTreeNodes From Coordinator, {} ms", endTime - startTime);
        return patternTreeNodes;
    }

    private GraphLoader[] processChangesAndLoadSubsequentSnapshots(GraphLoader initialLoader) {
        GraphLoader[] loaders = new GraphLoader[config.getTimestamp()];
        loaders[0] = initialLoader;
        // DEBUG Comment out
//        graphService.updateFirstSnapshot(initialLoader);
        logger.info("Load the updated first snapshot, graph edge size: {}, graph vertex size: {}",
                initialLoader.getGraph().getGraph().edgeSet().size(), initialLoader.getGraph().getGraph().vertexSet().size());
        List<List<Change>> changesData = dataShipperService.receiveChangesFromCoordinator();
        for (int i = 0; i < changesData.size(); i++) {
            GraphLoader copyOfLoader = DeepCopyUtil.deepCopy(loaders[i]);
            loaders[i + 1] = graphService.updateNextSnapshot(changesData.get(i), copyOfLoader);
            logger.info("Load the {} snapshot, graph edge size: {}, graph vertex size: {}",
                    i + 1, loaders[i + 1].getGraph().getGraph().edgeSet().size(), loaders[i + 1].getGraph().getGraph().vertexSet().size());
        }
        return loaders;
    }

    private void initializePatternDataStructures(List<PatternTreeNode> patternTreeNodes) {
        matchesPerTimestampsByPTN = new HashMap<>();
        entityURIsByPTN = new HashMap<>();

        for (PatternTreeNode ptn : patternTreeNodes) {
            String centerVertexType = ptn.getPattern().getCenterVertexType();
            matchesPerTimestampsByPTN.computeIfAbsent(ptn, k -> IntStream.range(0, config.getTimestamp())
                    .mapToObj(timestamp -> new HashSet<Set<ConstantLiteral>>())
                    .collect(Collectors.toList()));
            entityURIsByPTN.put(centerVertexType, new HashMap<>());
        }
    }

    private void evaluatePatternSupport(List<PatternTreeNode> patternTreeNodes, Map<String, Integer> vertexHistogram) {
        patternTreeNodes.forEach(ptn -> {
            String centerVertexType = ptn.getPattern().getCenterVertexType();
            double patternSupport = patternService.calculatePatternSupport(
                    entityURIsByPTN.get(centerVertexType),
                    vertexHistogram.get(ptn.getPattern().getCenterVertexType()),
                    config.getTimestamp()
            );
            if (patternSupport < config.getPatternTheta()) {
                ptn.setPruned(true);
                logger.info("Pruned pattern {} due to insufficient support.", ptn.getPattern().getPattern());
            }
        });
    }

    @Async
    public CompletableFuture<Integer> runSnapshotAsync(int snapshotID, PatternTreeNode newPattern, GraphLoader loader,
                                                       Set<Set<ConstantLiteral>> matchesOnTimestamps, int level, Map<String, List<Integer>> entityURIs,
                                                       Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        return CompletableFuture.completedFuture(
                runSnapshot(snapshotID, newPattern, loader, matchesOnTimestamps, level, entityURIs, vertexTypesToActiveAttributesMap)
        );
    }

    public int runSnapshot(int snapshotID, PatternTreeNode newPattern, GraphLoader loader, Set<Set<ConstantLiteral>> matchesOnTimestamps, int level,
                           Map<String, List<Integer>> entityURIs, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        String centerVertexType = newPattern.getPattern().getCenterVertex().getType();
        level = Math.min(level, 2);

        Set<String> validTypes = newPattern.getPattern().getPattern().vertexSet().stream()
                .map(Vertex::getType)
                .collect(Collectors.toSet());

        int finalLevel = level;
        graph.vertexSet().stream()
                .filter(vertex -> vertex.getType().equals(centerVertexType))
                .filter(vertex -> entityURIs.containsKey(vertex.getUri()) )
                .filter(vertex -> entityURIs.get(vertex.getUri()).get(snapshotID) > 0)
                .forEach(centerVertex -> {
                    Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, centerVertex, 1, validTypes);
                    VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results =
                            graphService.checkIsomorphism(subgraph, newPattern.getPattern(), false);

                    if (results.isomorphismExists()) {
                        Set<Set<ConstantLiteral>> matches = new HashSet<>();
                        patternService.extractMatches(results.getMappings(), matches, newPattern, snapshotID, vertexTypesToActiveAttributesMap);
                        matchesOnTimestamps.addAll(matches);
                    }
                });
        int matchesSize = matchesOnTimestamps.size();
        logger.info("Found {} matches in snapshot {}", matchesSize, snapshotID);
        return matchesSize;
    }
}


