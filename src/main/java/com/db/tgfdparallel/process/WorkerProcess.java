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
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
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
    private final JobService jobService;
    private final S3Service s3Service;

    @Autowired
    public WorkerProcess(AppConfig config, ActiveMQService activeMQService, DataShipperService dataShipperService, GraphService graphService,
                         PatternService patternService, HSpawnService hSpawnService, TGFDService tgfdService, JobService jobService, S3Service s3Service) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
        this.graphService = graphService;
        this.patternService = patternService;
        this.hSpawnService = hSpawnService;
        this.tgfdService = tgfdService;
        this.jobService = jobService;
        this.s3Service = s3Service;
    }

    public void start() {
        // Send the status to the coordinator
        logger.info("{} send the status to Coordinator at {}", config.getNodeName(), LocalDateTime.now());
        activeMQService.sendStatus();

        // Receive the histogram data from the coordinator
        // TODO: histogram字段并不全部有用
        long histogramStartTime = System.currentTimeMillis();
        ProcessedHistogramData histogramData = dataShipperService.receiveHistogramData();
        long histogramEndTime = System.currentTimeMillis();
        logger.info("Received Histogram From Coordinator, {} ms", histogramEndTime - histogramStartTime);
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = histogramData.getVertexTypesToActiveAttributesMap();
        List<String> edgeData = histogramData.getSortedFrequentEdgesHistogram().stream().map(FrequencyStatistics::getType).collect(Collectors.toList());
        Map<String, Integer> vertexHistogram = histogramData.getSortedVertexHistogram().stream()
                .collect(Collectors.toMap(FrequencyStatistics::getType, FrequencyStatistics::getFrequency));
        Set<String> vertexTypes = histogramData.getSortedVertexHistogram().stream()
                .map(FrequencyStatistics::getType).collect(Collectors.toSet());

        // Receive the pattern tree from the coordinator
        long singlePatternStartTime = System.currentTimeMillis();
        List<PatternTreeNode> patternTreeNodes = dataShipperService.receiveSinglePatternNode();
        long singlePatternEndTime = System.currentTimeMillis();
        logger.info("Received singlePatternTreeNodes From Coordinator, {} ms", singlePatternEndTime - singlePatternStartTime);

        // Load the first snapshot
        String dataPath = dataShipperService.awsWorkerDataPreparation();
        GraphLoader graphLoader = graphService.loadFirstSnapshot(dataPath, vertexTypes);
        logger.info("Load the first split graph, graph edge size: {}, graph vertex size: {}",
                graphLoader.getGraph().getGraph().edgeSet().size(),
                graphLoader.getGraph().getGraph().vertexSet().size());

        // By using the change file, generate new loader for each snapshot
        GraphLoader[] loaders = new GraphLoader[config.getTimestamp()];
        loaders[0] = graphLoader;
        graphService.updateFirstSnapshot(graphLoader);

        List<List<Change>> changesData = dataShipperService.receiveChangesFromCoordinator();
        for (int i = 0; i < changesData.size(); i++) {
            // I create a deep copy of previous loader (用前一个graph，而不是第一个graph)
            GraphLoader copyOfFirstLoader = DeepCopyUtil.deepCopy(loaders[i]);
            GraphLoader changeLoader = graphService.updateNextSnapshot(changesData.get(i), copyOfFirstLoader);
            loaders[i + 1] = changeLoader;
        }

        // Initialize the matchesPerTimestampsByPTN and entityURIsByPTN
        Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN = new HashMap<>();
        Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN = new HashMap<>();
        Map<Integer, List<Job>> assignedJobsBySnapshot = new HashMap<>();
        init(patternTreeNodes, matchesPerTimestampsByPTN, entityURIsByPTN);

        // run first level matches
        Map<String, PatternTreeNode> patternTreeNodeMap = patternTreeNodes.stream()
                .collect(Collectors.toMap(
                        node -> node.getPattern().getCenterVertexType(),
                        node -> node
                ));
        for (int i = 0; i < config.getTimestamp(); i++) {
            patternService.singleNodePatternInitialization(loaders[i].getGraph(), i, vertexTypesToActiveAttributesMap,
                    patternTreeNodeMap, entityURIsByPTN, matchesPerTimestampsByPTN, assignedJobsBySnapshot);
        }

        List<TGFD> constantTGFDs = new ArrayList<>();
        List<TGFD> generalTGFDs = new ArrayList<>();
        Map<Integer, List<TGFD>> constantTGFDMap = new HashMap<>();
        Map<Integer, List<TGFD>> generalTGFDMap = new HashMap<>();

        // Start VSpawn
        PatternTree patternTree = new PatternTree();
        patternTree.getTree().get(0).addAll(patternTreeNodes);
        int level = 0;
        while (level < config.getK()) {
            List<VSpawnPattern> vSpawnPatternList = patternService.vSpawnGenerator(vertexTypesToActiveAttributesMap, edgeData, patternTree, level)
                    .stream()
                    .filter(x -> x.getNewPattern() != null)
                    .collect(Collectors.toList());
            if (vSpawnPatternList.isEmpty()) {
                break;
            }
            List<PatternTreeNode> newPatternList = vSpawnPatternList.stream().map(VSpawnPattern::getNewPattern).collect(Collectors.toList());
            patternTree.getTree().add(newPatternList);
            level++;

            for (VSpawnPattern vSpawnedPatterns : vSpawnPatternList) {
                PatternTreeNode newPattern = vSpawnedPatterns.getNewPattern();
                Graph<Vertex, RelationshipEdge> pattern = newPattern.getPattern().getPattern();
                matchesPerTimestampsByPTN.put(newPattern, new ArrayList<>());
                for (int timestamp = 0; timestamp < config.getTimestamp(); timestamp++) {
                    matchesPerTimestampsByPTN.get(newPattern).add(new HashSet<>());
                    entityURIsByPTN.put(newPattern, new HashMap<>());
                }
                logger.info("Finding TGFDs at level {} for pattern {}", level, pattern);

                Map<Integer, List<Job>> newJobsList = jobService.createNewJobsList(assignedJobsBySnapshot, vSpawnedPatterns.getOldPattern().getPattern(), newPattern);
                int numOfNewJobs = newJobsList.values().stream()
                        .mapToInt(List::size)
                        .sum();
                logger.info("We got {} new jobs to find new pattern's matches", numOfNewJobs);
                for (int superstep = 0; superstep < config.getTimestamp(); superstep++) {
                    GraphLoader loader = loaders[superstep];
                    int numOfMatches = runSnapshot(superstep, loader, newJobsList, matchesPerTimestampsByPTN, level, entityURIsByPTN, vertexTypesToActiveAttributesMap);
                    logger.info("We got {} matches for pattern: {} at timestamp: {}", numOfMatches, pattern, superstep);
                }

                // 计算new Pattern的support，然后判断与theta的关系，如果support不够，则把ptn设为pruned
                double newPatternSupport = patternService.calculatePatternSupport(entityURIsByPTN.get(newPattern),
                        vertexHistogram.get(newPattern.getPattern().getCenterVertexType()), config.getTimestamp());
                logger.info("The pattern support for pattern: {} is {}", pattern, newPatternSupport);
                newPattern.setPatternSupport(newPatternSupport);
                if (newPatternSupport < config.getPatternTheta()) {
                    newPattern.setPruned(true);
                    logger.info("The pattern: {} didn't pass the support threshold", pattern);
                    continue;
                }

                // 计算新pattern的HSpawn
                List<List<TGFD>> tgfds = hSpawnService.performHSPawn(vertexTypesToActiveAttributesMap, newPattern, matchesPerTimestampsByPTN.get(newPattern));
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
        for (TGFD data : constantTGFDs) {
            int hashKey = tgfdService.getConstantTGFDKey(data.getDependency());
            List<TGFD> constantTGFDsList = constantTGFDMap.computeIfAbsent(hashKey, k -> new ArrayList<>());
            constantTGFDsList.add(data);
        }
        for (TGFD data : generalTGFDs) {
            int hashKey = tgfdService.getGeneralTGFDKey(data.getDependency());
            List<TGFD> generalTGFDsList = generalTGFDMap.computeIfAbsent(hashKey, k -> new ArrayList<>());
            generalTGFDsList.add(data);
        }

        // Send data(Constant TGFDs) back to coordinator
        logger.info("Send {} constant and {} general TGFDs to Coordinator", constantTGFDs.size(), generalTGFDs.size());
        dataShipperService.uploadTGFD(constantTGFDMap, generalTGFDMap);
        logger.info(config.getNodeName() + " Done");
        if (dataShipperService.isAmazonMode()) {
            s3Service.stopInstance();
        }
    }

    public void init(List<PatternTreeNode> patternTreeNodes,
                     Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN,
                     Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN) {
        for (PatternTreeNode ptn : patternTreeNodes) {
            matchesPerTimestampsByPTN.computeIfAbsent(ptn, k -> IntStream.range(0, config.getTimestamp())
                    .mapToObj(timestamp -> new HashSet<Set<ConstantLiteral>>())
                    .collect(Collectors.toList()));

            entityURIsByPTN.put(ptn, new HashMap<>());
        }
    }

    public int runSnapshot(int snapshotID, GraphLoader loader, Map<Integer, List<Job>> newJobsList,
                           Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN, int level,
                           Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN, Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
        long startTime = System.currentTimeMillis();
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        Map<String, Vertex> nodeMap = loader.getGraph().getNodeMap();
        Set<Vertex> verticesInGraph = new HashSet<>(graph.vertexSet());
        int numOfMatchesInTimestamp = 0;

        for (Job job : newJobsList.get(snapshotID)) {
            if (!verticesInGraph.contains(job.getCenterNode())) {
                continue;
            }

            Set<String> validTypes = job.getPatternTreeNode().getPattern().getPattern().vertexSet().stream()
                    .flatMap(v -> v.getTypes().stream())
                    .collect(Collectors.toSet());

            // Diameter 根据level变化
            Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, job.getCenterNode(), level, validTypes);
            if (snapshotID != 0) {
                subgraph = graphService.updateChangedGraph(nodeMap, subgraph);
            }
            VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results =
                    graphService.checkIsomorphism(subgraph, job.getPatternTreeNode().getPattern(), false);

            if (results.isomorphismExists()) {
                Set<Set<ConstantLiteral>> matches = new HashSet<>();
//                logger.info("Start Matching at {}", LocalDateTime.now());
                numOfMatchesInTimestamp += patternService.extractMatches(results.getMappings(), matches, job.getPatternTreeNode(),
                        entityURIsByPTN.get(job.getPatternTreeNode()), snapshotID, vertexTypesToActiveAttributesMap);
//                logger.info("End Matching at {}", LocalDateTime.now());
                matchesPerTimestampsByPTN.get(job.getPatternTreeNode()).get(snapshotID).addAll(matches);
            }
        }
        return numOfMatchesInTimestamp;
    }
}


