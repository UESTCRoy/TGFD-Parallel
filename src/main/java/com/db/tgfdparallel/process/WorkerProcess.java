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

    @Autowired
    public WorkerProcess(AppConfig config, ActiveMQService activeMQService, DataShipperService dataShipperService, GraphService graphService,
                         PatternService patternService, HSpawnService hSpawnService, TGFDService tgfdService, JobService jobService) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
        this.graphService = graphService;
        this.patternService = patternService;
        this.hSpawnService = hSpawnService;
        this.tgfdService = tgfdService;
        this.jobService = jobService;
    }

    public void start() {
        // Send the status to the coordinator
        logger.info("{} send the status to Coordinator at {}", config.getNodeName(), LocalDateTime.now());
        activeMQService.sendStatus();

        // Receive the histogram data from the coordinator
        // TODO: histogram字段并不全部有用
        ProcessedHistogramData histogramData = dataShipperService.receiveHistogramData();
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = histogramData.getVertexTypesToActiveAttributesMap();
        List<String> edgeData = histogramData.getSortedFrequentEdgesHistogram().stream().map(FrequencyStatistics::getType).collect(Collectors.toList());
        Map<String, Integer> vertexHistogram = histogramData.getVertexHistogram();

        // Receive the pattern tree from the coordinator
        List<PatternTreeNode> patternTreeNodes = dataShipperService.receiveSinglePatternNode();

        // Load the first snapshot
        GraphLoader graphLoader = graphService.loadFirstSnapshot(config.getDataPath());

        // TODO: 之前通过MQ worker接受一个很大的String job: id # CenterNodeVertexID # diameter # FragmentID # Type, 改进：针对每个worker创建一个job队列
        // TODO: 我们可能都不需要发送job了，因为worker可以根据singleNodePattern来生成job

        // By using the change file, generate new loader for each snapshot
        GraphLoader[] loaders = new GraphLoader[config.getTimestamp()];
        loaders[0] = graphLoader;
        graphService.updateFirstSnapshot(graphLoader);

        List<List<Change>> changesData = dataShipperService.receiveChangesFromCoordinator();
        for (int i = 0; i < changesData.size(); i++) {
            // I create a deep copy of firstLoader here
            GraphLoader copyOfFirstLoader = DeepCopyUtil.deepCopy(graphLoader);
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
            patternService.singleNodePatternInitialization(loaders[i].getGraph().getGraph(), i, vertexTypesToActiveAttributesMap,
                    patternTreeNodeMap, entityURIsByPTN, matchesPerTimestampsByPTN, assignedJobsBySnapshot);
        }

        List<TGFD> constantTGFDs = new ArrayList<>();
        List<TGFD> generalTGFDs = new ArrayList<>();
        Map<Integer, Set<TGFD>> constantTGFDMap = new HashMap<>();

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
                matchesPerTimestampsByPTN.put(newPattern, new ArrayList<>());
                for (int timestamp = 0; timestamp < config.getTimestamp(); timestamp++) {
                    matchesPerTimestampsByPTN.get(newPattern).add(new HashSet<>());
                    entityURIsByPTN.put(newPattern, new HashMap<>());
                }

                Map<Integer, List<Job>> newJobsList = jobService.createNewJobsList(assignedJobsBySnapshot, vSpawnedPatterns.getOldPattern().getPattern(), newPattern);

                for (int superstep = 0; superstep < config.getTimestamp(); superstep++) {
                    GraphLoader loader = loaders[superstep];
                    runSnapshot(superstep, loader, newJobsList, matchesPerTimestampsByPTN, entityURIsByPTN, vertexTypesToActiveAttributesMap);
                }

                // 计算new Pattern的support，然后判断与theta的关系，如果support不够，则把ptn设为pruned
                double newPatternSupport = patternService.calculatePatternSupport(entityURIsByPTN.get(newPattern),
                        vertexHistogram.get(newPattern.getPattern().getCenterVertexType()), config.getTimestamp());
                if (newPatternSupport < config.getPatternTheta()) {
                    newPattern.setPruned(true);
                    continue;
                }

                // 计算新pattern的HSpawn
                List<List<TGFD>> tgfds = hSpawnService.performHSPawn(vertexTypesToActiveAttributesMap, newPattern, matchesPerTimestampsByPTN.get(newPattern));
                constantTGFDs.addAll(tgfds.get(0));
                generalTGFDs.addAll(tgfds.get(1));
            }

            if (level > 1) {
                for (TGFD data : constantTGFDs) {
                    int hashKey = tgfdService.getTGFDKey(data.getDependency());
                    Set<TGFD> tgfdSet = constantTGFDMap.computeIfAbsent(hashKey, k -> new HashSet<>());
                    tgfdSet.add(data);
                }
            }
        }

        // Send data(Constant TGFDs) back to coordinator
        dataShipperService.uploadConstantTGFD(constantTGFDMap);
        logger.info(config.getNodeName() + " Done");
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

    public void runSnapshot(int snapShotID, GraphLoader loader, Map<Integer, List<Job>> newJobsList, Map<PatternTreeNode,
            List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN, Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN,
                            Map<String, Set<String>> vertexTypesToActiveAttributesMap) {

        logger.info("Retrieving matches for all the joblets.");

        long startTime = System.currentTimeMillis();
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        Set<Vertex> verticesInGraph = new HashSet<>(graph.vertexSet());

        for (int index = 0; index <= snapShotID; index++) {
            for (Job job : newJobsList.get(index)) {
                if (!verticesInGraph.contains(job.getCenterNode())) {
                    continue;
                }

                Set<String> validTypes = job.getPatternTreeNode().getPattern().getPattern().vertexSet().stream()
                        .flatMap(v -> v.getTypes().stream())
                        .collect(Collectors.toSet());

                Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, job.getCenterNode(), job.getDiameter(), validTypes);
                job.setSubgraph(subgraph);

                VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = graphService.checkIsomorphism(subgraph, job.getPatternTreeNode().getPattern(), false);

                if (results.isomorphismExists()) {
                    Set<Set<ConstantLiteral>> matches = new HashSet<>();
                    int numOfMatchesInTimestamp = patternService.extractMatches(results.getMappings(), matches, job.getPatternTreeNode(),
                            entityURIsByPTN.get(job.getPatternTreeNode()), snapShotID, vertexTypesToActiveAttributesMap);
                    matchesPerTimestampsByPTN.get(job.getPatternTreeNode()).get(snapShotID).addAll(matches);
                }
            }
        }
    }

}
