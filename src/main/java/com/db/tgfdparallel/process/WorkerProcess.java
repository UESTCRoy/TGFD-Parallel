package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.service.*;
import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

    @Autowired
    public WorkerProcess(AppConfig config, ActiveMQService activeMQService, DataShipperService dataShipperService, GraphService graphService,
                         PatternService patternService, HSpawnService hSpawnService) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
        this.graphService = graphService;
        this.patternService = patternService;
        this.hSpawnService = hSpawnService;
    }

    public void start() {
        // Send the status to the coordinator
        activeMQService.sendStatus();

        // Receive the pattern tree from the coordinator
        List<PatternTreeNode> patternTreeNodes = dataShipperService.receiveSinglePatternNode();

        // Receive the histogram data from the coordinator
        ProcessedHistogramData histogramData = dataShipperService.receiveHistogramData();
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = histogramData.getVertexTypesToActiveAttributesMap();

        // Load the first snapshot
        GraphLoader graphLoader = graphService.loadFirstSnapshot(config.getDataPath());

        // TODO: 之前通过MQ worker接受一个很大的String job: id # CenterNodeVertexID # diameter # FragmentID # Type, 改进：针对每个worker创建一个job队列
        // TODO: 我们可能都不需要发送job了，因为worker可以根据singleNodePattern来生成job

        // By using the change file, generate new loader for each snapshot
        GraphLoader[] loaders = new GraphLoader[config.getTimestamp()];
        loaders[0] = graphLoader;
        graphService.updateFirstSnapshot(graphLoader);
        for (int i = 1; i < config.getTimestamp(); i++) {
            GraphLoader changeLoader = graphService.updateNextSnapshot(i + 1, graphLoader);
            loaders[i] = changeLoader;
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
            patternService.singleNodePatternInitialization(loaders[i].getGraph().getGraph(), i + 1, vertexTypesToActiveAttributesMap,
                    patternTreeNodeMap, entityURIsByPTN, matchesPerTimestampsByPTN, assignedJobsBySnapshot);
        }

        List<TGFD> constantTGFDs = new ArrayList<>();
        List<TGFD> generalTGFDs = new ArrayList<>();

        // Start VSpawn
        PatternTree patternTree = new PatternTree();
        patternTree.getTree().get(0).addAll(patternTreeNodes);
        int level = 0;
        List<String> edgeData = histogramData.getSortedFrequentEdgesHistogram().stream().map(FrequencyStatistics::getType).collect(Collectors.toList());
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

                Map<Integer, List<Job>> newJobsList = new HashMap<>();
                for (int index : assignedJobsBySnapshot.keySet()) {
                    List<Job> newJobsAtIndex = new ArrayList<>();
                    for (Job job : assignedJobsBySnapshot.get(index)) {
                        if (job.getPatternTreeNode().getPattern().equals(vSpawnedPatterns.getOldPattern().getPattern())) {
                            // TODO: change Diameter here? CurrentLevel + 1?
                            Job newJob = new Job(job.getCenterNode(), newPattern);
                            newJobsAtIndex.add(newJob);
                            assignedJobsBySnapshot.get(index).add(newJob);
                        }
                    }
                    newJobsList.put(index, newJobsAtIndex);
                }

                for (int superstep = 0; superstep <= config.getTimestamp(); superstep++) {
                    GraphLoader loader = loaders[superstep];
                    runSnapshot(superstep, loader, newJobsList, matchesPerTimestampsByPTN, entityURIsByPTN, vertexTypesToActiveAttributesMap);
                }

                // 计算new Pattern的support，然后判断与theta的关系
//                patternService.calculateTotalSupport();

                // 计算新pattern的HSpawn
                List<List<TGFD>> tgfds = hSpawnService.performHSPawn(vertexTypesToActiveAttributesMap, newPattern, matchesPerTimestampsByPTN.get(newPattern));
            }
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

    public void runSnapshot(int snapShotID, GraphLoader loader, Map<Integer, List<Job>> newJobsList, Map<PatternTreeNode,
            List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN, Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN,
                            Map<String, Set<String>> vertexTypesToActiveAttributesMap) {

        System.out.println("Retrieving matches for all the joblets.");

        long startTime = System.currentTimeMillis();
        Graph<Vertex, RelationshipEdge> graph = loader.getGraph().getGraph();
        Set<Vertex> verticesInGraph = new HashSet<>(graph.vertexSet());

        for (int index = 0; index <= snapShotID; index++) {
            for (Job job : newJobsList.get(index)) {
                if (!verticesInGraph.contains(job.getCenterNode())) {
                    continue;
                }

                Graph<Vertex, RelationshipEdge> subgraph = graphService.getSubGraphWithinDiameter(graph, job.getCenterNode(), job.getDiameter());
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
