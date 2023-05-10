package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.service.*;
import org.jgrapht.Graph;
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

    @Autowired
    public WorkerProcess(AppConfig config, ActiveMQService activeMQService, DataShipperService dataShipperService, GraphService graphService,
                         PatternService patternService) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
        this.graphService = graphService;
        this.patternService = patternService;
    }

    public void start() {
        // Send the status to the coordinator
        activeMQService.sendStatus();

        // Receive the pattern tree from the coordinator
        List<PatternTreeNode> patternTreeNodes = dataShipperService.receiveSinglePatternNode();

        // Receive the histogram data from the coordinator
        ProcessedHistogramData histogramData = dataShipperService.receiveHistogramData();

        // Load the first snapshot
        GraphLoader graphLoader = graphService.loadFirstSnapshot(config.getDataPath());

        // TODO: 之前通过MQ worker接受一个很大的String job: id # CenterNodeVertexID # diameter # FragmentID # Type, 改进：针对每个worker创建一个job队列
        // TODO: 我们可能都不需要发送job了，因为worker可以根据singleNodePattern来生成job

        // By using the change file, generate new loader for each snapshot
        GraphLoader[] loaders = new GraphLoader[config.getTimestamp()];
        loaders[0] = graphLoader;
        runFirstSnapshot(graphLoader);
        for (int i = 1; i < config.getTimestamp(); i++) {
            GraphLoader changeLoader = runNextSnapshot(i + 1, graphLoader);
            loaders[i] = changeLoader;
        }

        // Initialize the matchesPerTimestampsByPTN and entityURIsByPTN
        Map<PatternTreeNode, List<Set<Set<ConstantLiteral>>>> matchesPerTimestampsByPTN = new HashMap<>();
        Map<PatternTreeNode, Map<String, List<Integer>>> entityURIsByPTN = new HashMap<>();
        Map<Integer, Map<Integer, Job>> assignedJobsBySnapshot = new HashMap<>();
        init(patternTreeNodes, matchesPerTimestampsByPTN, entityURIsByPTN);

        // run first level matches
        Map<String, PatternTreeNode> patternTreeNodeMap = patternTreeNodes.stream()
                .collect(Collectors.toMap(
                        node -> node.getPattern().getCenterVertexType(),
                        node -> node
                ));
        for (int i = 0; i < config.getTimestamp(); i++) {
            patternService.singleNodePatternInitialization(loaders[i].getGraph().getGraph(), i + 1, histogramData.getVertexTypesToActiveAttributesMap(),
                    patternTreeNodeMap, entityURIsByPTN, matchesPerTimestampsByPTN, assignedJobsBySnapshot);
        }

        // Start VSpawn
        PatternTree patternTree = new PatternTree();
        patternTree.getTree().get(0).addAll(patternTreeNodes);
        int level = patternTree.getTree().size();
        List<String> edgeData = histogramData.getSortedFrequentEdgesHistogram().stream().map(FrequencyStatistics::getType).collect(Collectors.toList());
        Map<String, Set<String>> vertexTypesToActiveAttributesMap = histogramData.getVertexTypesToActiveAttributesMap();
        while (true) {
            patternService.vSpawnGenerator(vertexTypesToActiveAttributesMap, edgeData, patternTree.getTree().get(level));
            if (patternTree.getTree().get(level).isEmpty()) {
                break;
            }
            patternTree.getTree().add(new ArrayList<>());
            if (++level >= config.getK()) {
                break;
            }
        }
    }

    public void runFirstSnapshot(GraphLoader graphLoader) {
        boolean datashipper = false;
        HashMap<Integer, ArrayList<SimpleEdge>> dataToBeShipped = new HashMap<>();
        VF2DataGraph graph = graphLoader.getGraph();

        try {
            while (!datashipper) {
                String msg = activeMQService.receive();
                if (msg.startsWith("#datashipper")) {
                    dataToBeShipped = dataShipperService.readEdgesToBeShipped(msg);
                    logger.info("The data to be shipped has been received.");
                    datashipper = true;
                }
            }

            dataToBeShipped.forEach((workerID, edges) -> {
                try {
                    Graph<Vertex, RelationshipEdge> extractedGraph = graphService.extractGraphToBeSent(graphLoader, edges);
                    dataShipperService.sendGraphToBeShippedToOtherWorkers(extractedGraph, workerID);
                } catch (Exception e) {
                    logger.error("Error while extracting and sending graph for workerID: " + workerID, e);
                }
            });

            int receiveData = 0;
            activeMQService.connectConsumer(config.getNodeName() + "_data");

            while (receiveData < dataToBeShipped.size() - 1) {
                logger.info("*WORKER*: Start reading data from other workers...");
                String msg = activeMQService.receive();
                logger.info("*WORKER*: Received a new message.");

                if (msg != null) {
                    logger.info("*DATA RECEIVER*: Graph object has been received from '" + msg + "' successfully");
                    Object obj = dataShipperService.downloadObject(msg);
                    if (obj != null) {
                        Graph<Vertex, RelationshipEdge> receivedGraph = (Graph<Vertex, RelationshipEdge>) obj;
                        logger.info("*WORKER*: Received a new graph.");
                        if (receivedGraph != null) {
                            graphService.mergeGraphs(graph, receivedGraph);
                        }
                    }
                }
                receiveData++;
            }

            activeMQService.sendResult(1);
        } catch (Exception e) {
            logger.error("Error while running first snapshot", e);
        }
    }

    public GraphLoader runNextSnapshot(int superStepNumber, GraphLoader baseLoader) {
        boolean changeReceived = false;
        List<Change> changeList = new ArrayList<>();

        while (!changeReceived) {
            try {
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#change")) {
                    String fileName = msg.split("\n")[1];
                    Object obj = dataShipperService.downloadObject(fileName);
                    if (obj != null) {
                        changeList = (List<Change>) obj;
                        logger.info("List of changes have been received.");
                        changeReceived = true;
                    }
                }
            } catch (Exception e) {
                logger.error("Error while receiving changes for SuperStep " + superStepNumber, e);
            }
        }

        try {
            graphService.updateEntireGraph(baseLoader.getGraph(), changeList);
            activeMQService.sendResult(superStepNumber);
        } catch (Exception e) {
            logger.error("Error while updating graph and sending results for SuperStep " + superStepNumber, e);
        }
        return baseLoader;
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

}
