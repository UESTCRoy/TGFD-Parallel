package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.service.*;
import com.db.tgfdparallel.utils.FileUtil;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class CoordinatorProcess {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorProcess.class);
    private final AppConfig config;
    private final GraphService graphService;
    private final HistogramService histogramService;
    private final PatternService patternService;
    private final ActiveMQService activeMQService;
    private final JobService jobService;
    private final DataShipperService dataShipperService;
    private final ChangeService changeService;
    private final TGFDService tgfdService;
    private final S3Service s3Service;

    @Autowired
    public CoordinatorProcess(AppConfig config, GraphService graphService, HistogramService histogramService, PatternService patternService,
                              ActiveMQService activeMQService, JobService jobService, DataShipperService dataShipperService, ChangeService changeService,
                              TGFDService tgfdService, S3Service s3Service) {
        this.config = config;
        this.graphService = graphService;
        this.histogramService = histogramService;
        this.patternService = patternService;
        this.activeMQService = activeMQService;
        this.jobService = jobService;
        this.dataShipperService = dataShipperService;
        this.changeService = changeService;
        this.tgfdService = tgfdService;
        this.s3Service = s3Service;
    }

    public void start() {
        initializeWorkers();

        // Graph Path
        String firstGraphPath = config.getFirstGraphPath();
        List<String> splitGraphPath = config.getSplitGraphPath();
        String changeFilePath = config.getChangeFilePath();
        // AWS Data Preparation
        if (dataShipperService.isAmazonMode()) {
            changeFilePath = "/home/ec2-user/changeFile";
//            dataShipperService.awsCoordinatorDataPreparation(allDataPath, splitGraphPath, changeFilePath);
        }

        // Generate histogram and send the histogram data to all workers
        List<Graph<Vertex, RelationshipEdge>> graphLoaders = graphService.loadAllSnapshots(Stream.of(firstGraphPath).collect(Collectors.toList()));

        ProcessedHistogramData histogramData = histogramService.computeHistogramAllSnapshot(graphLoaders);
        logger.info("Send the histogram data to the worker");
        dataShipperService.sendHistogramData(histogramData);

        List<FrequencyStatistics> sortedVertexHistogram = histogramData.getSortedVertexHistogram();
        Set<String> vertexTypes = sortedVertexHistogram
                .stream()
                .map(FrequencyStatistics::getType)
                .collect(Collectors.toSet());
        logger.info("There are {} vertices we are going to focus on. They are {}", vertexTypes.size(), vertexTypes);

        // First Level initialization of the pattern tree
        PatternTree patternTree = new PatternTree();
        List<PatternTreeNode> patternTreeNodes = patternService.vSpawnSinglePatternTreeNode(sortedVertexHistogram, patternTree);

        // Send the first level of pattern tree to the worker
        String fileName = dataShipperService.uploadSingleNodePattern(patternTreeNodes);
        // Send single pattern tree to the worker
        logger.info("Send single pattern tree to the worker");
        activeMQService.sendMessage("#singlePattern" + "\t" + fileName);

        // Initialize the graph from the split graph, String (VertexURI) -> Integer (FragmentID)
        Map<String, Integer> fragmentsForTheInitialLoad = graphService.initializeFromSplitGraph(splitGraphPath, vertexTypes);

        // Define jobs and assign them to the workers
        Graph<Vertex, RelationshipEdge> firstGraph = graphService.loadFirstSnapshot(firstGraphPath, vertexTypes).getGraph().getGraph();
        logger.info("The result of optimized first snapshot graph has {} vertices and {} edges", firstGraph.vertexSet().size(), firstGraph.edgeSet().size());
        Map<Integer, List<RelationshipEdge>> edgesToBeShipped = jobService.defineEdgesToBeShipped(firstGraph, fragmentsForTheInitialLoad, patternTreeNodes);

        // Send the edge data to the workers
//        Map<Integer, List<String>> listOfFiles = dataShipperService.dataToBeShippedAndSend(800000, edgesToBeShipped, fragmentsForTheInitialLoad);
//        dataShipperService.edgeShipper(listOfFiles);

        // Generate all the changes for histogram computation and send to all workers
        List<List<Change>> changesData = changeService.changeGenerator(changeFilePath, config.getTimestamp());
        logger.info("Generating change files for {} snapshots and got {} change files", config.getTimestamp(), changesData.size());
        // Send the changes to the workers
        // 一次性把change上传，然后让worker逐步生成new graph
        StringBuilder sb = new StringBuilder("#change");
        for (int i = 0; i < changesData.size(); i++) {
            String changeFileName = dataShipperService.changeShipped(changesData.get(i), i + 2);
            sb.append("\n").append(changeFileName);
        }
        for (String worker : config.getWorkers()) {
            activeMQService.connectProducer();
            activeMQService.send(worker, sb.toString());
            logger.info("Change objects have been shared with '" + worker + "' successfully");
            activeMQService.closeProducer();
        }

        Map<Integer, Integer> dependencyMap = dataShipperService.downloadDependencyMap("dependency");

        // 处理Constant TGFD
        Map<Integer, Set<TGFD>> constantTGFDMap = dataShipperService.downloadConstantTGFD("constant");
        tgfdService.processConstantTGFD(constantTGFDMap, dependencyMap);

        // 处理General TGFD
        Map<Integer, Set<TGFD>> generalTGFDMap = dataShipperService.downloadConstantTGFD("general");
        tgfdService.processGeneralTGFD(generalTGFDMap);

        FileUtil.saveConstantTGFDsToFile(constantTGFDMap, "Constant-TGFD");
        FileUtil.saveConstantTGFDsToFile(generalTGFDMap, "General-TGFD");
        if (dataShipperService.isAmazonMode()) {
            s3Service.stopInstance();
        }
//        if (dataShipperService.isAmazonMode()) {
//
//        }
    }

    private void initializeWorkers() {
        logger.info("Check the status of the workers");
        activeMQService.initializeWorkersStatus();
        activeMQService.statusCheck();
    }

}
