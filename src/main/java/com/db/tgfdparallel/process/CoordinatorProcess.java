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
    private final DataShipperService dataShipperService;
    private final TGFDService tgfdService;
    private final S3Service s3Service;
    private final ChangeService changeService;

    @Autowired
    public CoordinatorProcess(AppConfig config, GraphService graphService, HistogramService histogramService, PatternService patternService, ChangeService changeService,
                              ActiveMQService activeMQService, DataShipperService dataShipperService, TGFDService tgfdService, S3Service s3Service) {
        this.config = config;
        this.graphService = graphService;
        this.histogramService = histogramService;
        this.patternService = patternService;
        this.activeMQService = activeMQService;
        this.dataShipperService = dataShipperService;
        this.tgfdService = tgfdService;
        this.s3Service = s3Service;
        this.changeService = changeService;
    }

    public void start() {
        long startTime = System.currentTimeMillis();
        initializeWorkers();

        // Graph Path
        String firstGraphPath = config.getFirstGraphPath();
        String changeFilePath = config.getChangeFilePath();
        // Generate histogram and send the histogram data to all workers
        List<Graph<Vertex, RelationshipEdge>> graphLoaders = graphService.loadAllSnapshots(Stream.of(firstGraphPath).collect(Collectors.toList()));

        ProcessedHistogramData histogramData = histogramService.computeHistogramAllSnapshot(graphLoaders);
        logger.info("Send the histogram data to the worker");
        dataShipperService.sendHistogramData(histogramData);
        printHistogram(histogramData);

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

//        // Generate all the changes for histogram computation and send to all workers
//        List<List<Change>> changesData = changeService.changeGenerator(changeFilePath, config.getTimestamp());
//        logger.info("Generating change files for {} snapshots and got {} change files", config.getTimestamp(), changesData.size());
//        // Send the changes to the workers
//        // 一次性把change上传，然后让worker逐步生成new graph
//        StringBuilder sb = new StringBuilder("#change");
//        for (int i = 0; i < changesData.size(); i++) {
//            String changeFileName = dataShipperService.changeShipped(changesData.get(i), i + 2);
//            sb.append("\n").append(changeFileName);
//        }
//        for (String worker : config.getWorkers()) {
//            activeMQService.connectProducer();
//            activeMQService.send(worker, sb.toString());
//            logger.info("Change objects have been shared with '" + worker + "' successfully");
//            activeMQService.closeProducer();
//        }

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

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long hours = durationMillis / 3600000; // 3600000 毫秒/小时
        long minutes = (durationMillis % 3600000) / 60000; // 60000 毫秒/分钟
        long seconds = ((durationMillis % 3600000) % 60000) / 1000;
        logger.info("The coordinator process has been completed in {} hours, {} minutes, {} seconds", hours, minutes, seconds);
    }

    private void initializeWorkers() {
        logger.info("Check the status of the workers");
        activeMQService.initializeWorkersStatus();
        activeMQService.statusCheck();
    }

    private void printHistogram(ProcessedHistogramData histogramData) {
        histogramData.getSortedVertexHistogram().forEach(vertex -> {
            logger.info("Vertex Type: {}, Frequency: {}", vertex.getType(), vertex.getFrequency());
        });
        histogramData.getSortedFrequentEdgesHistogram().forEach(edge -> {
            logger.info("Edge: {}", edge);
        });
        histogramData.getVertexTypesToActiveAttributesMap().forEach((key, value) -> {
            logger.info("Vertex Type: {}, Active Attributes: {}", key, value);
        });
        Set<String> allAttributes = histogramData.getVertexTypesToActiveAttributesMap().values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        allAttributes.forEach(attribute -> {
            logger.info("Attribute: {}", attribute);
        });
    }

}
