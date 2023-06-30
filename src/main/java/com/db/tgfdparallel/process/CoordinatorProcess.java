package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Autowired
    public CoordinatorProcess(AppConfig config, GraphService graphService, HistogramService histogramService, PatternService patternService,
                              ActiveMQService activeMQService, JobService jobService, DataShipperService dataShipperService, ChangeService changeService) {
        this.config = config;
        this.graphService = graphService;
        this.histogramService = histogramService;
        this.patternService = patternService;
        this.activeMQService = activeMQService;
        this.jobService = jobService;
        this.dataShipperService = dataShipperService;
        this.changeService = changeService;
    }

    public void start() {
        logger.info("Check the status of the workers");
        activeMQService.initializeWorkersStatus();
        activeMQService.statusCheck();

        // Generate all the changes for histogram computation and send to all workers
        List<List<Change>> changesData = changeService.changeGenerator();
        logger.info("Generating change files for {} snapshots and got {} change files", config.getTimestamp(), changesData.size());

        // Generate histogram and send the histogram data to all workers
        String dataPath = config.getDataPath();
        logger.info("Load the first snapshot from the data path: {}", dataPath);
        GraphLoader firstLoader = graphService.loadFirstSnapshot(dataPath);
        // I use deep copy: kryo here
        ProcessedHistogramData histogramData = histogramService.computeHistogram(firstLoader.getGraph(), changesData);
        logger.info("Send the histogram data to the worker");
        dataShipperService.sendHistogramData(histogramData);

        // First Level initialization of the pattern tree
        PatternTree patternTree = new PatternTree();
        List<PatternTreeNode> patternTreeNodes = patternService.vSpawnSinglePatternTreeNode(histogramData, patternTree);

        // Send the first level of pattern tree to the worker
        String fileName = dataShipperService.uploadSingleNodePattern(patternTreeNodes);
        // Send single pattern tree to the worker
        logger.info("Send single pattern tree to the worker");
        activeMQService.sendMessage("#singlePattern" + "\t" + fileName);

        // Initialize the graph from the split graph, String (VertexURI) -> Integer (FragmentID)
        Map<String, Integer> fragmentsForTheInitialLoad = graphService.initializeFromSplitGraph(config.getSplitGraphPath());

        // Define jobs and assign them to the workers
        Map<Integer, List<RelationshipEdge>> edgesToBeShipped = jobService.defineEdgesToBeShipped(firstLoader.getGraph().getGraph(), fragmentsForTheInitialLoad, patternTreeNodes);

        // Send the edge data to the workers
        Map<Integer, List<String>> listOfFiles = dataShipperService.dataToBeShippedAndSend(800000, edgesToBeShipped, fragmentsForTheInitialLoad);
        dataShipperService.edgeShipper(listOfFiles);

        // Send the changes to the workers
        // 不搞异步通过changeFile生成new graph，与worker确认巴拉巴拉，我们一次性把change上传，然后让worker逐步生成new graph
        StringBuilder sb = new StringBuilder("#change");
        for (int i = 0; i < changesData.size(); i++) {
            String changeFileName = dataShipperService.changeShipped(changesData.get(i), i + 2);
            sb.append("\n").append(changeFileName);
        }
        for (String worker : config.getWorkers()) {
            activeMQService.send(worker, sb.toString());
            logger.info("Change objects have been shared with '" + worker + "' successfully");
        }

        Map<Integer, List<TGFD>> integerSetMap = dataShipperService.downloadConstantTGFD();
        for (Map.Entry<Integer, List<TGFD>> entry : integerSetMap.entrySet()) {
            List<TGFD> constantTGFDsList = entry.getValue();
            Integer hashKey = entry.getKey();
            // 如果constantTGFDsList size为1，positive情况，skip
            // 如果constantTGFDsList size不为1，比较DataDependency的rhs的attrValue
            //      1.如果attrValue一样，则更新Delta和entitySize，然后重新计算support
            //          a. Delta无交集：各自计算support，然后与theta比较
            //          b. Delta有交集：取交集部分
            //      2.如果attrValue不一样，则视为negative处理
            if (constantTGFDsList.size() == 1) {
                continue;
            } else {
                Set<String> collect = constantTGFDsList.stream()
                        .map(TGFD::getDependency)
                        .map(x -> x.getY().get(0))
                        .map(x -> (ConstantLiteral) x)
                        .map(ConstantLiteral::getAttrValue)
                        .collect(Collectors.toSet());
                // 有不一样的人attrValue，integerSetMap
                if (collect.size() != constantTGFDsList.size()) {
                    integerSetMap.remove(hashKey);
                } else {
                    // TODO: 给TGFD加个entitySize属性
                    // TODO: 给delta处理交集
                }
            }
        }
    }

//    public void changeShipperAndWaitResult(Map<Integer, String> changesToBeSentToAllWorkers) {
//        AtomicInteger superstep = new AtomicInteger(1);
//        AtomicBoolean resultGetter = new AtomicBoolean(false);
//
//        CompletableFuture<Void> changeShipperFuture = asyncService.changeShipper(changesToBeSentToAllWorkers, superstep, resultGetter);
//        CompletableFuture<Void> resultsGetterFuture = asyncService.resultsGetter(superstep, resultGetter);
//
//        // Wait for both methods to complete
//        CompletableFuture.allOf(changeShipperFuture, resultsGetterFuture).join();
//    }

}
