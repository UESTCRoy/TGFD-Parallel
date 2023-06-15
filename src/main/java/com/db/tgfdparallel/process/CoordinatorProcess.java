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
        Map<Integer, List<Job>> jobsByFragmentID = jobService.defineJobs(firstLoader.getGraph().getGraph(), fragmentsForTheInitialLoad, patternTreeNodes);
//        jobService.jobAssigner(jobsByFragmentID);

        // Send the edge data to the workers
        Map<Integer, List<String>> listOfFiles = dataShipperService.dataToBeShippedAndSend(800000, jobsByFragmentID, fragmentsForTheInitialLoad);
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

        //TODO: 接收来自workers的constant TGFDs，然后处理, 这里可能得用list而不是set
//        Map<Integer, Set<TGFD>> integerSetMap = dataShipperService.downloadConstantTGFD();
//        for (Map.Entry<Integer, Set<TGFD>> entry : integerSetMap.entrySet()) {
//            Set<TGFD> constantTGFDSets = entry.getValue();
//            Integer hashKey = entry.getKey();
//            // Negative
//            if (constantTGFDSets.size() > 1) {
//                int passCount = 0;
//                double maxSupport = 0;
//                for (TGFD data : constantTGFDSets) {
//                    if (data.getTgfdSupport() >= config.getTgfdTheta()) {
//                        passCount++;
//                    }
//                    maxSupport = Math.max(data.getTgfdSupport(), maxSupport);
//                }
//                if (maxSupport < config.getTgfdTheta()) {
//                    integerSetMap.remove(hashKey);
//                } else if (passCount > 1) {
//                    // convert into general TGFD
//                } else {
//                    // only one TGFD pass the support
//                }
//            }
//        }
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
